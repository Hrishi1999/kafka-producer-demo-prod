"""Main application class"""

import signal
import threading
import time
from datetime import datetime
from typing import Dict, Any

import structlog
from prometheus_client import start_http_server
import schedule

from ..models import AppConfig, QueryConfig
from ..executors import SQLExecutor, ConnectionPool
from ..producers import KafkaProducerWrapper
from ..utils.monitoring import setup_logging, metrics
from .state import StateManager


logger = structlog.get_logger(__name__)


class SQLKafkaProducer:
    """Main application for SQL to Kafka data pipeline"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logger.bind(component="main")
        
        # Setup logging
        setup_logging(
            log_level=config.monitoring.log_level,
            log_format=config.monitoring.log_format,
            log_file=config.monitoring.log_file,
            max_file_size=config.monitoring.max_log_file_size_mb * 1024 * 1024,
            backup_count=config.monitoring.log_backup_count
        )
        
        # Initialize components
        self.connection_pool = None
        self.sql_executor = None
        self.kafka_producer = None
        self.state_manager = None
        
        # Control flags
        self._running = False
        self._shutdown_event = threading.Event()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info("Shutdown signal received", signal=signum)
        self.stop()
    
    def initialize(self):
        """Initialize all components"""
        try:
            self.logger.info("Initializing SQL Kafka Producer")
            
            # Initialize connection pool
            self.connection_pool = ConnectionPool(self.config.database)
            self.sql_executor = SQLExecutor(self.connection_pool)
            
            # Test database connection
            if not self.sql_executor.test_connection():
                raise RuntimeError("Failed to connect to database")
            
            # Initialize Kafka producer
            self.kafka_producer = KafkaProducerWrapper(self.config.kafka)
            
            # Initialize state manager
            self.state_manager = StateManager(self.config.state_file)
            self.state_manager.load()
            
            # Start monitoring if enabled
            if self.config.monitoring.enabled:
                start_http_server(
                    port=self.config.monitoring.metrics_port,
                    addr="0.0.0.0"
                )
                self.logger.info(
                    "Metrics server started",
                    port=self.config.monitoring.metrics_port
                )
            
            self.logger.info("Initialization complete")
            
        except Exception as e:
            self.logger.error("Initialization failed", error=str(e), exc_info=True)
            self.cleanup()
            raise
    
    def run_query(self, query_config: QueryConfig):
        """Run a single query"""
        if not query_config.enabled:
            return
        
        start_time = time.time()
        try:
            self.logger.info("Running query", query_id=query_config.id)
            
            # Get parameters for query
            parameters = self._get_query_parameters(query_config)
            
            # Execute query and process results
            total_rows = 0
            
            for batch in self.sql_executor.execute_query(query_config, parameters):
                total_rows += len(batch)
                
                # Use transactional produces if enabled, otherwise regular
                if self.kafka_producer.transactions_enabled:
                    # Use transactional produce with state coordination
                    with self.state_manager.transactional_update(query_config.id, datetime.now()) as state:
                        def state_callback():
                            self.logger.debug("State committed with transaction", query_id=query_config.id)
                        
                        result = self.kafka_producer.produce_query_results_transactional(
                            query_config, batch, state_callback
                        )
                else:
                    # Regular produce
                    result = self.kafka_producer.produce_query_results(query_config, batch)
                    # Update state separately
                    self.state_manager.update_last_run(query_config.id, datetime.now())
            
            # Record metrics
            duration = time.time() - start_time
            metrics.query_duration.labels(query_id=query_config.id).observe(duration)
            metrics.query_rows_total.labels(query_id=query_config.id).inc(total_rows)
            
            self.logger.info(
                "Query completed",
                query_id=query_config.id,
                total_rows=total_rows,
                duration_seconds=duration
            )
            
        except Exception as e:
            metrics.query_errors_total.labels(
                query_id=query_config.id,
                error_type=type(e).__name__
            ).inc()
            
            self.logger.error(
                "Query execution failed",
                query_id=query_config.id,
                error=str(e),
                exc_info=True
            )
    
    def _get_query_parameters(self, query_config: QueryConfig) -> Dict[str, Any]:
        """Get parameters for query execution"""
        parameters = {}
        
        # Add last run timestamp if needed
        if "last_run_timestamp" in query_config.parameters:
            last_run = self.state_manager.get_last_run(query_config.id)
            if last_run:
                parameters["last_run_timestamp"] = last_run
            else:
                from datetime import timedelta
                parameters["last_run_timestamp"] = datetime.now() - timedelta(hours=24)
        
        return parameters
    
    def schedule_queries(self):
        """Schedule queries with support for seconds-based intervals"""
        for query in self.config.queries:
            if query.schedule and query.enabled:
                schedule_str = query.schedule.strip()
                
                # Handle interval-based scheduling (supports seconds)
                if schedule_str.startswith("interval:"):
                    interval_part = schedule_str.replace("interval:", "")
                    
                    if interval_part.endswith("s"):
                        # Seconds interval (e.g., "interval:5s")
                        seconds = int(interval_part[:-1])
                        schedule.every(seconds).seconds.do(self.run_query, query)
                        self.logger.info("Scheduled query with seconds interval", 
                                       query_id=query.id, interval_seconds=seconds)
                    
                    elif interval_part.endswith("m"):
                        # Minutes interval (e.g., "interval:5m")
                        minutes = int(interval_part[:-1])
                        schedule.every(minutes).minutes.do(self.run_query, query)
                        self.logger.info("Scheduled query with minutes interval", 
                                       query_id=query.id, interval_minutes=minutes)
                    
                    elif interval_part.endswith("h"):
                        # Hours interval (e.g., "interval:1h")
                        hours = int(interval_part[:-1])
                        schedule.every(hours).hours.do(self.run_query, query)
                        self.logger.info("Scheduled query with hours interval", 
                                       query_id=query.id, interval_hours=hours)
                
                # Handle cron-style scheduling (minutes resolution)
                elif "*/5" in schedule_str:
                    schedule.every(5).minutes.do(self.run_query, query)
                    self.logger.info("Scheduled query (cron-style)", 
                                   query_id=query.id, schedule="every 5 minutes")
                elif "*/10" in schedule_str:
                    schedule.every(10).minutes.do(self.run_query, query)
                    self.logger.info("Scheduled query (cron-style)", 
                                   query_id=query.id, schedule="every 10 minutes")
                elif "*/30" in schedule_str:
                    schedule.every(30).minutes.do(self.run_query, query)
                    self.logger.info("Scheduled query (cron-style)", 
                                   query_id=query.id, schedule="every 30 minutes")
                elif "0 *" in schedule_str:
                    schedule.every().hour.do(self.run_query, query)
                    self.logger.info("Scheduled query (cron-style)", 
                                   query_id=query.id, schedule="every hour")
                else:
                    # Default: every minute
                    schedule.every().minute.do(self.run_query, query)
                    self.logger.info("Scheduled query (cron-style)", 
                                   query_id=query.id, schedule="every minute")
    
    def run(self):
        """Run the application"""
        if self._running:
            return
        
        self._running = True
        self.logger.info("Starting SQL Kafka Producer")
        
        try:
            # Schedule queries
            self.schedule_queries()
            
            # Main loop
            while self._running and not self._shutdown_event.is_set():
                schedule.run_pending()
                
                # Check for state persistence
                if self.state_manager.should_checkpoint():
                    self.state_manager.save()
                
                time.sleep(1)
            
        except Exception as e:
            self.logger.error("Runtime error", error=str(e), exc_info=True)
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the application gracefully"""
        if not self._running:
            return
        
        self.logger.info("Stopping SQL Kafka Producer")
        self._running = False
        self._shutdown_event.set()
        
        # Clear scheduled jobs
        schedule.clear()
        
        # Save state
        if self.state_manager:
            self.state_manager.save()
        
        # Cleanup resources
        self.cleanup()
        
        self.logger.info("SQL Kafka Producer stopped")
    
    def cleanup(self):
        """Cleanup all resources"""
        if self.kafka_producer:
            try:
                self.kafka_producer.close()
            except Exception as e:
                self.logger.error("Error closing Kafka producer", error=str(e))
        
        if self.connection_pool:
            try:
                self.connection_pool.close()
            except Exception as e:
                self.logger.error("Error closing connection pool", error=str(e))