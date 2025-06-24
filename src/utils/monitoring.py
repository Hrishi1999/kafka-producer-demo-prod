"""Producer monitoring and metrics utilities"""

from prometheus_client import Counter, Histogram, Gauge
import structlog
import logging
import sys
import requests
import time
from pathlib import Path
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler


# Producer-specific Prometheus metrics
query_duration = Histogram(
    "sql_query_duration_seconds",
    "SQL query execution duration",
    ["query_id"]
)

query_rows_total = Counter(
    "sql_query_rows_total",
    "Total rows returned by queries",
    ["query_id"]
)

query_errors_total = Counter(
    "sql_query_errors_total",
    "Total query execution errors",
    ["query_id", "error_type"]
)

kafka_messages_produced = Counter(
    "kafka_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic", "status"]
)

kafka_producer_request_rate = Gauge(
    "kafka_producer_request_rate",
    "Producer request rate (requests/sec)",
    ["topic"]
)

kafka_producer_byte_rate = Gauge(
    "kafka_producer_byte_rate", 
    "Producer byte rate (bytes/sec)",
    ["topic"]
)

kafka_producer_response_rate = Gauge(
    "kafka_producer_response_rate",
    "Producer response rate (responses/sec)",
    ["topic"]
)

kafka_producer_request_latency_avg = Gauge(
    "kafka_producer_request_latency_avg_ms",
    "Average producer request latency (ms)",
    ["topic"]
)

kafka_producer_batch_size_avg = Gauge(
    "kafka_producer_batch_size_avg",
    "Average producer batch size",
    ["topic"]
)

kafka_producer_record_queue_time_avg = Gauge(
    "kafka_producer_record_queue_time_avg_ms",
    "Average time records spend in producer queue (ms)",
    ["topic"]
)

batch_processing_duration = Histogram(
    "batch_processing_duration_seconds",
    "Duration to process a batch",
    ["query_id"]
)

db_connections_active = Gauge(
    "db_connections_active",
    "Number of active database connections"
)

# DLQ and error handling metrics
dlq_messages_sent = Counter(
    "dlq_messages_sent_total",
    "Messages sent to Dead Letter Queue",
    ["original_topic", "error_type"]
)

dlq_send_failures = Counter(
    "dlq_send_failures_total",
    "Failed attempts to send to DLQ",
    ["original_topic", "error_type"]
)

dlq_circuit_breaker_open = Counter(
    "dlq_circuit_breaker_open_total",
    "Number of times DLQ circuit breaker opened"
)

message_processing_errors = Counter(
    "message_processing_errors_total",
    "Message-level processing errors",
    ["query_id", "error_type", "topic"]
)

transaction_aborts = Counter(
    "kafka_transaction_aborts_total",
    "Number of Kafka transaction aborts",
    ["query_id", "reason"]
)

messages_quarantined = Counter(
    "messages_quarantined_total",
    "Messages sent to quarantine after all retries failed",
    ["query_id", "topic", "error_type"]
)

final_failures = Counter(
    "final_failures_total",
    "Messages that failed permanently after all retry attempts",
    ["query_id", "topic", "failure_stage"]
)


class ConfluentCloudMetrics:
    """Confluent Cloud API metrics collector"""
    
    def __init__(self, api_key: str = None, api_secret: str = None, cluster_id: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.cluster_id = cluster_id
        self.logger = structlog.get_logger(__name__)
        self.last_fetch = 0
        self.fetch_interval = 60  # Fetch every 60 seconds
        
    def fetch_producer_metrics(self, topic: str = None) -> Dict[str, Any]:
        """Fetch producer metrics from Confluent Cloud API"""
        if not self._should_fetch():
            return {}
            
        if not all([self.api_key, self.api_secret, self.cluster_id]):
            self.logger.warning("Confluent Cloud API credentials not configured")
            return {}
            
        try:
            # Confluent Cloud Metrics API endpoint
            url = f"https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
            
            headers = {
                "Content-Type": "application/json"
            }
            
            # Query for producer metrics
            query = {
                "aggregations": [
                    {
                        "metric": "io.confluent.kafka.server/sent_bytes",
                        "aggregation": "AVG"
                    },
                    {
                        "metric": "io.confluent.kafka.server/sent_records", 
                        "aggregation": "AVG"
                    },
                    {
                        "metric": "io.confluent.kafka.server/request_latency_avg",
                        "aggregation": "AVG" 
                    }
                ],
                "filter": {
                    "op": "AND",
                    "filters": [
                        {
                            "field": "resource.kafka.id",
                            "op": "EQ",
                            "value": self.cluster_id
                        }
                    ]
                },
                "intervals": ["PT1M"],
                "granularity": "PT1M"
            }
            
            if topic:
                query["filter"]["filters"].append({
                    "field": "metric.topic",
                    "op": "EQ", 
                    "value": topic
                })
            
            response = requests.post(
                url,
                json=query,
                headers=headers,
                auth=(self.api_key, self.api_secret),
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                self._update_producer_metrics(data, topic)
                self.last_fetch = time.time()
                return data
            else:
                self.logger.error("Failed to fetch Confluent Cloud metrics", 
                                status_code=response.status_code, response=response.text)
                
        except Exception as e:
            self.logger.error("Error fetching Confluent Cloud metrics", error=str(e))
            
        return {}
    
    def _should_fetch(self) -> bool:
        """Check if metrics should be fetched based on interval"""
        return time.time() - self.last_fetch >= self.fetch_interval
    
    def _update_producer_metrics(self, data: Dict[str, Any], topic: str = None):
        """Update Prometheus metrics with Confluent Cloud data"""
        try:
            for result in data.get("data", []):
                metric_name = result.get("metric", "")
                topic_name = topic or result.get("topic", "unknown")
                
                for timestamp_data in result.get("data", []):
                    value = timestamp_data.get("value", 0)
                    
                    if "sent_bytes" in metric_name:
                        kafka_producer_byte_rate.labels(topic=topic_name).set(value)
                    elif "sent_records" in metric_name:
                        kafka_producer_request_rate.labels(topic=topic_name).set(value)
                    elif "request_latency_avg" in metric_name:
                        kafka_producer_request_latency_avg.labels(topic=topic_name).set(value)
                        
        except Exception as e:
            self.logger.error("Error updating producer metrics", error=str(e))


class Metrics:
    """Producer metrics collection class"""
    
    def __init__(self, confluent_api_key: str = None, confluent_api_secret: str = None, cluster_id: str = None):
        # SQL/Producer metrics
        self.query_duration = query_duration
        self.query_rows_total = query_rows_total
        self.query_errors_total = query_errors_total
        self.kafka_messages_produced = kafka_messages_produced
        self.batch_processing_duration = batch_processing_duration
        self.db_connections_active = db_connections_active
        
        # Kafka producer metrics
        self.kafka_producer_request_rate = kafka_producer_request_rate
        self.kafka_producer_byte_rate = kafka_producer_byte_rate
        self.kafka_producer_response_rate = kafka_producer_response_rate
        self.kafka_producer_request_latency_avg = kafka_producer_request_latency_avg
        self.kafka_producer_batch_size_avg = kafka_producer_batch_size_avg
        self.kafka_producer_record_queue_time_avg = kafka_producer_record_queue_time_avg
        
        # Error handling metrics
        self.dlq_messages_sent = dlq_messages_sent
        self.dlq_send_failures = dlq_send_failures
        self.dlq_circuit_breaker_open = dlq_circuit_breaker_open
        self.message_processing_errors = message_processing_errors
        self.transaction_aborts = transaction_aborts
        self.messages_quarantined = messages_quarantined
        self.final_failures = final_failures
        
        # Confluent Cloud metrics
        self.confluent_metrics = ConfluentCloudMetrics(confluent_api_key, confluent_api_secret, cluster_id)
    
    def update_confluent_metrics(self, topic: str = None):
        """Update metrics from Confluent Cloud API"""
        self.confluent_metrics.fetch_producer_metrics(topic)


# Global metrics instance
metrics = Metrics()


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[str] = "logs/sql-kafka-producer.log",
    max_file_size: int = 10485760,  # 10MB
    backup_count: int = 5
) -> None:
    """Setup structured logging with file and console output"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()  # JSON for both file and console
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Use JSON formatter for file logs
        class StructlogFormatter(logging.Formatter):
            def format(self, record):
                # Let structlog handle the formatting
                return record.getMessage()
        
        file_handler.setFormatter(StructlogFormatter())
        root_logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Use the same JSON formatter for console
    class StructlogFormatter(logging.Formatter):
        def format(self, record):
            return record.getMessage()
    
    console_handler.setFormatter(StructlogFormatter())
    root_logger.addHandler(console_handler)