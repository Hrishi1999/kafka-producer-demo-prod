"""Monitoring and metrics utilities"""

from prometheus_client import Counter, Histogram, Gauge
import structlog
import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler


# Prometheus metrics
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

kafka_messages_sent = Counter(
    "kafka_messages_sent_total",
    "Total messages sent to Kafka",
    ["topic", "status"]
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


class Metrics:
    """Metrics collection class"""
    
    def __init__(self):
        self.query_duration = query_duration
        self.query_rows_total = query_rows_total
        self.query_errors_total = query_errors_total
        self.kafka_messages_sent = kafka_messages_sent
        self.batch_processing_duration = batch_processing_duration
        self.db_connections_active = db_connections_active


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