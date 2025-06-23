"""Monitoring and metrics utilities"""

from prometheus_client import Counter, Histogram, Gauge
import structlog
import logging
import sys
from typing import Optional


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
    log_format: str = "json"
) -> None:
    """Setup structured logging"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if log_format == "json" else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)