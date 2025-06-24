"""Consumer monitoring and metrics utilities"""

from prometheus_client import Counter, Histogram, Gauge
import time
import requests
import structlog
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from logging.handlers import RotatingFileHandler
from collections import defaultdict, deque
import threading


# Consumer-specific Prometheus metrics
kafka_messages_consumed = Counter(
    "kafka_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["topic", "status"]
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag_records",
    "Consumer lag in number of records",
    ["topic", "partition", "group_id"]
)

kafka_consumer_fetch_rate = Gauge(
    "kafka_consumer_fetch_rate",
    "Consumer fetch rate (records/sec)",
    ["topic", "group_id"]
)

kafka_consumer_bytes_consumed_rate = Gauge(
    "kafka_consumer_bytes_consumed_rate",
    "Consumer bytes consumed rate (bytes/sec)",
    ["topic", "group_id"]
)

kafka_consumer_fetch_latency_avg = Gauge(
    "kafka_consumer_fetch_latency_avg_ms",
    "Average consumer fetch latency (ms)",
    ["topic", "group_id"]
)

kafka_consumer_records_per_request_avg = Gauge(
    "kafka_consumer_records_per_request_avg",
    "Average records per fetch request",
    ["topic", "group_id"]
)

# HTTP client metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests made",
    ["endpoint", "method", "status_code"]
)

http_request_duration = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["endpoint", "method"]
)

http_circuit_breaker_state = Gauge(
    "http_circuit_breaker_state",
    "HTTP circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["endpoint"]
)

# Message processing metrics
messages_processed_total = Counter(
    "messages_processed_total",
    "Total messages processed",
    ["topic", "status"]
)

message_processing_duration = Histogram(
    "message_processing_duration_seconds",
    "Message processing duration",
    ["topic"]
)

# Shared DLQ metrics (moved from producer)
dlq_messages_sent = Counter(
    "dlq_messages_sent_total",
    "Messages sent to Dead Letter Queue",
    ["original_topic", "error_type"]
)

retry_messages_sent = Counter(
    "retry_messages_sent_total",
    "Messages sent to retry topic",
    ["original_topic", "retry_count"]
)

avro_deserialization_errors = Counter(
    "avro_deserialization_errors_total",
    "Avro deserialization errors",
    ["topic", "error_type"]
)


class ConfluentCloudConsumerMetrics:
    """Confluent Cloud API metrics collector for consumer"""
    
    def __init__(self, api_key: str = None, api_secret: str = None, cluster_id: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.cluster_id = cluster_id
        self.logger = structlog.get_logger(__name__)
        self.last_fetch = 0
        self.fetch_interval = 60  # Fetch every 60 seconds
        
    def fetch_consumer_metrics(self, topic: str = None, group_id: str = None) -> Dict[str, Any]:
        """Fetch consumer metrics from Confluent Cloud API"""
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
            
            # Query for consumer metrics
            query = {
                "aggregations": [
                    {
                        "metric": "io.confluent.kafka.server/received_bytes",
                        "aggregation": "AVG"
                    },
                    {
                        "metric": "io.confluent.kafka.server/received_records",
                        "aggregation": "AVG"
                    },
                    {
                        "metric": "io.confluent.kafka.server/consumer_lag_offsets",
                        "aggregation": "MAX"
                    },
                    {
                        "metric": "io.confluent.kafka.server/fetch_latency_avg",
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
                
            if group_id:
                query["filter"]["filters"].append({
                    "field": "metric.consumer_group_id",
                    "op": "EQ",
                    "value": group_id
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
                self._update_consumer_metrics(data, topic, group_id)
                self.last_fetch = time.time()
                return data
            else:
                self.logger.error("Failed to fetch Confluent Cloud consumer metrics",
                                status_code=response.status_code, response=response.text)
                
        except Exception as e:
            self.logger.error("Error fetching Confluent Cloud consumer metrics", error=str(e))
            
        return {}
    
    def _should_fetch(self) -> bool:
        """Check if metrics should be fetched based on interval"""
        return time.time() - self.last_fetch >= self.fetch_interval
    
    def _update_consumer_metrics(self, data: Dict[str, Any], topic: str = None, group_id: str = None):
        """Update Prometheus metrics with Confluent Cloud data"""
        try:
            for result in data.get("data", []):
                metric_name = result.get("metric", "")
                topic_name = topic or result.get("topic", "unknown")
                group_name = group_id or result.get("consumer_group_id", "unknown")
                partition = result.get("partition", "0")
                
                for timestamp_data in result.get("data", []):
                    value = timestamp_data.get("value", 0)
                    
                    if "received_bytes" in metric_name:
                        kafka_consumer_bytes_consumed_rate.labels(topic=topic_name, group_id=group_name).set(value)
                    elif "received_records" in metric_name:
                        kafka_consumer_fetch_rate.labels(topic=topic_name, group_id=group_name).set(value)
                    elif "consumer_lag_offsets" in metric_name:
                        kafka_consumer_lag.labels(topic=topic_name, partition=partition, group_id=group_name).set(value)
                    elif "fetch_latency_avg" in metric_name:
                        kafka_consumer_fetch_latency_avg.labels(topic=topic_name, group_id=group_name).set(value)
                        
        except Exception as e:
            self.logger.error("Error updating consumer metrics", error=str(e))


class MetricsCollector:
    """Consumer metrics collection class"""
    
    def __init__(self, confluent_api_key: str = None, confluent_api_secret: str = None, cluster_id: str = None, group_id: str = None):
        # Consumer metrics
        self.kafka_messages_consumed = kafka_messages_consumed
        self.kafka_consumer_lag = kafka_consumer_lag
        self.kafka_consumer_fetch_rate = kafka_consumer_fetch_rate
        self.kafka_consumer_bytes_consumed_rate = kafka_consumer_bytes_consumed_rate
        self.kafka_consumer_fetch_latency_avg = kafka_consumer_fetch_latency_avg
        self.kafka_consumer_records_per_request_avg = kafka_consumer_records_per_request_avg
        
        # HTTP client metrics
        self.http_requests_total = http_requests_total
        self.http_request_duration = http_request_duration
        self.http_circuit_breaker_state = http_circuit_breaker_state
        
        # Processing metrics
        self.messages_processed_total = messages_processed_total
        self.message_processing_duration = message_processing_duration
        
        # Error handling metrics
        self.dlq_messages_sent = dlq_messages_sent
        self.retry_messages_sent = retry_messages_sent
        self.avro_deserialization_errors = avro_deserialization_errors
        
        # Confluent Cloud metrics
        self.confluent_metrics = ConfluentCloudConsumerMetrics(confluent_api_key, confluent_api_secret, cluster_id)
        self.group_id = group_id
        
        # Legacy simple metrics for compatibility
        self.counters: Dict[str, int] = defaultdict(int)
        self.histograms: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.gauges: Dict[str, float] = {}
        self.lock = threading.Lock()
    
    def increment_counter(self, name: str, value: int = 1, labels: Dict[str, str] = None):
        with self.lock:
            key = self._make_key(name, labels)
            self.counters[key] += value
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        with self.lock:
            key = self._make_key(name, labels)
            self.histograms[key].append((time.time(), value))
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        with self.lock:
            key = self._make_key(name, labels)
            self.gauges[key] = value
    
    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{name}{{{label_str}}}"
        return name
    
    def get_metrics(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "counters": dict(self.counters),
                "histograms": {k: list(v) for k, v in self.histograms.items()},
                "gauges": dict(self.gauges),
                "timestamp": time.time()
            }
    
    def get_prometheus_format(self) -> str:
        metrics = []
        
        with self.lock:
            # Counters
            for name, value in self.counters.items():
                metrics.append(f"# TYPE {name.split('{')[0]} counter")
                metrics.append(f"{name} {value}")
            
            # Gauges
            for name, value in self.gauges.items():
                metrics.append(f"# TYPE {name.split('{')[0]} gauge")
                metrics.append(f"{name} {value}")
            
            # Histograms (simplified - just current values)
            for name, values in self.histograms.items():
                if values:
                    recent_values = [v[1] for v in list(values)[-100:]]  # Last 100 values
                    avg = sum(recent_values) / len(recent_values)
                    metrics.append(f"# TYPE {name.split('{')[0]} histogram")
                    metrics.append(f"{name}_avg {avg}")
                    metrics.append(f"{name}_count {len(recent_values)}")
        
        return "\n".join(metrics)
    
    def update_confluent_metrics(self, topic: str = None):
        """Update metrics from Confluent Cloud API"""
        self.confluent_metrics.fetch_consumer_metrics(topic, self.group_id)


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json", 
    log_file: Optional[str] = "logs/http-kafka-consumer.log",
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
            structlog.processors.JSONRenderer() if log_format == "json" else structlog.dev.ConsoleRenderer()
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
    
    # Use the same formatter for console
    class StructlogFormatter(logging.Formatter):
        def format(self, record):
            return record.getMessage()
    
    console_handler.setFormatter(StructlogFormatter())
    root_logger.addHandler(console_handler)