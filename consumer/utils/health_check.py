import json
import logging
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

from ..clients.kafka_consumer import KafkaConsumerWrapper
from ..clients.http_client import HTTPClient


class HealthCheckHandler(BaseHTTPRequestHandler):
    def __init__(self, health_checker, *args, **kwargs):
        self.health_checker = health_checker
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/metrics":
            self._handle_metrics()
        elif self.path == "/status":
            self._handle_status()
        else:
            self._send_response(404, {"error": "Not found"})
    
    def _handle_health(self):
        health_status = self.health_checker.get_health()
        status_code = 200 if health_status["status"] == "healthy" else 503
        self._send_response(status_code, health_status)
    
    def _handle_metrics(self):
        if hasattr(self.health_checker, 'metrics') and self.health_checker.metrics:
            metrics = self.health_checker.metrics.get_prometheus_format()
            self._send_response(200, metrics, content_type="text/plain")
        else:
            self._send_response(404, {"error": "Metrics not enabled"})
    
    def _handle_status(self):
        status = self.health_checker.get_detailed_status()
        self._send_response(200, status)
    
    def _send_response(self, status_code: int, data, content_type: str = "application/json"):
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        
        if isinstance(data, str):
            response_data = data.encode('utf-8')
        else:
            response_data = json.dumps(data, indent=2).encode('utf-8')
        
        self.wfile.write(response_data)
    
    def log_message(self, format, *args):
        # Suppress default HTTP server logging
        pass


class HealthChecker:
    def __init__(self, port: int, kafka_consumer: KafkaConsumerWrapper, http_client: HTTPClient, metrics=None):
        self.port = port
        self.kafka_consumer = kafka_consumer
        self.http_client = http_client
        self.metrics = metrics
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(__name__)
        self.start_time = time.time()
        
    def start(self):
        def handler(*args, **kwargs):
            return HealthCheckHandler(self, *args, **kwargs)
        
        try:
            self.server = HTTPServer(("0.0.0.0", self.port), handler)
            self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.server_thread.start()
            self.logger.info(f"Health check server started on port {self.port}")
        except Exception as e:
            self.logger.error(f"Failed to start health check server: {e}")
    
    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.server_thread:
            self.server_thread.join(timeout=5)
        self.logger.info("Health check server stopped")
    
    def get_health(self) -> dict:
        checks = {
            "kafka_consumer": self._check_kafka_consumer(),
            "http_client": self._check_http_client(),
        }
        
        overall_status = "healthy" if all(check["status"] == "healthy" for check in checks.values()) else "unhealthy"
        
        return {
            "status": overall_status,
            "timestamp": int(time.time()),
            "uptime_seconds": int(time.time() - self.start_time),
            "checks": checks
        }
    
    def get_detailed_status(self) -> dict:
        return {
            "service": "http-kafka-consumer",
            "version": "1.0.0",
            "uptime_seconds": int(time.time() - self.start_time),
            "kafka": {
                "subscribed_topics": self.kafka_consumer.kafka_config.topics,
                "group_id": self.kafka_consumer.kafka_config.group_id,
                "isolation_level": self.kafka_consumer.kafka_config.isolation_level,
            },
            "http": {
                "endpoint_url": self.http_client.config.endpoint_url,
                "timeout_seconds": self.http_client.config.timeout_seconds,
                "circuit_breaker_enabled": self.http_client.config.circuit_breaker_enabled,
                "circuit_breaker_state": getattr(self.http_client.circuit_breaker, 'state', 'N/A') if self.http_client.circuit_breaker else 'disabled'
            },
            "metrics": self.metrics.get_metrics() if self.metrics else None
        }
    
    def _check_kafka_consumer(self) -> dict:
        try:
            # Simple check - if consumer exists and is configured
            if self.kafka_consumer and self.kafka_consumer.consumer:
                return {
                    "status": "healthy",
                    "message": "Kafka consumer is active"
                }
            else:
                return {
                    "status": "unhealthy",
                    "message": "Kafka consumer is not initialized"
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Kafka consumer check failed: {str(e)}"
            }
    
    def _check_http_client(self) -> dict:
        try:
            # Check if HTTP client is available and circuit breaker status
            if self.http_client.circuit_breaker:
                if self.http_client.circuit_breaker.state == "OPEN":
                    return {
                        "status": "degraded",
                        "message": "HTTP client circuit breaker is OPEN"
                    }
            
            return {
                "status": "healthy",
                "message": "HTTP client is ready"
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"HTTP client check failed: {str(e)}"
            }