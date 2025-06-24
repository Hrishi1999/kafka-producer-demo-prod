import logging
import signal
import sys
import time
import uuid
from threading import Event

from ..models.config import ConsumerConfig
from ..clients.kafka_consumer import KafkaConsumerWrapper, MessageContext
from ..clients.http_client import HTTPClient, HTTPErrorType
from ..utils.monitoring import MetricsCollector
from ..utils.health_check import HealthChecker


class HTTPKafkaConsumer:
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.shutdown_event = Event()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.kafka_consumer = KafkaConsumerWrapper(
            config.kafka, 
            config.retry, 
            config.dlq
        )
        self.http_client = HTTPClient(config.http)
        
        if config.monitoring.enabled:
            self.metrics = MetricsCollector(
                confluent_api_key=config.kafka.confluent_api_key,
                confluent_api_secret=config.kafka.confluent_api_secret,
                cluster_id=config.kafka.cluster_id,
                group_id=config.kafka.group_id
            )
            self.health_checker = HealthChecker(
                config.monitoring.port,
                self.kafka_consumer,
                self.http_client,
                self.metrics
            )
        else:
            self.metrics = None
            self.health_checker = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("HTTPKafkaConsumer initialized")
    
    def _signal_handler(self, signum, _frame):
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_event.set()
    
    def run(self):
        self.logger.info("Starting HTTPKafkaConsumer...")
        
        if self.health_checker:
            self.health_checker.start()
        
        last_metrics_update = time.time()
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    message = self.kafka_consumer.poll_message(timeout=1.0)
                    if message:
                        self._process_message(message)
                    
                    # Update Confluent Cloud metrics periodically
                    if self.metrics and time.time() - last_metrics_update > 60:
                        for topic in self.config.kafka.topics:
                            self.metrics.update_confluent_metrics(topic)
                        last_metrics_update = time.time()
                        
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    if self.metrics:
                        self.metrics.increment_counter("consumer.errors.total")
                    time.sleep(1)
        
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self._shutdown()
    
    def _process_message(self, message: MessageContext):
        start_time = time.time()
        
        # Generate correlation ID for tracing - use existing one from headers if available
        correlation_id = message.headers.get('correlation_id') or message.headers.get('x-correlation-id') or str('Not Available')
        
        try:
            self.logger.debug(f"Processing message from {message.topic}:{message.partition}:{message.offset}", 
                            extra={"correlation_id": correlation_id})
            
            if self.metrics:
                self.metrics.kafka_messages_consumed.labels(topic=message.topic, status="received").inc()
            
            # Call HTTP endpoint with correlation ID
            response = self.http_client.post(message.value, correlation_id=correlation_id)
            
            # Record HTTP metrics
            if self.metrics:
                self.metrics.http_requests_total.labels(
                    endpoint=self.config.http.endpoint_url,
                    method="POST",
                    status_code=str(response.status_code)
                ).inc()
                self.metrics.http_request_duration.labels(
                    endpoint=self.config.http.endpoint_url,
                    method="POST"
                ).observe(time.time() - start_time)
                
                # Update circuit breaker state
                if hasattr(self.http_client, 'circuit_breaker') and self.http_client.circuit_breaker:
                    state_map = {"CLOSED": 0, "OPEN": 1, "HALF_OPEN": 2}
                    state_value = state_map.get(self.http_client.circuit_breaker.state, 0)
                    self.metrics.http_circuit_breaker_state.labels(endpoint=self.config.http.endpoint_url).set(state_value)
            
            if response.is_success():
                # Success - commit the message
                self.kafka_consumer.commit_message(message)
                
                if self.metrics:
                    self.metrics.messages_processed_total.labels(topic=message.topic, status="success").inc()
                    self.metrics.message_processing_duration.labels(topic=message.topic).observe(time.time() - start_time)
                
                self.logger.debug(f"Successfully processed message {message.topic}:{message.partition}:{message.offset}")
            
            elif response.is_transient_error() or response.error_type == HTTPErrorType.CIRCUIT_BREAKER:
                # Transient error - send to retry
                error_msg = f"HTTP {response.status_code}: {response.error_message or response.content}"
                self.kafka_consumer.send_to_retry(message, error_msg, correlation_id)
                self.kafka_consumer.commit_message(message)
                
                if self.metrics:
                    self.metrics.retry_messages_sent.labels(original_topic=message.topic, retry_count="1").inc()
                    self.metrics.messages_processed_total.labels(topic=message.topic, status="retried").inc()
                
                self.logger.warning(f"Transient error processing message {message.topic}:{message.partition}:{message.offset}: {error_msg}",
                                  extra={"correlation_id": correlation_id})
            
            else:
                # Permanent error - send to DLQ
                error_msg = f"HTTP {response.status_code}: {response.content}"
                self.kafka_consumer.send_to_dlq(message, error_msg, correlation_id)
                self.kafka_consumer.commit_message(message)
                
                if self.metrics:
                    self.metrics.dlq_messages_sent.labels(original_topic=message.topic, error_type="http_error").inc()
                    self.metrics.messages_processed_total.labels(topic=message.topic, status="failed").inc()
                
                self.logger.error(f"Permanent error processing message {message.topic}:{message.partition}:{message.offset}: {error_msg}",
                                extra={"correlation_id": correlation_id})
        
        except Exception as e:
            # Unexpected error - send to DLQ
            error_msg = f"Unexpected error: {str(e)}"
            try:
                self.kafka_consumer.send_to_dlq(message, error_msg, correlation_id)
                self.kafka_consumer.commit_message(message)
            except Exception as dlq_error:
                self.logger.critical(f"Failed to send message to DLQ: {dlq_error}",
                                   extra={"correlation_id": correlation_id})
            
            if self.metrics:
                self.metrics.increment_counter("consumer.errors.total")
            
            self.logger.error(f"Unexpected error processing message {message.topic}:{message.partition}:{message.offset}: {e}",
                            extra={"correlation_id": correlation_id})
    
    def _shutdown(self):
        self.logger.info("Shutting down HTTPKafkaConsumer...")
        
        if self.health_checker:
            self.health_checker.stop()
        
        if self.kafka_consumer:
            try:
                # Commit any remaining offsets before closing
                self.logger.info("Committing final offsets before shutdown...")
                self.kafka_consumer.consumer.commit(asynchronous=False)
                self.kafka_consumer.close()
                self.logger.info("Final commit and consumer close completed")
            except Exception as e:
                self.logger.error(f"Error during shutdown commit: {e}")
                self.kafka_consumer.close()
        
        self.logger.info("HTTPKafkaConsumer shutdown complete")


def main():
    import yaml
    import argparse
    
    parser = argparse.ArgumentParser(description="HTTP Kafka Consumer")
    parser.add_argument("--config", default="consumer_config.yaml", help="Config file path")
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Load configuration
        with open(args.config, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        config = ConsumerConfig(**config_dict)
        
        # Create and run consumer
        consumer = HTTPKafkaConsumer(config)
        consumer.run()
        
    except Exception as e:
        logging.error(f"Failed to start consumer: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()