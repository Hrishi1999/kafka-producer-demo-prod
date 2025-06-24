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


class HTTPRetryConsumer:
    """Consumer specifically for processing retry messages"""
    
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.shutdown_event = Event()
        self.logger = logging.getLogger(__name__)
        
        # Update topics to consume from retry topics
        retry_topics = []
        for topic in config.kafka.topics:
            retry_topics.append(f"{topic}{config.retry.topic_suffix}")
        
        # Create modified config for retry consumer
        retry_config = config.model_copy(deep=True)
        retry_config.kafka.topics = retry_topics
        retry_config.kafka.group_id = f"{config.kafka.group_id}-retry"
        
        # Initialize components
        self.kafka_consumer = KafkaConsumerWrapper(
            retry_config.kafka, 
            retry_config.retry, 
            retry_config.dlq
        )
        self.http_client = HTTPClient(config.http)
        
        if config.monitoring.enabled:
            self.metrics = MetricsCollector(
                confluent_api_key=config.kafka.confluent_api_key,
                confluent_api_secret=config.kafka.confluent_api_secret,
                cluster_id=config.kafka.cluster_id,
                group_id=retry_config.kafka.group_id
            )
            self.health_checker = HealthChecker(
                config.monitoring.port + 1,  # Use different port for retry consumer
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
        
        self.logger.info(f"HTTPRetryConsumer initialized for topics: {retry_topics}")
    
    def _signal_handler(self, signum, _frame):
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_event.set()
    
    def run(self):
        self.logger.info("Starting HTTPRetryConsumer...")
        
        if self.health_checker:
            self.health_checker.start()
        
        last_metrics_update = time.time()
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    message = self.kafka_consumer.poll_message(timeout=1.0)
                    if message:
                        self._process_retry_message(message)
                    
                    # Update Confluent Cloud metrics periodically
                    if self.metrics and time.time() - last_metrics_update > 60:
                        for topic in self.kafka_consumer.kafka_config.topics:
                            self.metrics.update_confluent_metrics(topic)
                        last_metrics_update = time.time()
                        
                except Exception as e:
                    self.logger.error(f"Error in retry consumer main loop: {e}")
                    if self.metrics:
                        self.metrics.increment_counter("retry_consumer.errors.total")
                    time.sleep(1)
        
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self._shutdown()
    
    def _process_retry_message(self, message: MessageContext):
        start_time = time.time()
        
        try:
            # Parse retry message payload
            retry_data = message.value
            original_topic = retry_data.get("original_topic", "unknown")
            original_data = retry_data.get("original_data", {})
            retry_count = int(retry_data.get("retry_count", "1"))
            correlation_id = retry_data.get("correlation_id") or message.headers.get('x-correlation-id') or 'Not Available'
            
            self.logger.info(f"Processing retry message from {message.topic}:{message.partition}:{message.offset}, "
                           f"retry_count={retry_count}, original_topic={original_topic}",
                           extra={"correlation_id": correlation_id})
            
            if self.metrics:
                self.metrics.kafka_messages_consumed.labels(topic=message.topic, status="received").inc()
            
            # Call HTTP endpoint with original data
            response = self.http_client.post(original_data, correlation_id=correlation_id)
            
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
                # Success - commit the retry message
                self.kafka_consumer.commit_message(message)
                
                if self.metrics:
                    self.metrics.messages_processed_total.labels(topic=original_topic, status="retry_success").inc()
                    self.metrics.message_processing_duration.labels(topic=original_topic).observe(time.time() - start_time)
                
                self.logger.info(f"Successfully processed retry message {message.topic}:{message.partition}:{message.offset} "
                               f"for original topic {original_topic}", extra={"correlation_id": correlation_id})
            
            else:
                # Retry failed - send directly to DLQ (no more retries)
                error_msg = f"Retry failed - HTTP {response.status_code}: {response.error_message or response.content}"
                
                # Create original message context for DLQ
                original_message = MessageContext(
                    topic=original_topic,
                    partition=retry_data.get("original_partition", 0),
                    offset=retry_data.get("original_offset", 0),
                    key=message.key,
                    value=original_data,
                    headers={"x-correlation-id": correlation_id} if correlation_id != 'Not Available' else {}
                )
                
                self.kafka_consumer.send_to_dlq(original_message, error_msg, correlation_id)
                self.kafka_consumer.commit_message(message)  # Commit the retry message
                
                if self.metrics:
                    self.metrics.dlq_messages_sent.labels(original_topic=original_topic, error_type="retry_failed").inc()
                    self.metrics.messages_processed_total.labels(topic=original_topic, status="retry_failed").inc()
                
                self.logger.error(f"Retry failed for message {message.topic}:{message.partition}:{message.offset}, "
                                f"sent to DLQ: {error_msg}", extra={"correlation_id": correlation_id})
        
        except Exception as e:
            # Unexpected error processing retry - send to DLQ
            error_msg = f"Unexpected retry processing error: {str(e)}"
            correlation_id = message.headers.get('x-correlation-id', 'Not Available')
            
            try:
                # Try to extract original topic from message, fallback to retry topic
                retry_data = message.value
                original_topic = retry_data.get("original_topic", message.topic.replace(self.config.retry.topic_suffix, ""))
                original_data = retry_data.get("original_data", message.value)
                
                original_message = MessageContext(
                    topic=original_topic,
                    partition=retry_data.get("original_partition", message.partition),
                    offset=retry_data.get("original_offset", message.offset),
                    key=message.key,
                    value=original_data,
                    headers=message.headers
                )
                
                self.kafka_consumer.send_to_dlq(original_message, error_msg, correlation_id)
                self.kafka_consumer.commit_message(message)
            except Exception as dlq_error:
                self.logger.critical(f"Failed to send retry message to DLQ: {dlq_error}",
                                   extra={"correlation_id": correlation_id})
            
            if self.metrics:
                self.metrics.increment_counter("retry_consumer.errors.total")
            
            self.logger.error(f"Unexpected error processing retry message {message.topic}:{message.partition}:{message.offset}: {e}",
                            extra={"correlation_id": correlation_id})
    
    def _shutdown(self):
        self.logger.info("Shutting down HTTPRetryConsumer...")
        
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
        
        self.logger.info("HTTPRetryConsumer shutdown complete")