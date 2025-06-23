"""Error handling and DLQ management for Kafka producer"""

import json
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

import structlog
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry.avro import SerializationError

from ..models import KafkaConfig, QueryConfig
from ..utils.monitoring import metrics
from .final_failure_handler import FinalFailureHandler


logger = structlog.get_logger(__name__)


class ErrorType(Enum):
    """Classification of error types"""
    TRANSIENT = "transient"
    PERMANENT = "permanent" 
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class FailedMessage:
    """Represents a message that failed to process"""
    original_payload: Dict[str, Any]
    original_topic: str
    key: Optional[str]
    query_id: str
    correlation_id: str
    error_type: ErrorType
    error_reason: str
    error_details: str
    failure_timestamp: str
    producer_hostname: str
    retry_attempt: int = 0
    
    def to_dlq_message(self) -> Dict[str, Any]:
        """Convert to DLQ message format"""
        return asdict(self)


class CircuitBreaker:
    """
    Circuit breaker for DLQ producer
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit breaker trips, all requests fail fast
    - HALF_OPEN: Testing if service has recovered
    
    Behavior when OPEN:
    - Prevents cascade failures by failing fast
    - Protects DLQ infrastructure from overload
    - Causes transaction aborts (data safety preserved)
    - Automatically retries after timeout period
    """
    
    def __init__(self, failure_threshold: int, timeout_seconds: int):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.logger = logger.bind(component="circuit_breaker")
    
    def can_execute(self) -> bool:
        """Check if operation can be executed"""
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout_seconds:
                self.state = "HALF_OPEN"
                self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """Record successful operation"""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            self.logger.info("Circuit breaker CLOSED - recovered")
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.logger.error(
                "Circuit breaker OPEN - too many failures",
                failure_count=self.failure_count,
                threshold=self.failure_threshold
            )


class ErrorClassifier:
    """Classifies errors as transient or permanent"""
    
    TRANSIENT_ERRORS = {
        KafkaError._TIMED_OUT,
        KafkaError._BROKER_NOT_AVAILABLE,
        KafkaError._NETWORK_EXCEPTION,
        KafkaError._REQUEST_TIMED_OUT,
        KafkaError._TRANSPORT,
        KafkaError._UNKNOWN_TOPIC_OR_PART,
        KafkaError._LEADER_NOT_AVAILABLE,
        KafkaError._REPLICA_NOT_AVAILABLE,
        KafkaError._NOT_ENOUGH_REPLICAS,
        KafkaError._NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    }
    
    PERMANENT_ERRORS = {
        KafkaError._MSG_SIZE_TOO_LARGE,
        KafkaError._INVALID_MSG,
        KafkaError._INVALID_MSG_SIZE,
        KafkaError._INVALID_TOPIC_EXCEPTION,
        KafkaError._RECORD_LIST_TOO_LARGE,
        KafkaError._INVALID_REQUIRED_ACKS,
        KafkaError._TOPIC_AUTHORIZATION_FAILED,
        KafkaError._CLUSTER_AUTHORIZATION_FAILED,
        KafkaError._INVALID_TIMESTAMP,
    }
    
    @classmethod
    def classify_error(cls, error: Exception) -> ErrorType:
        """Classify error type"""
        if isinstance(error, SerializationError):
            return ErrorType.PERMANENT
        
        if isinstance(error, KafkaException):
            kafka_error = error.args[0] if error.args else None
            if hasattr(kafka_error, 'code'):
                if kafka_error.code() in cls.TRANSIENT_ERRORS:
                    return ErrorType.TRANSIENT
                elif kafka_error.code() in cls.PERMANENT_ERRORS:
                    return ErrorType.PERMANENT
        
        # Default to transient for unknown errors
        return ErrorType.TRANSIENT


class DLQHandler:
    """Handles Dead Letter Queue operations with circuit breaker"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.dlq_config = config.dlq
        self.logger = logger.bind(component="dlq_handler")
        
        # Create non-transactional producer for DLQ
        self.dlq_producer = self._create_dlq_producer()
        
        # Circuit breaker for DLQ operations
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.dlq_config.circuit_breaker_failure_threshold,
            timeout_seconds=self.dlq_config.circuit_breaker_timeout_seconds
        )
        
        import socket
        self.hostname = socket.gethostname()
    
    def _create_dlq_producer(self) -> Producer:
        """Create dedicated DLQ producer (non-transactional)"""
        producer_config = self.config.get_producer_config()
        
        # Remove transactional settings for DLQ producer
        producer_config.pop("transactional.id", None)
        producer_config.pop("transaction.timeout.ms", None)
        
        # Configure for reliability
        producer_config.update({
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 1000,
            "delivery.timeout.ms": 120000,
        })
        
        return Producer(producer_config)
    
    def send_to_dlq(
        self, 
        failed_message: FailedMessage,
        dlq_topic: str
    ) -> bool:
        """Send message to DLQ with circuit breaker protection"""
        if not self.dlq_config.enabled:
            self.logger.warning("DLQ is disabled, dropping failed message", 
                              query_id=failed_message.query_id)
            return False
        
        if not self.circuit_breaker.can_execute():
            self.logger.error("DLQ circuit breaker is OPEN, cannot send to DLQ")
            metrics.dlq_circuit_breaker_open.inc()
            # When circuit breaker is OPEN:
            # 1. This exception propagates to error_handler.handle_message_error()
            # 2. handle_message_error() returns False (don't continue)
            # 3. Kafka transaction is aborted
            # 4. SQL checkpoint is NOT updated (data safety)
            # 5. Whole batch will be retried when circuit breaker closes
            raise RuntimeError("DLQ circuit breaker is OPEN")
        
        try:
            dlq_message = failed_message.to_dlq_message()
            value = json.dumps(dlq_message, default=str)
            
            # Send to DLQ synchronously to ensure delivery with correlation ID header
            self.dlq_producer.produce(
                topic=dlq_topic,
                key=failed_message.key,
                value=value,
                headers={"x-correlation-id": failed_message.correlation_id},
                callback=self._dlq_delivery_callback
            )
            
            # Wait for delivery confirmation
            self.dlq_producer.flush(timeout=30)
            
            self.circuit_breaker.record_success()
            
            metrics.dlq_messages_sent.labels(
                original_topic=failed_message.original_topic,
                error_type=failed_message.error_type.value
            ).inc()
            
            self.logger.info(
                "Message sent to DLQ",
                dlq_topic=dlq_topic,
                original_topic=failed_message.original_topic,
                query_id=failed_message.query_id,
                correlation_id=failed_message.correlation_id,
                error_type=failed_message.error_type.value
            )
            
            return True
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            self.logger.error(
                "Failed to send message to DLQ",
                dlq_topic=dlq_topic,
                error=str(e),
                exc_info=True
            )
            
            metrics.dlq_send_failures.labels(
                original_topic=failed_message.original_topic,
                error_type=failed_message.error_type.value
            ).inc()
            
            raise RuntimeError(f"Failed to send message to DLQ: {str(e)}")
    
    def _dlq_delivery_callback(self, err, msg):
        """Callback for DLQ message delivery"""
        if err is not None:
            self.logger.error(
                "DLQ message delivery failed",
                topic=msg.topic() if msg else "unknown",
                error=str(err)
            )
        else:
            self.logger.debug(
                "DLQ message delivered successfully",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
    
    def get_dlq_topic_name(self, original_topic: str) -> str:
        """Generate DLQ topic name"""
        return f"{original_topic}{self.dlq_config.topic_suffix}"
    
    def get_retry_topic_name(self, original_topic: str, retry_level: int = 1) -> str:
        """Generate retry topic name"""
        return f"{original_topic}{self.dlq_config.retry_topic_suffix}.{retry_level}"
    
    def close(self):
        """Close DLQ producer"""
        if self.dlq_producer:
            try:
                self.dlq_producer.flush(10)
                self.logger.info("DLQ producer closed")
            except Exception as e:
                self.logger.error("Error closing DLQ producer", error=str(e))


class ErrorHandler:
    """Main error handler coordinating DLQ and retry logic"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.dlq_handler = DLQHandler(config)
        self.final_failure_handler = FinalFailureHandler()
        self.logger = logger.bind(component="error_handler")
        self._retry_counts = {}  # Track retries per correlation_id
    
    def handle_message_error(
        self,
        error: Exception,
        message_data: Dict[str, Any],
        query_config: QueryConfig,
        key: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> bool:
        """
        Handle message-level errors
        
        Returns:
            bool: True if error was handled (message should be skipped),
                  False if error should cause transaction abort
        """
        error_type = ErrorClassifier.classify_error(error)
        
        # Create logger with correlation ID context
        contextual_logger = self.logger.bind(
            correlation_id=correlation_id,
            query_id=query_config.id
        )
        
        contextual_logger.warning(
            "Message processing failed",
            error_type=error_type.value,
            error=str(error)
        )
        
        # Record error metrics
        metrics.message_processing_errors.labels(
            query_id=query_config.id,
            error_type=error_type.value,
            topic=query_config.target_topic
        ).inc()
        
        if error_type == ErrorType.PERMANENT:
            # Send permanent errors to DLQ
            failed_message = FailedMessage(
                original_payload=message_data,
                original_topic=query_config.target_topic,
                key=key,
                query_id=query_config.id,
                correlation_id=correlation_id or "unknown",
                error_type=error_type,
                error_reason=type(error).__name__,
                error_details=str(error),
                failure_timestamp=datetime.now(timezone.utc).isoformat(),
                producer_hostname=self.dlq_handler.hostname,
                retry_attempt=0
            )
            
            try:
                dlq_topic = self.dlq_handler.get_dlq_topic_name(query_config.target_topic)
                self.dlq_handler.send_to_dlq(failed_message, dlq_topic)
                return True  # Message handled, continue with batch
            except Exception as dlq_error:
                # Track retry attempts
                retry_count = self._retry_counts.get(correlation_id, 0) + 1
                self._retry_counts[correlation_id] = retry_count
                
                contextual_logger.error(
                    "Critical: Failed to send message to DLQ",
                    dlq_error=str(dlq_error),
                    original_error=str(error),
                    retry_count=retry_count
                )
                
                # If this is a circuit breaker error or we've hit max retries
                if isinstance(dlq_error, RuntimeError) and "circuit breaker" in str(dlq_error):
                    # Circuit breaker is open, let transaction abort and retry later
                    return False
                elif retry_count >= 3:  # Max DLQ retry attempts
                    # Final failure - quarantine the message
                    contextual_logger.critical(
                        "Message failed all DLQ attempts, quarantining",
                        retry_count=retry_count
                    )
                    
                    quarantined = self.final_failure_handler.handle_final_failure(
                        message_data=message_data,
                        query_config=query_config,
                        error=error,
                        correlation_id=correlation_id or "unknown",
                        retry_count=retry_count
                    )
                    
                    if quarantined:
                        # Message quarantined, continue with batch
                        # This prevents one bad message from blocking everything
                        return True
                    else:
                        # Even quarantine failed, abort transaction
                        return False
                else:
                    # Still have retries left, abort transaction to retry
                    return False
        
        elif error_type == ErrorType.TRANSIENT:
            # For transient errors, let the outer retry mechanism handle it
            contextual_logger.info(
                "Transient error detected, will be retried by outer mechanism",
                error=str(error)
            )
            return False  # Don't handle here, let transaction abort and retry
        
        else:  # CIRCUIT_BREAKER
            contextual_logger.error(
                "Circuit breaker triggered, aborting batch processing",
                circuit_breaker_state="OPEN",
                impact="transaction_will_abort",
                data_safety="preserved_no_checkpoint_update"
            )
            return False
    
    def close(self):
        """Close error handler resources"""
        self.dlq_handler.close()