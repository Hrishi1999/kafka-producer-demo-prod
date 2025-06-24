from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
import logging
import json
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass

from ..models.config import KafkaConfig, RetryConfig, DLQConfig


@dataclass
class MessageContext:
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Dict[str, Any]
    headers: Dict[str, str]


class KafkaConsumerWrapper:
    def __init__(self, kafka_config: KafkaConfig, retry_config: RetryConfig, dlq_config: DLQConfig):
        self.kafka_config = kafka_config
        self.retry_config = retry_config
        self.dlq_config = dlq_config
        self.logger = logging.getLogger(__name__)
        
        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': kafka_config.bootstrap_servers,
            'group.id': kafka_config.group_id,
            'isolation.level': kafka_config.isolation_level,
            'enable.auto.commit': kafka_config.enable_auto_commit,
            'auto.offset.reset': kafka_config.auto_offset_reset,
            'max.poll.interval.ms': kafka_config.session_timeout_ms,
            'session.timeout.ms': kafka_config.session_timeout_ms,
            'heartbeat.interval.ms': kafka_config.heartbeat_interval_ms,
        }
        
        # Add security config if provided
        if kafka_config.security_protocol:
            consumer_conf['security.protocol'] = kafka_config.security_protocol
        if kafka_config.sasl_mechanism:
            consumer_conf['sasl.mechanism'] = kafka_config.sasl_mechanism
        if kafka_config.sasl_username:
            consumer_conf['sasl.username'] = kafka_config.sasl_username
        if kafka_config.sasl_password:
            consumer_conf['sasl.password'] = kafka_config.sasl_password
        
        self.consumer = Consumer(consumer_conf)
        
        # Producer for retry/DLQ (same config as consumer)
        producer_conf = consumer_conf.copy()
        producer_conf.pop('group.id', None)
        producer_conf.pop('isolation.level', None)
        producer_conf.pop('enable.auto.commit', None)
        producer_conf.pop('auto.offset.reset', None)
        producer_conf.pop('max.poll.interval.ms', None)
        producer_conf.pop('session.timeout.ms', None)
        producer_conf.pop('heartbeat.interval.ms', None)
        
        self.producer = Producer(producer_conf)
        
        # Initialize Schema Registry client and deserializers
        self.schema_registry_client = self._create_schema_registry_client()
        self.key_deserializer = StringDeserializer('utf_8')
        self._avro_deserializers = {}  # Cache for topic-specific deserializers
        
        # Subscribe to topics
        self.consumer.subscribe(kafka_config.topics)
        self.logger.info(f"Subscribed to topics: {kafka_config.topics}")
    
    def _create_schema_registry_client(self) -> SchemaRegistryClient:
        """Create Schema Registry client - required for Avro-only operation"""
        if not self.kafka_config.schema_registry_url:
            raise RuntimeError("Schema Registry URL is required for Avro-only consumer")
        
        sr_config = {
            'url': self.kafka_config.schema_registry_url
        }
        
        if self.kafka_config.schema_registry_auth:
            sr_config['basic.auth.user.info'] = self.kafka_config.schema_registry_auth
        
        try:
            client = SchemaRegistryClient(sr_config)
            # Test connection by attempting to get subjects
            client.get_subjects()
            self.logger.info("Schema Registry client created and tested", url=sr_config.get("url"))
            return client
        except Exception as e:
            self.logger.error("Failed to create or connect to Schema Registry", error=str(e))
            raise RuntimeError(f"Schema Registry is required but unavailable: {str(e)}")
    
    def _get_avro_deserializer(self, topic: str) -> AvroDeserializer:
        """Get or create Avro deserializer for a topic"""
        if topic in self._avro_deserializers:
            return self._avro_deserializers[topic]
        
        try:
            # Create deserializer that will auto-fetch schema from registry
            deserializer = AvroDeserializer(
                schema_registry_client=self.schema_registry_client,
                from_dict=lambda obj, _: obj  # Return as dict
            )
            
            self._avro_deserializers[topic] = deserializer
            self.logger.info("Created Avro deserializer for topic", topic=topic)
            return deserializer
        except Exception as e:
            self.logger.error("Failed to create Avro deserializer", topic=topic, error=str(e))
            raise RuntimeError(f"Failed to create Avro deserializer for topic {topic}: {str(e)}")
    
    def poll_message(self, timeout: float = 1.0) -> Optional[MessageContext]:
        msg = self.consumer.poll(timeout)
        
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            else:
                raise KafkaException(msg.error())
        
        try:
            # Deserialize using Avro only
            value = self._deserialize_message_value(msg)
            
            # Extract headers
            headers = {}
            if msg.headers():
                for header_name, header_value in msg.headers():
                    headers[header_name] = header_value.decode('utf-8') if header_value else ""
            
            return MessageContext(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key().decode('utf-8') if msg.key() else None,
                value=value,
                headers=headers
            )
        
        except Exception as e:
            self.logger.error(f"Failed to deserialize message: {e}")
            # Send to DLQ immediately for deserialization errors
            self._send_to_dlq_raw(msg, f"Deserialization error: {e}")
            self.consumer.commit(msg)
            return None
    
    def _deserialize_message_value(self, msg) -> Dict[str, Any]:
        """Deserialize message value using Avro only"""
        if not msg.value():
            return {}
        
        # Get Avro deserializer
        avro_deserializer = self._get_avro_deserializer(msg.topic())
        if not avro_deserializer:
            raise RuntimeError(f"No Avro deserializer available for topic {msg.topic()}")
        
        # Deserialize using Avro
        ctx = SerializationContext(msg.topic(), MessageField.VALUE, msg.headers())
        avro_data = avro_deserializer(msg.value(), ctx)
        
        if avro_data:
            self.logger.debug("Successfully deserialized Avro message", topic=msg.topic())
            return avro_data
        else:
            raise ValueError(f"Avro deserialization returned empty data for topic {msg.topic()}")
    
    def commit_message(self, message_context: MessageContext):
        try:
            self.consumer.commit(asynchronous=False)
            self.logger.debug(f"Committed offset {message_context.offset} for {message_context.topic}:{message_context.partition}")
        except Exception as e:
            self.logger.error(f"Failed to commit offset: {e}")
            raise
    
    def send_to_retry(self, message_context: MessageContext, error_message: str, correlation_id: str = None):
        if not self.retry_config.enabled:
            self.send_to_dlq(message_context, error_message, correlation_id)
            return
        
        retry_topic = f"{message_context.topic}{self.retry_config.topic_suffix}"
        
        retry_payload = {
            "original_topic": message_context.topic,
            "original_partition": message_context.partition,
            "original_offset": message_context.offset,
            "retry_count": message_context.headers.get("retry_count", "0"),
            "error_message": error_message,
            "original_data": message_context.value,
            "correlation_id": correlation_id
        }
        
        # Preserve correlation ID in headers
        headers = {"retry_count": str(int(message_context.headers.get("retry_count", "0")) + 1)}
        if correlation_id:
            headers["x-correlation-id"] = correlation_id
        
        self.producer.produce(
            topic=retry_topic,
            key=message_context.key,
            value=json.dumps(retry_payload),
            headers=headers
        )
        self.producer.flush()
        self.logger.info(f"Sent message to retry topic: {retry_topic}")
    
    def send_to_dlq(self, message_context: MessageContext, error_message: str, correlation_id: str = None):
        if not self.dlq_config.enabled:
            self.logger.warning(f"DLQ disabled, discarding failed message: {error_message}")
            return
        
        dlq_topic = f"{message_context.topic}{self.dlq_config.topic_suffix}"
        
        if self.dlq_config.include_error_context:
            dlq_payload = {
                "original_topic": message_context.topic,
                "original_partition": message_context.partition,
                "original_offset": message_context.offset,
                "error_message": error_message,
                "error_timestamp": int(time.time()),
                "original_data": message_context.value,
                "original_headers": message_context.headers,
                "correlation_id": correlation_id
            }
        else:
            dlq_payload = message_context.value
        
        # Preserve correlation ID in headers
        headers = {}
        if correlation_id:
            headers["x-correlation-id"] = correlation_id
        
        self.producer.produce(
            topic=dlq_topic,
            key=message_context.key,
            value=json.dumps(dlq_payload),
            headers=headers
        )
        self.producer.flush()
        self.logger.info(f"Sent message to DLQ: {dlq_topic}")
    
    def _send_to_dlq_raw(self, msg, error_message: str):
        dlq_topic = f"{msg.topic()}{self.dlq_config.topic_suffix}"
        
        dlq_payload = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "error_message": error_message,
            "error_timestamp": int(time.time()),
            "raw_data": msg.value().decode('utf-8', errors='replace') if msg.value() else "",
        }
        
        self.producer.produce(
            topic=dlq_topic,
            key=msg.key(),
            value=json.dumps(dlq_payload)
        )
        self.producer.flush()
        self.logger.warning(f"Sent raw message to DLQ: {dlq_topic}")
    
    def close(self):
        self.consumer.close()
        self.producer.flush()
        self.logger.info("Kafka consumer and producer closed")