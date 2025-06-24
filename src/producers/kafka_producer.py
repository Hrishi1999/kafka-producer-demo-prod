"""Kafka producer with error handling and metrics using Confluent Cloud with Avro"""

import json
import time
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
import decimal

import structlog
from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from tenacity import retry, stop_after_attempt, wait_exponential

from ..models import KafkaConfig, QueryConfig
from ..utils.monitoring import metrics
from ..utils.avro_schema import AvroSchemaGenerator
from .error_handler import ErrorHandler


logger = structlog.get_logger(__name__)


class KafkaProducerWrapper:
    """Production-grade Kafka producer with monitoring and error handling for Confluent Cloud"""
    
    def __init__(self, config: KafkaConfig, executor: Optional[ThreadPoolExecutor] = None):
        self.config = config
        self.executor = executor or ThreadPoolExecutor(max_workers=4)
        self.producer = self._create_producer()
        self.logger = logger.bind(component="kafka_producer")
        self._pending_messages = 0
        self.transactions_enabled = config.enable_transactions
        self._transaction_active = False
        
        # Initialize Schema Registry client and serializers
        self.schema_registry_client = self._create_schema_registry_client()
        self.key_serializer = StringSerializer('utf_8')
        self._avro_serializers = {}  # Cache for topic-specific serializers
        
        # Initialize error handler
        self.error_handler = ErrorHandler(config)
        
        # Initialize transactions if enabled
        if self.transactions_enabled:
            self._init_transactions()
    
    def _serialize_row_data(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Python types to Avro-compatible types"""
        serialized_row = {}
        
        for key, value in row.items():
            if value is None:
                serialized_row[key] = None
            elif isinstance(value, dict):
                # Keep dictionaries as-is (like _metadata)
                serialized_row[key] = value
            elif isinstance(value, (list, tuple)):
                # Keep lists/tuples as-is
                serialized_row[key] = list(value)
            elif isinstance(value, (date, datetime)):
                # Convert dates to ISO format strings
                if isinstance(value, datetime):
                    serialized_row[key] = value.isoformat()
                else:
                    serialized_row[key] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                # Convert Decimal to float
                serialized_row[key] = float(value)
            elif isinstance(value, (int, float, str, bool)):
                # These types are already compatible
                serialized_row[key] = value
            else:
                # Convert other types to string
                serialized_row[key] = str(value)
        
        return serialized_row
    
    def _create_producer(self) -> Producer:
        """Create Confluent Kafka producer instance"""
        producer_config = self.config.get_producer_config()
        
        try:
            producer = Producer(producer_config)
            self.logger.info("Confluent Kafka producer created", config=self.config.client_id)
            return producer
        except Exception as e:
            self.logger.error("Failed to create Kafka producer", error=str(e))
            raise
    
    def _create_schema_registry_client(self) -> Optional[SchemaRegistryClient]:
        """Create Schema Registry client if configured"""
        sr_config = self.config.get_schema_registry_config()
        
        if not sr_config:
            self.logger.warning("Schema Registry not configured, falling back to JSON serialization")
            return None
        
        try:
            client = SchemaRegistryClient(sr_config)
            # Test connection by attempting to get subjects
            client.get_subjects()
            self.logger.info("Schema Registry client created and tested", url=sr_config.get("url"))
            return client
        except Exception as e:
            self.logger.error("Failed to create or connect to Schema Registry", error=str(e))
            raise RuntimeError(f"Schema Registry is configured but unavailable: {str(e)}")
    
    def _init_transactions(self):
        """Initialize Kafka transactions"""
        try:
            self.producer.init_transactions(timeout=30)
            self.logger.info("Kafka transactions initialized", transactional_id=self.config.transactional_id)
        except Exception as e:
            self.logger.error("Failed to initialize transactions", error=str(e))
            raise RuntimeError(f"Failed to initialize Kafka transactions: {str(e)}")
    
    def begin_transaction(self):
        """Begin a new transaction"""
        if not self.transactions_enabled:
            raise RuntimeError("Transactions are not enabled")
        
        if self._transaction_active:
            raise RuntimeError("Transaction is already active")
        
        try:
            self.producer.begin_transaction()
            self._transaction_active = True
            self.logger.debug("Transaction started")
        except Exception as e:
            self.logger.error("Failed to begin transaction", error=str(e))
            raise
    
    def commit_transaction(self):
        """Commit the current transaction"""
        if not self.transactions_enabled:
            raise RuntimeError("Transactions are not enabled")
        
        if not self._transaction_active:
            raise RuntimeError("No active transaction to commit")
        
        try:
            self.producer.commit_transaction(timeout=30)
            self._transaction_active = False
            self.logger.debug("Transaction committed")
        except Exception as e:
            self.logger.error("Failed to commit transaction", error=str(e))
            self._transaction_active = False
            raise
    
    def abort_transaction(self):
        """Abort the current transaction"""
        if not self.transactions_enabled:
            raise RuntimeError("Transactions are not enabled")
        
        if not self._transaction_active:
            self.logger.warning("No active transaction to abort")
            return
        
        try:
            self.producer.abort_transaction()
            self._transaction_active = False
            self.logger.debug("Transaction aborted")
        except Exception as e:
            self.logger.error("Failed to abort transaction", error=str(e))
            self._transaction_active = False
            raise
    
    def _get_avro_serializer(self, topic: str, sample_row: Dict[str, Any]) -> Optional[AvroSerializer]:
        """Get or create Avro serializer for a topic"""
        if not self.schema_registry_client:
            return None
        
        if topic in self._avro_serializers:
            return self._avro_serializers[topic]
        
        try:
            # Generate schema from sample row
            schema_name = f"{topic.replace('.', '_').replace('-', '_')}_value"
            avro_schema = AvroSchemaGenerator.generate_schema_from_row(
                sample_row=sample_row,
                schema_name=schema_name,
                namespace="sql.avro"
            )
            
            # Create serializer
            serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=avro_schema,
                to_dict=lambda obj, _: obj  # Objects are already dicts
            )
            
            self._avro_serializers[topic] = serializer
            self.logger.info("Created Avro serializer for topic", topic=topic, schema_name=schema_name)
            return serializer
            
        except Exception as e:
            self.logger.error("Failed to create Avro serializer", topic=topic, error=str(e))
            # When Schema Registry is configured, serializer creation failure should be fatal
            raise RuntimeError(f"Failed to create Avro serializer for topic {topic}: {str(e)}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        self._pending_messages -= 1
        
        if err is not None:
            self.logger.error(
                "Message delivery failed",
                topic=msg.topic(),
                partition=msg.partition(),
                error=str(err)
            )
            
            metrics.kafka_messages_sent.labels(
                topic=msg.topic(),
                status="error"
            ).inc()
        else:
            self.logger.debug(
                "Message delivered successfully",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
            
            metrics.kafka_messages_sent.labels(
                topic=msg.topic(),
                status="success"
            ).inc()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def produce_query_results(
        self,
        query_config: QueryConfig,
        rows: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Produce query results to Kafka using Confluent Cloud"""
        start_time = time.time()
        messages_sent = 0
        errors = []
        
        try:
            # Get Avro serializer for this topic (using first row as sample)
            avro_serializer = None
            if rows:
                avro_serializer = self._get_avro_serializer(query_config.target_topic, rows[0])
            
            # Process rows in batches
            for row in rows:
                # Generate correlation ID for this message
                correlation_id = str(uuid.uuid4())
                
                # Build message key
                key = None
                if query_config.key_column and query_config.key_column in row:
                    key = str(row[query_config.key_column])
                
                # Add metadata and convert dates to strings
                row_with_metadata = self._serialize_row_data(row.copy())
                row_with_metadata["_metadata"] = {
                    "query_id": query_config.id,
                    "extracted_at": datetime.now(datetime.timezone.utc).isoformat(),
                    "producer": "sql-kafka-producer"
                }
                
                # Serialize value
                try:
                    if self.schema_registry_client:
                        # Schema Registry is configured, Avro is required
                        if not avro_serializer:
                            raise RuntimeError(f"Failed to create Avro serializer for topic {query_config.target_topic}")
                        
                        # Use Avro serialization
                        serialized_key = self.key_serializer(key, SerializationContext(query_config.target_topic, MessageField.KEY)) if key else None
                        serialized_value = avro_serializer(row_with_metadata, SerializationContext(query_config.target_topic, MessageField.VALUE))
                        
                        # Produce message with serialized data and correlation ID
                        self.producer.produce(
                            topic=query_config.target_topic,
                            key=serialized_key,
                            value=serialized_value,
                            headers={"x-correlation-id": correlation_id},
                            callback=self._delivery_callback
                        )
                    else:
                        # No Schema Registry configured, use JSON serialization
                        value = json.dumps(row_with_metadata, default=str)
                        
                        self.producer.produce(
                            topic=query_config.target_topic,
                            key=key,
                            value=value,
                            headers={"x-correlation-id": correlation_id},
                            callback=self._delivery_callback
                        )
                    
                    self._pending_messages += 1
                    messages_sent += 1
                    
                    # Poll for delivery callbacks periodically
                    if messages_sent % 100 == 0:
                        self.producer.poll(0)
                    
                except KafkaException as e:
                    error_msg = f"Failed to produce message: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error("Kafka produce error", error=error_msg)
                    
                except Exception as e:
                    error_msg = f"Failed to serialize message: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error("Serialization error", error=error_msg)
            
            # Wait for all messages to be delivered
            self.producer.flush(timeout=30)
            
            # Record metrics
            duration = time.time() - start_time
            metrics.batch_processing_duration.labels(
                query_id=query_config.id
            ).observe(duration)
            
            result = {
                "query_id": query_config.id,
                "rows_processed": len(rows),
                "messages_sent": messages_sent,
                "errors": len(errors),
                "duration_seconds": duration,
            }
            
            self.logger.info("Batch processing completed", **result)
            return result
            
        except Exception as e:
            self.logger.error(
                "Batch processing failed",
                query_id=query_config.id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def flush(self, timeout: int = 30):
        """Flush pending messages"""
        try:
            self.producer.flush(timeout)
            self.logger.debug("Producer flushed")
        except Exception as e:
            self.logger.error("Error flushing producer", error=str(e))
    
    def close(self):
        """Close producer and cleanup resources"""
        try:
            # Wait for pending messages
            if self._pending_messages > 0:
                self.logger.info(f"Waiting for {self._pending_messages} pending messages")
                self.producer.flush(30)
            
            # Close producer
            if self.producer:
                self.producer.flush(10)
                # Note: confluent-kafka Producer doesn't have explicit close()
                
            if self.executor:
                self.executor.shutdown(wait=True)
            
            # Close error handler
            if self.error_handler:
                self.error_handler.close()
                
            self.logger.info("Kafka producer closed")
        except Exception as e:
            self.logger.error("Error closing producer", error=str(e))
    
    def produce_query_results_transactional(
        self,
        query_config: QueryConfig,
        rows: List[Dict[str, Any]],
        state_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """Produce query results to Kafka using transactions for exactly-once semantics with DLQ support"""
        if not self.transactions_enabled:
            # Fall back to regular produce if transactions aren't enabled
            return self.produce_query_results(query_config, rows)
        
        start_time = time.time()
        messages_sent = 0
        messages_dlq = 0
        successful_rows = []
        
        try:
            # Begin transaction
            self.begin_transaction()
            
            # Get Avro serializer for this topic (using first row as sample)
            avro_serializer = None
            if rows:
                avro_serializer = self._get_avro_serializer(query_config.target_topic, rows[0])
            
            # Process rows within transaction
            for row in rows:
                # Build message key
                key = None
                if query_config.key_column and query_config.key_column in row:
                    key = str(row[query_config.key_column])
                
                # Generate correlation ID for this message
                correlation_id = str(uuid.uuid4())
                
                # Add metadata and convert dates to strings
                row_with_metadata = self._serialize_row_data(row.copy())
                row_with_metadata["_metadata"] = {
                    "query_id": query_config.id,
                    "extracted_at": datetime.now(datetime.timezone.utc).isoformat(),
                    "producer": "sql-kafka-producer"
                }
                
                # Try to serialize and produce message
                try:
                    if self.schema_registry_client:
                        # Schema Registry is configured, Avro is required
                        if not avro_serializer:
                            raise RuntimeError(f"Failed to create Avro serializer for topic {query_config.target_topic}")
                        
                        # Use Avro serialization
                        serialized_key = self.key_serializer(key, SerializationContext(query_config.target_topic, MessageField.KEY)) if key else None
                        serialized_value = avro_serializer(row_with_metadata, SerializationContext(query_config.target_topic, MessageField.VALUE))
                        
                        # Produce message with serialized data and correlation ID in headers
                        self.producer.produce(
                            topic=query_config.target_topic,
                            key=serialized_key,
                            value=serialized_value,
                            headers={"x-correlation-id": correlation_id},
                            callback=self._delivery_callback
                        )
                    else:
                        # No Schema Registry configured, use JSON serialization
                        value = json.dumps(row_with_metadata, default=str)
                        
                        self.producer.produce(
                            topic=query_config.target_topic,
                            key=key,
                            value=value,
                            headers={"x-correlation-id": correlation_id},
                            callback=self._delivery_callback
                        )
                    
                    self._pending_messages += 1
                    messages_sent += 1
                    successful_rows.append(row)
                    
                    # Poll for delivery callbacks periodically
                    if messages_sent % 100 == 0:
                        self.producer.poll(0)
                    
                except Exception as e:
                    # Handle message-level error with correlation ID
                    should_continue = self.error_handler.handle_message_error(
                        error=e,
                        message_data=row,
                        query_config=query_config,
                        key=key,
                        correlation_id=correlation_id
                    )
                    
                    if should_continue:
                        # Error was handled (sent to DLQ), continue with batch
                        messages_dlq += 1
                        self.logger.debug(
                            "Message error handled, continuing with batch",
                            query_id=query_config.id,
                            correlation_id=correlation_id,
                            key=key
                        )
                    else:
                        # Error requires transaction abort
                        self.logger.error(
                            "Critical error during message processing, aborting transaction",
                            query_id=query_config.id,
                            error=str(e)
                        )
                        metrics.transaction_aborts.labels(
                            query_id=query_config.id,
                            reason="message_processing_error"
                        ).inc()
                        raise
            
            # Flush messages before committing
            self.producer.flush(timeout=30)
            
            # Update state if callback provided (e.g., checkpoint state)
            # Only update state for successfully processed rows
            if state_callback:
                state_callback()
            
            # Commit transaction
            self.commit_transaction()
            
            # Record metrics
            duration = time.time() - start_time
            metrics.batch_processing_duration.labels(
                query_id=query_config.id
            ).observe(duration)
            
            result = {
                "query_id": query_config.id,
                "rows_processed": len(rows),
                "messages_sent": messages_sent,
                "messages_dlq": messages_dlq,
                "successful_rows": len(successful_rows),
                "duration_seconds": duration,
                "transaction_committed": True,
            }
            
            self.logger.info("Transactional batch processing completed", **result)
            return result
            
        except Exception as e:
            # Abort transaction on any error
            try:
                self.abort_transaction()
                self.logger.error("Transaction aborted due to error", error=str(e))
                metrics.transaction_aborts.labels(
                    query_id=query_config.id,
                    reason="exception"
                ).inc()
            except Exception as abort_error:
                self.logger.error("Failed to abort transaction", error=str(abort_error))
            
            self.logger.error(
                "Transactional batch processing failed",
                query_id=query_config.id,
                error=str(e),
                exc_info=True
            )
            raise