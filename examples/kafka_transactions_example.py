"""Example demonstrating Kafka transactions usage in the SQL producer"""

import logging
from datetime import datetime, timezone

from src.models.config import KafkaConfig, QueryConfig
from src.producers.kafka_producer import KafkaProducerWrapper
from src.core.state import StateManager

# Configure logging
logging.basicConfig(level=logging.INFO)

def example_transactional_producer():
    """Example of using Kafka transactions with the SQL producer"""
    
    # Configure Kafka with transactions enabled
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        enable_transactions=True,
        transactional_id="sql-producer-001",  # Must be unique per producer instance
        transaction_timeout_ms=60000,
        client_id="sql-kafka-producer-transactional"
    )
    
    # Create query config
    query_config = QueryConfig(
        id="sample_query",
        name="Sample Transactional Query",
        sql="SELECT id, name, created_at FROM users WHERE created_at > ?",
        target_topic="users.events",
        key_column="id",
        batch_size=1000
    )
    
    # Sample data (would normally come from SQL query)
    sample_rows = [
        {"id": 1, "name": "Alice", "created_at": "2025-01-01T00:00:00Z"},
        {"id": 2, "name": "Bob", "created_at": "2025-01-01T01:00:00Z"},
        {"id": 3, "name": "Charlie", "created_at": "2025-01-01T02:00:00Z"},
    ]
    
    # Initialize state manager
    state_manager = StateManager("transactional_state.json")
    state_manager.load()
    
    # Create producer
    producer = KafkaProducerWrapper(kafka_config)
    
    try:
        # Method 1: Basic transactional produce
        print("=== Basic Transactional Produce ===")
        result = producer.produce_query_results_transactional(
            query_config=query_config,
            rows=sample_rows
        )
        print(f"Transaction result: {result}")
        
        # Method 2: Transactional produce with state management
        print("\n=== Transactional Produce with State Management ===")
        timestamp = datetime.now(timezone.utc)
        
        # Use state manager's transactional context
        with state_manager.transactional_update(query_config.id, timestamp) as state:
            # Define state callback to be called within transaction
            def state_callback():
                # This will be called only if Kafka transaction commits
                print("State callback executed - transaction committed")
            
            result = producer.produce_query_results_transactional(
                query_config=query_config,
                rows=sample_rows,
                state_callback=state_callback
            )
            print(f"Transaction with state result: {result}")
        
        # Method 3: Manual transaction control
        print("\n=== Manual Transaction Control ===")
        try:
            producer.begin_transaction()
            
            # Produce messages manually
            for i, row in enumerate(sample_rows):
                row["manual_tx"] = True
                row["batch_id"] = i
                
                if producer.schema_registry_client:
                    # With Schema Registry
                    key = str(row["id"])
                    avro_serializer = producer._get_avro_serializer(query_config.target_topic, row)
                    serialized_value = avro_serializer(row, None)
                    producer.producer.produce(
                        topic=query_config.target_topic,
                        key=key,
                        value=serialized_value
                    )
                else:
                    # JSON serialization
                    import json
                    producer.producer.produce(
                        topic=query_config.target_topic,
                        key=str(row["id"]),
                        value=json.dumps(row, default=str)
                    )
            
            # Flush and commit
            producer.producer.flush(timeout=30)
            producer.commit_transaction()
            print("Manual transaction committed successfully")
            
        except Exception as e:
            print(f"Manual transaction failed: {e}")
            producer.abort_transaction()
            raise
        
        # Show state
        print(f"\nFinal state: {state_manager.get_state()}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        producer.close()

def example_transaction_failure_handling():
    """Example demonstrating transaction failure and rollback"""
    
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        enable_transactions=True,
        transactional_id="sql-producer-failure-test",
        client_id="failure-test-producer"
    )
    
    query_config = QueryConfig(
        id="failure_test_query",
        name="Failure Test Query",
        sql="SELECT * FROM test_table",
        target_topic="test.failures",
        batch_size=10
    )
    
    producer = KafkaProducerWrapper(kafka_config)
    state_manager = StateManager("failure_test_state.json")
    state_manager.load()
    
    try:
        # Simulate failure scenario
        sample_rows = [
            {"id": 1, "valid": True},
            {"id": 2, "valid": False, "cause_error": True},  # This will cause failure
            {"id": 3, "valid": True},
        ]
        
        timestamp = datetime.now(timezone.utc)
        
        with state_manager.transactional_update(query_config.id, timestamp):
            try:
                # Simulate error during transaction
                producer.begin_transaction()
                
                for row in sample_rows:
                    if row.get("cause_error"):
                        raise RuntimeError("Simulated processing error")
                    
                    # Normal processing would happen here
                    print(f"Processing row: {row}")
                
                producer.commit_transaction()
                print("Transaction committed")
                
            except Exception as e:
                print(f"Error in transaction: {e}")
                producer.abort_transaction()
                raise  # Re-raise to trigger state rollback
                
    except Exception as e:
        print(f"Transaction and state rolled back due to: {e}")
        
    finally:
        producer.close()

if __name__ == "__main__":
    print("Kafka Transactions Example")
    print("=" * 50)
    
    try:
        example_transactional_producer()
        print("\n" + "=" * 50)
        example_transaction_failure_handling()
        
    except Exception as e:
        print(f"Example failed: {e}")