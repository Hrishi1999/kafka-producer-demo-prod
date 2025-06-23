"""Configuration models using Pydantic for validation"""

import os
import re
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, SecretStr


class DatabaseConfig(BaseModel):
    """SQL Server database configuration"""
    
    host: str
    port: int = 1433
    database: str
    username: str
    password: SecretStr
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_server_certificate: bool = True
    encrypt: bool = True
    connection_timeout: int = 30
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 3600
    
    @property
    def connection_string(self) -> str:
        """Generate ODBC connection string"""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password.get_secret_value()};"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'};"
            f"Encrypt={'yes' if self.encrypt else 'no'};"
            f"Connection Timeout={self.connection_timeout};"
        )


class KafkaConfig(BaseModel):
    """Kafka producer configuration"""
    
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
    client_id: str = "sql-kafka-producer"
    acks: str = "all"
    retries: int = 3
    retry_backoff_ms: int = 1000
    compression_type: str = "gzip"
    batch_size: int = 16384
    linger_ms: int = 100
    
    # Transactional settings
    enable_transactions: bool = False
    transactional_id: Optional[str] = None
    transaction_timeout_ms: int = 60000
    
    # Schema Registry settings
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[SecretStr] = None
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get Confluent Kafka producer configuration dict"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "compression.type": self.compression_type,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
            # Confluent Cloud optimizations
            "enable.idempotence": True,
            "request.timeout.ms": 300000,
            "max.in.flight.requests.per.connection": 5,
        }
        
        # Add transactional settings if enabled
        if self.enable_transactions:
            if not self.transactional_id:
                raise ValueError("transactional_id is required when enable_transactions is True")
            config.update({
                "transactional.id": self.transactional_id,
                "transaction.timeout.ms": self.transaction_timeout_ms,
            })
        
        if self.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.security_protocol
            
            if self.sasl_mechanism:
                config["sasl.mechanism"] = self.sasl_mechanism
                config["sasl.username"] = self.sasl_username
                config["sasl.password"] = self.sasl_password.get_secret_value()
        
        return config
    
    def get_schema_registry_config(self) -> Dict[str, Any]:
        """Get Schema Registry client configuration"""
        if not self.schema_registry_url:
            return {}
            
        config = {
            "url": self.schema_registry_url
        }
        
        if self.schema_registry_username and self.schema_registry_password:
            config.update({
                "basic.auth.user.info": f"{self.schema_registry_username}:{self.schema_registry_password.get_secret_value()}"
            })
        
        return config


class QueryConfig(BaseModel):
    """Individual query configuration"""
    
    id: str
    name: str
    description: Optional[str] = None
    sql: str
    parameters: List[str] = Field(default_factory=list)
    target_topic: str
    key_column: Optional[str] = None
    batch_size: int = 1000
    fetch_size: int = 1000
    schedule: Optional[str] = None
    enabled: bool = True
    timeout_seconds: int = 300
    max_retries: int = 3


class MonitoringConfig(BaseModel):
    """Monitoring and metrics configuration"""
    
    enabled: bool = True
    metrics_port: int = 8080
    log_level: str = "INFO"
    log_format: str = "json"
    log_file: Optional[str] = "logs/sql-kafka-producer.log"
    max_log_file_size_mb: int = 10
    log_backup_count: int = 5


class AppConfig(BaseModel):
    """Main application configuration"""
    
    database: DatabaseConfig
    kafka: KafkaConfig
    queries: List[QueryConfig] = Field(default_factory=list)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    
    # Global settings
    state_file: str = "state.json"
    checkpoint_interval_seconds: int = 60
    worker_threads: int = 4
    
    @classmethod
    def from_yaml(cls, yaml_data: Dict[str, Any]) -> "AppConfig":
        """Create config from YAML data with environment variable substitution"""
        def substitute_env_vars(obj):
            """Recursively substitute environment variables"""
            if isinstance(obj, str):
                # Replace ${VAR_NAME} with environment variable
                pattern = r'\$\{([^}]+)\}'
                return re.sub(
                    pattern,
                    lambda m: os.environ.get(m.group(1), m.group(0)),
                    obj
                )
            elif isinstance(obj, dict):
                return {k: substitute_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [substitute_env_vars(item) for item in obj]
            return obj
        
        substituted_data = substitute_env_vars(yaml_data)
        return cls(**substituted_data)