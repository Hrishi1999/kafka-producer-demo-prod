from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
import os
import re


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    group_id: str = Field(..., description="Consumer group ID")
    topics: List[str] = Field(..., description="Topics to consume from")
    
    # Consumer settings
    isolation_level: str = Field("read_committed", description="Isolation level")
    enable_auto_commit: bool = Field(False, description="Enable auto commit")
    auto_offset_reset: str = Field("latest", description="Auto offset reset")
    max_poll_records: int = Field(500, description="Max records per poll")
    session_timeout_ms: int = Field(30000, description="Session timeout in ms")
    heartbeat_interval_ms: int = Field(3000, description="Heartbeat interval in ms")
    
    # Security
    security_protocol: str = Field("SASL_SSL", description="Security protocol")
    sasl_mechanism: str = Field("PLAIN", description="SASL mechanism")
    sasl_username: Optional[str] = Field(None, description="SASL username")
    sasl_password: Optional[str] = Field(None, description="SASL password")
    
    # Schema Registry (optional)
    schema_registry_url: Optional[str] = Field(None, description="Schema Registry URL")
    schema_registry_auth: Optional[str] = Field(None, description="Schema Registry auth")
    
    # Confluent Cloud API for metrics
    confluent_api_key: Optional[str] = Field(None, description="Confluent Cloud API key")
    confluent_api_secret: Optional[str] = Field(None, description="Confluent Cloud API secret")
    cluster_id: Optional[str] = Field(None, description="Kafka cluster ID")


class HTTPConfig(BaseModel):
    endpoint_url: str = Field(..., description="HTTP endpoint URL")
    timeout_seconds: int = Field(30, description="HTTP timeout in seconds")
    max_retries: int = Field(3, description="Max HTTP retries")
    retry_delay_seconds: List[int] = Field([1, 2, 4], description="Retry delays")
    
    # Circuit breaker
    circuit_breaker_enabled: bool = Field(True, description="Enable circuit breaker")
    failure_threshold: int = Field(5, description="Failure threshold")
    recovery_timeout_seconds: int = Field(60, description="Recovery timeout")
    
    # Headers
    headers: Dict[str, str] = Field(default_factory=dict, description="Default headers")


class RetryConfig(BaseModel):
    enabled: bool = Field(True, description="Enable retry topic")
    topic_suffix: str = Field(".retry", description="Retry topic suffix")
    max_retries: int = Field(1, description="Max retry attempts")


class DLQConfig(BaseModel):
    enabled: bool = Field(True, description="Enable DLQ")
    topic_suffix: str = Field(".dlq", description="DLQ topic suffix")
    include_error_context: bool = Field(True, description="Include error context")


class MonitoringConfig(BaseModel):
    enabled: bool = Field(True, description="Enable monitoring")
    port: int = Field(8081, description="Health check port")
    metrics_enabled: bool = Field(True, description="Enable Prometheus metrics")


class ConsumerConfig(BaseModel):
    kafka: KafkaConfig
    http: HTTPConfig
    retry: RetryConfig = RetryConfig()
    dlq: DLQConfig = DLQConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    
    @validator('*', pre=True, always=True)
    def substitute_env_vars(cls, v):
        if isinstance(v, str):
            def replace_env_var(match):
                env_var = match.group(1)
                return os.getenv(env_var, match.group(0))
            
            return re.sub(r'\$\{([^}]+)\}', replace_env_var, v)
        return v