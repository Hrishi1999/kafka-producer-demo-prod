"""Handler for messages that fail after all retry attempts"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import structlog

from ..models import QueryConfig
from ..utils.monitoring import metrics


logger = structlog.get_logger(__name__)


class FinalFailureHandler:
    """
    Handles messages that fail after all retry attempts
    
    Options for handling final failures:
    1. Write to local quarantine file
    2. Send alert/notification
    3. Log with critical severity
    4. Update metrics for monitoring
    """
    
    def __init__(self, quarantine_dir: str = "quarantine"):
        self.quarantine_dir = Path(quarantine_dir)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger.bind(component="final_failure_handler")
    
    def handle_final_failure(
        self,
        message_data: Dict[str, Any],
        query_config: QueryConfig,
        error: Exception,
        correlation_id: str,
        retry_count: int
    ) -> bool:
        """
        Handle a message that has failed all retry attempts
        
        Returns:
            bool: True if handled successfully, False otherwise
        """
        try:
            # Create quarantine record
            quarantine_record = {
                "correlation_id": correlation_id,
                "query_id": query_config.id,
                "topic": query_config.target_topic,
                "message_data": message_data,
                "final_error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "traceback": self._get_traceback(error)
                },
                "retry_count": retry_count,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "host": os.uname().nodename,
                "action_required": "manual_investigation"
            }
            
            # Write to quarantine file
            quarantine_file = self._write_to_quarantine(quarantine_record)
            
            # Log critical error
            self.logger.critical(
                "Message quarantined after all retries failed",
                correlation_id=correlation_id,
                query_id=query_config.id,
                topic=query_config.target_topic,
                retry_count=retry_count,
                quarantine_file=str(quarantine_file),
                error_type=type(error).__name__,
                error_message=str(error)
            )
            
            # Update metrics
            metrics.messages_quarantined.labels(
                query_id=query_config.id,
                topic=query_config.target_topic,
                error_type=type(error).__name__
            ).inc()
            
            metrics.final_failures.labels(
                query_id=query_config.id,
                topic=query_config.target_topic,
                failure_stage="quarantined"
            ).inc()
            
            # TODO: Send alert (email, Slack, PagerDuty, etc.)
            # self._send_alert(quarantine_record)
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to handle final failure",
                correlation_id=correlation_id,
                error=str(e)
            )
            return False
    
    def _write_to_quarantine(self, record: Dict[str, Any]) -> Path:
        """Write failed message to quarantine file"""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        correlation_id = record.get("correlation_id", "unknown")
        filename = f"{timestamp}_{correlation_id}.json"
        
        filepath = self.quarantine_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(record, f, indent=2, default=str)
        
        return filepath
    
    def _get_traceback(self, error: Exception) -> Optional[str]:
        """Get exception traceback if available"""
        import traceback
        try:
            return traceback.format_exc()
        except:
            return None
    
    def get_quarantined_messages(self, limit: int = 100) -> list:
        """Retrieve quarantined messages for investigation"""
        messages = []
        files = sorted(self.quarantine_dir.glob("*.json"), reverse=True)[:limit]
        
        for file in files:
            try:
                with open(file, 'r') as f:
                    messages.append(json.load(f))
            except Exception as e:
                self.logger.warning(f"Failed to read quarantine file: {file}", error=str(e))
        
        return messages
    
    def replay_quarantined_message(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a quarantined message for manual replay
        
        Args:
            correlation_id: The correlation ID of the message to replay
            
        Returns:
            The quarantined message data or None if not found
        """
        for file in self.quarantine_dir.glob(f"*_{correlation_id}.json"):
            try:
                with open(file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Failed to read quarantine file: {file}", error=str(e))
        
        return None


class RetryPolicy:
    """
    Configurable retry policy for handling failures
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        max_retry_time_seconds: int = 3600,  # 1 hour
        exponential_base: int = 2
    ):
        self.max_retries = max_retries
        self.max_retry_time_seconds = max_retry_time_seconds
        self.exponential_base = exponential_base
    
    def should_retry(self, retry_count: int, first_failure_time: datetime) -> bool:
        """Determine if we should retry based on count and time"""
        if retry_count >= self.max_retries:
            return False
        
        elapsed_seconds = (datetime.now(timezone.utc) - first_failure_time).total_seconds()
        if elapsed_seconds > self.max_retry_time_seconds:
            return False
        
        return True
    
    def get_retry_delay(self, retry_count: int) -> int:
        """Calculate exponential backoff delay in seconds"""
        return min(
            self.exponential_base ** retry_count,
            300  # Max 5 minutes between retries
        )