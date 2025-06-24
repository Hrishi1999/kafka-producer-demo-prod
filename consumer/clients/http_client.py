import requests
import logging
import time
from typing import Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass

from ..models.config import HTTPConfig


class HTTPErrorType(Enum):
    PERMANENT = "permanent"  # 4xx errors
    TRANSIENT = "transient"  # 5xx errors, timeouts
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class HTTPResponse:
    status_code: int
    content: str
    error_type: Optional[HTTPErrorType] = None
    error_message: Optional[str] = None
    
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300
    
    def is_permanent_error(self) -> bool:
        return 400 <= self.status_code < 500
    
    def is_transient_error(self) -> bool:
        return self.status_code >= 500 or self.error_type == HTTPErrorType.TRANSIENT


class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: int):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.logger = logging.getLogger(__name__)
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                self.logger.info("Circuit breaker moving to HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                self.logger.info("Circuit breaker moving to CLOSED state")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                self.logger.warning(f"Circuit breaker moving to OPEN state after {self.failure_count} failures")
            
            raise e


class HTTPClient:
    def __init__(self, config: HTTPConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.headers)
        self.logger = logging.getLogger(__name__)
        
        if config.circuit_breaker_enabled:
            self.circuit_breaker = CircuitBreaker(
                config.failure_threshold,
                config.recovery_timeout_seconds
            )
        else:
            self.circuit_breaker = None
    
    def post(self, data: Dict[str, Any], correlation_id: str = None) -> HTTPResponse:
        if self.circuit_breaker:
            try:
                return self.circuit_breaker.call(self._post_with_retry, data, correlation_id)
            except Exception as e:
                return HTTPResponse(
                    status_code=0,
                    content="",
                    error_type=HTTPErrorType.CIRCUIT_BREAKER,
                    error_message=str(e)
                )
        else:
            return self._post_with_retry(data, correlation_id)
    
    def _post_with_retry(self, data: Dict[str, Any], correlation_id: str = None) -> HTTPResponse:
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Add correlation ID to headers if provided
                headers = {}
                if correlation_id:
                    headers['X-Correlation-ID'] = correlation_id
                
                response = self.session.post(
                    self.config.endpoint_url,
                    json=data,
                    headers=headers,
                    timeout=self.config.timeout_seconds
                )
                
                return HTTPResponse(
                    status_code=response.status_code,
                    content=response.text,
                    error_type=self._classify_error(response.status_code)
                )
                
            except (requests.exceptions.Timeout, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.RequestException) as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    delay = self.config.retry_delay_seconds[min(attempt, len(self.config.retry_delay_seconds) - 1)]
                    self.logger.warning(f"HTTP request failed (attempt {attempt + 1}/{self.config.max_retries + 1}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    self.logger.error(f"HTTP request failed after {self.config.max_retries + 1} attempts: {e}")
        
        return HTTPResponse(
            status_code=0,
            content="",
            error_type=HTTPErrorType.TRANSIENT,
            error_message=str(last_exception)
        )
    
    def _classify_error(self, status_code: int) -> Optional[HTTPErrorType]:
        if 400 <= status_code < 500:
            return HTTPErrorType.PERMANENT
        elif status_code >= 500:
            return HTTPErrorType.TRANSIENT
        return None