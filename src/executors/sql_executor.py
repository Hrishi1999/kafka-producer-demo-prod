"""SQL query executor with connection pooling"""

import pyodbc
import queue
import threading
import time
from typing import Dict, List, Any, Optional, Iterator
from contextlib import contextmanager
from datetime import datetime

import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ..models import DatabaseConfig, QueryConfig


logger = structlog.get_logger(__name__)


class ConnectionPool:
    """Thread-safe connection pool for SQL Server"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool = queue.Queue(maxsize=config.pool_size)
        self._all_connections = []
        self._lock = threading.Lock()
        self._closed = False
        
        # Initialize pool with connections
        for _ in range(config.pool_size):
            conn = self._create_connection()
            self._pool.put(conn)
            self._all_connections.append(conn)
    
    def _create_connection(self) -> pyodbc.Connection:
        """Create a new database connection"""
        try:
            conn = pyodbc.connect(
                self.config.connection_string,
                autocommit=False,
                timeout=self.config.connection_timeout
            )
            conn.timeout = self.config.connection_timeout
            return conn
        except Exception as e:
            logger.error("Failed to create database connection", error=str(e))
            raise
    
    @contextmanager
    def get_connection(self, timeout: float = 30.0):
        """Get a connection from the pool"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        connection = None
        start_time = time.time()
        
        try:
            connection = self._pool.get(timeout=timeout)
            
            # Test if connection is still valid
            try:
                connection.execute("SELECT 1").fetchone()
            except (pyodbc.Error, AttributeError):
                logger.warning("Dead connection detected, creating new one")
                connection.close()
                connection = self._create_connection()
            
            yield connection
            
        except queue.Empty:
            logger.error("Connection pool exhausted", 
                        pool_size=self.config.pool_size,
                        timeout=timeout)
            raise TimeoutError("Failed to get connection from pool")
        
        finally:
            if connection and not self._closed:
                try:
                    connection.rollback()  # Ensure clean state
                    self._pool.put(connection)
                except queue.Full:
                    connection.close()
    
    def close(self):
        """Close all connections in the pool"""
        self._closed = True
        
        with self._lock:
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception:
                    pass
            
            self._all_connections.clear()
            
            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except queue.Empty:
                    break


class SQLExecutor:
    """Execute SQL queries with batching and error handling"""
    
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.logger = logger.bind(component="sql_executor")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def execute_query(
        self, 
        query_config: QueryConfig,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Execute SQL query and yield results in batches
        """
        start_time = time.time()
        total_rows = 0
        
        try:
            with self.pool.get_connection(timeout=query_config.timeout_seconds) as conn:
                cursor = conn.cursor()
                cursor.arraysize = query_config.fetch_size
                
                # Prepare parameters
                param_values = self._prepare_parameters(query_config, parameters)
                
                self.logger.info(
                    "Executing query",
                    query_id=query_config.id,
                    query_name=query_config.name,
                    batch_size=query_config.batch_size
                )
                
                # Execute query
                if param_values:
                    cursor.execute(query_config.sql, param_values)
                else:
                    cursor.execute(query_config.sql)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Yield results in batches
                batch = []
                for row in cursor:
                    row_dict = dict(zip(columns, row))
                    
                    # Convert datetime objects to ISO format
                    for key, value in row_dict.items():
                        if isinstance(value, datetime):
                            row_dict[key] = value.isoformat()
                    
                    batch.append(row_dict)
                    total_rows += 1
                    
                    if len(batch) >= query_config.batch_size:
                        yield batch
                        batch = []
                
                # Yield remaining rows
                if batch:
                    yield batch
                
                duration = time.time() - start_time
                self.logger.info(
                    "Query execution completed",
                    query_id=query_config.id,
                    total_rows=total_rows,
                    duration_seconds=duration
                )
                
        except Exception as e:
            self.logger.error(
                "Query execution failed",
                query_id=query_config.id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _prepare_parameters(
        self, 
        query_config: QueryConfig,
        parameters: Optional[Dict[str, Any]]
    ) -> Optional[List]:
        """Prepare query parameters"""
        if not query_config.parameters:
            return None
        
        if not parameters:
            parameters = {}
        
        param_values = []
        for param_name in query_config.parameters:
            if param_name in parameters:
                param_values.append(parameters[param_name])
            elif param_name == "last_run_timestamp":
                param_values.append(self._get_last_run_timestamp(query_config.id))
            else:
                param_values.append(None)
        
        return param_values
    
    def _get_last_run_timestamp(self, query_id: str) -> datetime:
        """Get last run timestamp from state"""
        # This would integrate with state manager
        return datetime.now()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.pool.get_connection(timeout=5.0) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
        except Exception as e:
            self.logger.error("Connection test failed", error=str(e))
            return False