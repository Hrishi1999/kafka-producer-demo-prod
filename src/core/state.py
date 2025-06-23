"""State management for tracking query execution"""

import json
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from contextlib import contextmanager

import structlog


logger = structlog.get_logger(__name__)


class StateManager:
    """Manage persistent state for query execution tracking"""
    
    def __init__(self, state_file: str = "state.json"):
        self.state_file = Path(state_file)
        self.state: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.last_checkpoint = time.time()
        self.checkpoint_interval = 60  # seconds
        self.logger = logger.bind(component="state_manager")
    
    def load(self):
        """Load state from file"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
                self.logger.info(
                    "State loaded",
                    file=str(self.state_file),
                    queries=len(self.state.get("queries", {}))
                )
            else:
                self.state = {"queries": {}, "metadata": {}}
                self.logger.info("New state file created")
        except Exception as e:
            self.logger.error("Failed to load state", error=str(e))
            self.state = {"queries": {}, "metadata": {}}
    
    def save(self):
        """Save state to file"""
        try:
            with self.lock:
                # Update metadata
                self.state["metadata"] = {
                    "last_updated": datetime.now().isoformat(),
                    "version": "1.0"
                }
                
                # Write to temporary file first
                temp_file = self.state_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(self.state, f, indent=2, default=str)
                
                # Atomic move
                temp_file.replace(self.state_file)
                
                self.last_checkpoint = time.time()
                
            self.logger.debug("State saved", file=str(self.state_file))
            
        except Exception as e:
            self.logger.error("Failed to save state", error=str(e))
    
    def should_checkpoint(self) -> bool:
        """Check if state should be checkpointed"""
        return time.time() - self.last_checkpoint > self.checkpoint_interval
    
    def get_last_run(self, query_id: str) -> Optional[datetime]:
        """Get last run timestamp for a query"""
        with self.lock:
            query_state = self.state.get("queries", {}).get(query_id, {})
            last_run_str = query_state.get("last_run")
            
            if last_run_str:
                try:
                    return datetime.fromisoformat(last_run_str)
                except ValueError:
                    self.logger.warning(
                        "Invalid last_run timestamp",
                        query_id=query_id,
                        timestamp=last_run_str
                    )
            
            return None
    
    def update_last_run(self, query_id: str, timestamp: datetime):
        """Update last run timestamp for a query"""
        with self.lock:
            if "queries" not in self.state:
                self.state["queries"] = {}
            
            if query_id not in self.state["queries"]:
                self.state["queries"][query_id] = {}
            
            self.state["queries"][query_id]["last_run"] = timestamp.isoformat()
            self.state["queries"][query_id]["run_count"] = (
                self.state["queries"][query_id].get("run_count", 0) + 1
            )
    
    def get_state(self) -> Dict[str, Any]:
        """Get current state"""
        with self.lock:
            return dict(self.state)
    
    @contextmanager
    def transactional_update(self, query_id: str, timestamp: datetime):
        """Context manager for transactional state updates"""
        # Store original state for rollback
        original_state = None
        
        try:
            with self.lock:
                original_state = dict(self.state)
                
                # Prepare the state update
                if "queries" not in self.state:
                    self.state["queries"] = {}
                
                if query_id not in self.state["queries"]:
                    self.state["queries"][query_id] = {}
                
                # Update the state but don't save yet
                self.state["queries"][query_id]["last_run"] = timestamp.isoformat()
                self.state["queries"][query_id]["run_count"] = (
                    self.state["queries"][query_id].get("run_count", 0) + 1
                )
                self.state["queries"][query_id]["pending_commit"] = True
            
            # Yield control back to caller
            yield self
            
            # If we get here, commit the state
            with self.lock:
                if query_id in self.state.get("queries", {}):
                    self.state["queries"][query_id].pop("pending_commit", None)
                self.save()
                
            self.logger.debug("Transactional state update committed", query_id=query_id)
            
        except Exception as e:
            # Rollback on any error
            if original_state is not None:
                with self.lock:
                    self.state = original_state
                    
            self.logger.error("Transactional state update failed, rolled back", 
                            query_id=query_id, error=str(e))
            raise
    
    def has_pending_commit(self, query_id: str) -> bool:
        """Check if query has a pending commit"""
        with self.lock:
            query_state = self.state.get("queries", {}).get(query_id, {})
            return query_state.get("pending_commit", False)