"""Avro schema generation and management utilities"""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict


class AvroSchemaGenerator:
    """Generate Avro schemas from SQL result sets"""
    
    # Type mappings from Python/SQL types to Avro types
    TYPE_MAPPINGS = {
        str: "string",
        int: "long",
        float: "double",
        bool: "boolean",
        datetime: {"type": "long", "logicalType": "timestamp-millis"},
        Decimal: {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2},
        bytes: "bytes",
    }
    
    @classmethod
    def generate_schema_from_row(
        cls,
        sample_row: Dict[str, Any],
        schema_name: str,
        namespace: str = "sql.avro"
    ) -> str:
        """Generate Avro schema from a sample row"""
        
        fields = []
        for column_name, value in sample_row.items():
            # Skip metadata fields
            if column_name.startswith('_'):
                continue
                
            field_type = cls._get_avro_type(value)
            fields.append({
                "name": column_name,
                "type": ["null", field_type],  # Make all fields nullable
                "default": None
            })
        
        # Add standard metadata fields
        fields.extend([
            {
                "name": "_metadata",
                "type": ["null", {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                        {"name": "query_id", "type": ["null", "string"], "default": None},
                        {"name": "extracted_at", "type": ["null", "string"], "default": None},
                        {"name": "producer", "type": ["null", "string"], "default": None}
                    ]
                }],
                "default": None
            }
        ])
        
        schema = {
            "type": "record",
            "name": schema_name,
            "namespace": namespace,
            "fields": fields
        }
        
        return json.dumps(schema, indent=2)
    
    @classmethod
    def _get_avro_type(cls, value: Any) -> Dict[str, Any] | str:
        """Get Avro type for a Python value"""
        if value is None:
            return "string"  # Default to string for null values
        
        value_type = type(value)
        
        # Check direct type mappings
        if value_type in cls.TYPE_MAPPINGS:
            return cls.TYPE_MAPPINGS[value_type]
        
        # Handle special cases
        if isinstance(value, (int, float)) and abs(value) > 2**31:
            return "long"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, str):
            # Check if it's a datetime string
            if cls._is_datetime_string(value):
                return {"type": "long", "logicalType": "timestamp-millis"}
            return "string"
        
        # Default to string for unknown types
        return "string"
    
    @classmethod
    def _is_datetime_string(cls, value: str) -> bool:
        """Check if string looks like a datetime"""
        datetime_patterns = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ"
        ]
        
        for pattern in datetime_patterns:
            try:
                datetime.strptime(value, pattern)
                return True
            except ValueError:
                continue
        
        return False