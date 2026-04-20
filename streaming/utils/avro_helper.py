"""
Avro schema handling and deserialization utilities.

Converts between Avro schema format (JSON) and Spark DDL schema string.
"""

import json
from pathlib import Path
from typing import Dict, Any

# Load Avro schema from ingestion module
SCHEMA_PATH = (
    Path(__file__).parent.parent.parent / "ingestion" / "schema" / "social_post.avsc"
)


def get_avro_schema() -> Dict[str, Any]:
    """
    Load Avro schema from social_post.avsc.
    
    Returns:
        Dictionary representing the Avro schema
    """
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def avro_schema_to_dsl(avro_schema: Dict[str, Any]) -> str:
    """
    Convert Avro schema to Spark DDL schema string.
    
    Converts Avro type definitions to Spark SQL types:
    - string → STRING
    - int → INT
    - long → LONG
    - boolean → BOOLEAN
    - double → DOUBLE
    - record → STRUCT<...>
    - array → ARRAY<...>
    - enum → STRING
    
    Args:
        avro_schema: Dictionary with Avro schema definition
        
    Returns:
        DDL schema string suitable for Spark SQL
        
    Example:
        >>> schema = get_avro_schema()
        >>> dsl = avro_schema_to_dsl(schema)
        >>> print(dsl)
        STRUCT<post_id: STRING, source: STRING, event_time: LONG, ...>
    """
    
    def convert_type(avro_type: Any) -> str:
        """Recursively convert Avro type to Spark DDL type."""
        
        # Handle unions (e.g., ["null", "string"])
        if isinstance(avro_type, list):
            # Filter out null and get first non-null type
            non_null_types = [t for t in avro_type if t != "null"]
            if non_null_types:
                avro_type = non_null_types[0]
            else:
                return "STRING"  # Default for null-only union
        
        # Handle primitive types (strings)
        if isinstance(avro_type, str):
            type_map = {
                "string": "STRING",
                "int": "INT",
                "long": "LONG",
                "boolean": "BOOLEAN",
                "double": "DOUBLE",
                "float": "FLOAT",
                "bytes": "BINARY",
            }
            return type_map.get(avro_type, "STRING")
        
        # Handle complex types (dicts)
        if isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            
            if type_name == "record":
                # Nested record → STRUCT
                fields = ", ".join(
                    f"{f['name']}: {convert_type(f['type'])}"
                    for f in avro_type.get("fields", [])
                )
                return f"STRUCT<{fields}>"
            
            elif type_name == "array":
                # Array type
                item_type = convert_type(avro_type.get("items"))
                return f"ARRAY<{item_type}>"
            
            elif type_name == "map":
                # Map type
                value_type = convert_type(avro_type.get("values"))
                return f"MAP<STRING, {value_type}>"
            
            elif type_name == "enum":
                # Enum → STRING
                return "STRING"
            
            elif type_name == "fixed":
                # Fixed-length bytes
                return "BINARY"
        
        # Default fallback
        return "STRING"
    
    # Convert all fields
    fields = ", ".join(
        f"{f['name']}: {convert_type(f['type'])}"
        for f in avro_schema.get("fields", [])
    )
    
    return f"STRUCT<{fields}>"


def print_schema_info():
    """Print Avro schema information (useful for debugging)."""
    schema = get_avro_schema()
    dsl = avro_schema_to_dsl(schema)
    
    print("\n" + "=" * 80)
    print("AVRO SCHEMA:")
    print("=" * 80)
    print(json.dumps(schema, indent=2))
    
    print("\n" + "=" * 80)
    print("SPARK DDL SCHEMA:")
    print("=" * 80)
    print(dsl)
    print("=" * 80 + "\n")


if __name__ == "__main__":
    # For testing
    print_schema_info()
