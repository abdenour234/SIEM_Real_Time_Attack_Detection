"""register_schema.py
Register Avro schema with Schema Registry.

Usage:
    python register_schema.py --schema event_normalized.avsc --subject event_normalized-value
"""
import json
import sys
import argparse
import requests
from pathlib import Path


def register_avro_schema(
    schema_file: str,
    subject: str,
    schema_registry_url: str = "http://localhost:8082"
) -> bool:
    """Register an Avro schema with the Schema Registry.
    
    Args:
        schema_file: Path to the .avsc schema file
        subject: Subject name (e.g., 'event_normalized-value')
        schema_registry_url: URL of the Schema Registry
        
    Returns:
        True if registration succeeded, False otherwise
    """
    try:
        # Read the Avro schema file
        schema_path = Path(schema_file)
        if not schema_path.exists():
            print(f"❌ Schema file not found: {schema_file}")
            return False
            
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_json = json.load(f)
        
        # Prepare the schema for registration
        schema_str = json.dumps(schema_json)
        
        # Prepare the request payload
        payload = {
            "schema": schema_str
        }
        
        # Register the schema
        url = f"{schema_registry_url}/subjects/{subject}/versions"
        headers = {
            "Content-Type": "application/vnd.schemaregistry.v1+json"
        }
        
        print(f"📋 Registering schema for subject: {subject}")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code in [200, 409]:
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Schema registered successfully (ID: {result.get('id', 'N/A')})")
            else:
                print(f"✅ Schema already registered (HTTP 409)")
            return True
        else:
            print(f"❌ Failed to register schema (HTTP {response.status_code})")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error while registering schema: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in schema file: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def verify_schema_registration(
    subject: str,
    schema_registry_url: str = "http://localhost:8082"
) -> bool:
    """Verify that a schema is registered."""
    try:
        url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Schema verified: {subject} (version {result.get('version', 'N/A')})")
            return True
        else:
            print(f"⚠️  Schema not found: {subject}")
            return False
    except Exception as e:
        print(f"⚠️  Could not verify schema: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register Avro schema with Schema Registry")
    parser.add_argument("--schema", default="event_normalized.avsc", help="Path to Avro schema file")
    parser.add_argument("--subject", default="event_normalized-value", help="Subject name")
    parser.add_argument("--registry", default="http://schema-registry:8081", help="Schema Registry URL")
    args = parser.parse_args()
    
    success = register_avro_schema(args.schema, args.subject, args.registry)
    
    if success:
        verify_schema_registration(args.subject, args.registry)
        sys.exit(0)
    else:
        sys.exit(1)
