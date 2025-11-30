#!/usr/bin/env python3
"""init_all.py
Complete initialization script for Kafka streaming pipeline.

This script orchestrates:
1. Topic creation
2. Schema registration
3. ksqlDB transformations

Usage:
    python init_all.py
"""
import sys
import time
from create_topic import ensure_topic
from register_schema import register_avro_schema, verify_schema_registration


def print_banner(message: str):
    """Print a formatted banner."""
    print("")
    print("=" * 50)
    print(f"  {message}")
    print("=" * 50)
    print("")


def wait_for_ksqldb(server_url: str, max_retries: int = 30) -> bool:
    """Wait for ksqlDB server to be ready."""
    import requests
    print(f"🔄 Waiting for ksqlDB server at {server_url}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(f"{server_url}/info", timeout=5)
            if response.status_code == 200:
                print(f"✅ ksqlDB server is ready")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"⏳ Waiting for ksqlDB server... (attempt {attempt}/{max_retries})")
        time.sleep(2)
    
    print(f"❌ ksqlDB server not ready after {max_retries} attempts")
    return False


def main():
    """Main initialization orchestrator."""
    print_banner("🚀 Starting Kafka Initialization")
    
    # Configuration
    KAFKA_BOOTSTRAP = "kafka:9092"
    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
    
    success = True
    
    # ========================================================================
    # STEP 1: Wait for Kafka to be ready
    # ========================================================================
    print("📋 Step 1: Waiting for Kafka...")
    for attempt in range(1, 31):
        try:
            if ensure_topic("_test_connectivity", num_partitions=1, bootstrap_servers=KAFKA_BOOTSTRAP):
                print("✅ Kafka is ready")
                break
        except Exception:
            pass
        print(f"⏳ Waiting for Kafka... (attempt {attempt}/30)")
        time.sleep(2)
    else:
        print("❌ Kafka not ready after 30 attempts")
        return 1
    
    # ========================================================================
    # STEP 2: Create Kafka Topics
    # ========================================================================
    print("")
    print("📋 Step 2: Creating Kafka Topics...")
    
    topics = [
        ("data.stream.raw", 6),
        ("data.stream.normalized", 6)
    ]
    
    for topic_name, partitions in topics:
        if not ensure_topic(topic_name, num_partitions=partitions, bootstrap_servers=KAFKA_BOOTSTRAP):
            print(f"❌ Failed to create topic: {topic_name}")
            success = False
        else:
            print(f"✅ Topic '{topic_name}' ready")
    
    if not success:
        return 1
    
    # ========================================================================
    # STEP 3: Register Avro Schema
    # ========================================================================
    print("")
    print("📋 Step 3: Registering Avro Schema...")
    
    # Wait for Schema Registry
    print("🔄 Waiting for Schema Registry...")
    for attempt in range(1, 31):
        try:
            import requests
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            if response.status_code == 200:
                print("✅ Schema Registry is ready")
                break
        except Exception:
            pass
        print(f"⏳ Waiting for Schema Registry... (attempt {attempt}/30)")
        time.sleep(2)
    else:
        print("❌ Schema Registry not ready after 30 attempts")
        return 1
    
    # Register schema
    if not register_avro_schema(
        schema_file="/schemas/event_normalized.avsc",
        subject="event_normalized-value",
        schema_registry_url=SCHEMA_REGISTRY_URL
    ):
        print("❌ Failed to register schema")
        return 1
    
    verify_schema_registration("event_normalized-value", SCHEMA_REGISTRY_URL)
    
    # ========================================================================
    # Summary
    # ========================================================================
    print_banner("✅ Initialization Complete!")
    
    print("📊 Created Topics:")
    print("   - data.stream.raw (6 partitions)")
    print("   - data.stream.normalized (6 partitions)")
    print("")
    print("📋 Registered Schema:")
    print("   - event_normalized-value (Avro)")
    print("")
    print("🔄 ksqlDB will auto-load queries from /docker-entrypoint-initdb.d/")
    print("=" * 50)
    print("")
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n❌ Initialization interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
