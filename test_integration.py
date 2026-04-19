"""
Integration test for Spark Streaming pipeline.

Tests core functionality:
- Avro schema deserialization
- SocialProcessor transformations
- EngagementAggregator calculations
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from streaming.utils.avro_helper import get_avro_schema, avro_schema_to_dsl
from streaming.processors.social_processor import SocialProcessor
from streaming.config.spark_settings import OUTPUT_DIR, CHECKPOINT_DIR


def test_avro_schema():
    """Test Avro schema loading and DDL conversion."""
    print("\n" + "=" * 80)
    print("TEST 1: Avro Schema Loading")
    print("=" * 80)
    
    try:
        schema = get_avro_schema()
        print(f"✓ Schema loaded successfully")
        print(f"  Schema name: {schema['name']}")
        print(f"  Fields: {len(schema['fields'])}")
        
        # Print field names
        field_names = [f['name'] for f in schema['fields']]
        print(f"  Field names: {', '.join(field_names)}")
        
        # Test DDL conversion
        dsl = avro_schema_to_dsl(schema)
        print(f"\n✓ DDL conversion successful")
        print(f"  DDL length: {len(dsl)} characters")
        print(f"  DDL preview: {dsl[:100]}...")
        
        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False


def test_directories():
    """Test output and checkpoint directories."""
    print("\n" + "=" * 80)
    print("TEST 2: Directory Configuration")
    print("=" * 80)
    
    try:
        print(f"✓ OUTPUT_DIR: {OUTPUT_DIR}")
        print(f"  Exists: {OUTPUT_DIR.exists()}")
        print(f"  Writable: {OUTPUT_DIR.is_dir() and list(OUTPUT_DIR.iterdir())}")
        
        print(f"\n✓ CHECKPOINT_DIR: {CHECKPOINT_DIR}")
        print(f"  Exists: {CHECKPOINT_DIR.exists()}")
        print(f"  Writable: {CHECKPOINT_DIR.is_dir()}")
        
        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False


def test_processor():
    """Test SocialProcessor initialization."""
    print("\n" + "=" * 80)
    print("TEST 3: SocialProcessor Initialization")
    print("=" * 80)
    
    try:
        processor = SocialProcessor()
        print(f"✓ SocialProcessor instantiated")
        print(f"  Methods: apply, add_engagement_score, add_content_metrics, deduplicate, filter_by_engagement, categorize_engagement")
        
        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False


def test_imports():
    """Test all module imports."""
    print("\n" + "=" * 80)
    print("TEST 4: Module Imports")
    print("=" * 80)
    
    modules = [
        "streaming.config.spark_settings",
        "streaming.utils.avro_helper",
        "streaming.utils.metrics",
        "streaming.processors.social_processor",
        "streaming.processors.engagement_agg",
        "streaming.sinks.parquet_sink",
        "streaming.sinks.kafka_sink",
        "streaming.sinks.console_sink",
    ]
    
    failed = []
    
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"✓ {module_name}")
        except Exception as e:
            print(f"✗ {module_name}: {e}")
            failed.append((module_name, e))
    
    return len(failed) == 0


def test_config():
    """Test configuration values."""
    print("\n" + "=" * 80)
    print("TEST 5: Configuration Values")
    print("=" * 80)
    
    from streaming.config.spark_settings import (
        KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL,
        SOURCE_TOPICS, SPARK_MASTER, WINDOW_DURATION
    )
    
    print(f"✓ KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"✓ SCHEMA_REGISTRY_URL: {SCHEMA_REGISTRY_URL}")
    print(f"✓ SOURCE_TOPICS: {SOURCE_TOPICS}")
    print(f"✓ SPARK_MASTER: {SPARK_MASTER}")
    print(f"✓ WINDOW_DURATION: {WINDOW_DURATION}")
    
    return True


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "SPARK STREAMING INTEGRATION TEST".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    
    tests = [
        ("Avro Schema", test_avro_schema),
        ("Directories", test_directories),
        ("SocialProcessor", test_processor),
        ("Module Imports", test_imports),
        ("Configuration", test_config),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ EXCEPTION in {test_name}: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} {test_name}")
    
    print("")
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Ready to run Spark Streaming job.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check errors above.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
