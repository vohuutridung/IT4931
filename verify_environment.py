#!/usr/bin/env python
"""
Verify Conda environment for IT4931 project.

Checks:
- Python version
- Required packages installed
- Import functionality
- System requirements (Java, Docker, etc.)
"""

import sys
import subprocess
from pathlib import Path

# Colors
class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def log_info(msg):
    print(f"{Color.BLUE}[INFO]{Color.END} {msg}")

def log_success(msg):
    print(f"{Color.GREEN}[✓]{Color.END} {msg}")

def log_error(msg):
    print(f"{Color.RED}[✗]{Color.END} {msg}")

def log_warn(msg):
    print(f"{Color.YELLOW}[⚠]{Color.END} {msg}")

def print_header(title):
    print(f"\n{'='*80}")
    print(f"{title.center(80)}")
    print(f"{'='*80}\n")

def check_python_version():
    """Check Python version."""
    print_header("PYTHON VERSION")
    
    version_info = sys.version_info
    python_version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    
    log_info(f"Python executable: {sys.executable}")
    log_success(f"Python version: {python_version}")
    
    if version_info.major < 3 or version_info.minor < 8:
        log_warn("Python 3.8+ recommended (using Spark)")
        return False
    
    return True

def check_package(package_name, import_name=None):
    """Check if a package is installed and can be imported."""
    if import_name is None:
        import_name = package_name
    
    try:
        __import__(import_name)
        module = sys.modules[import_name]
        version = getattr(module, '__version__', 'unknown')
        log_success(f"{package_name} {version}")
        return True
    except ImportError as e:
        log_error(f"{package_name} - {str(e)}")
        return False

def check_required_packages():
    """Check all required packages."""
    print_header("REQUIRED PACKAGES")
    
    packages = [
        # Data processing
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("requests", "requests"),
        
        # Kafka & Avro
        ("confluent-kafka", "confluent_kafka"),
        ("fastavro", "fastavro"),
        ("python-dotenv", "dotenv"),
        
        # Spark
        ("pyspark", "pyspark"),
    ]
    
    results = []
    for package_name, import_name in packages:
        result = check_package(package_name, import_name)
        results.append(result)
    
    return all(results)

def check_project_modules():
    """Check project-specific modules."""
    print_header("PROJECT MODULES")
    
    modules = [
        "streaming.config.spark_settings",
        "streaming.utils.avro_helper",
        "streaming.utils.metrics",
        "streaming.processors.social_processor",
        "streaming.processors.engagement_agg",
        "streaming.sinks.parquet_sink",
        "streaming.sinks.kafka_sink",
        "streaming.sinks.console_sink",
        "ingestion.config.settings",
        "ingestion.producer.readers",
        "ingestion.producer.social_producer",
    ]
    
    results = []
    for module_name in modules:
        try:
            __import__(module_name)
            log_success(module_name)
            results.append(True)
        except ImportError as e:
            log_error(f"{module_name}: {e}")
            results.append(False)
    
    return all(results)

def check_system_requirements():
    """Check system requirements."""
    print_header("SYSTEM REQUIREMENTS")
    
    checks = {
        "Java (for Spark)": ["java", "-version"],
        "Docker": ["docker", "--version"],
        "Docker Compose": ["docker-compose", "--version"],
    }
    
    results = []
    
    for name, cmd in checks.items():
        try:
            output = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            if output.returncode == 0:
                # Get version line
                version_line = (output.stdout or output.stderr).split('\n')[0]
                log_success(f"{name}: {version_line.strip()}")
                results.append(True)
            else:
                log_error(f"{name} - command failed")
                results.append(False)
        except FileNotFoundError:
            log_error(f"{name} - not found in PATH")
            results.append(False)
        except Exception as e:
            log_error(f"{name} - {str(e)}")
            results.append(False)
    
    return results  # Return list to show partial failures

def check_directories():
    """Check project directories."""
    print_header("PROJECT DIRECTORIES")
    
    dirs = {
        "streaming": "./streaming",
        "ingestion": "./ingestion",
        "data": "./data",
    }
    
    results = []
    
    for name, path in dirs.items():
        p = Path(path)
        if p.exists() and p.is_dir():
            files = list(p.glob("*.py"))
            log_success(f"{name}: {len(files)} Python files")
            results.append(True)
        else:
            log_error(f"{name}: directory not found")
            results.append(False)
    
    return all(results)

def check_configuration():
    """Check configuration files."""
    print_header("CONFIGURATION FILES")
    
    configs = {
        "environment.yml": "./environment.yml",
        "requirements.txt": "./requirements.txt",
        "docker-compose.yml": "./docker-compose.yml",
        "Avro schema": "./ingestion/schema/social_post.avsc",
        "Spark settings": "./streaming/config/spark_settings.py",
    }
    
    results = []
    
    for name, path in configs.items():
        p = Path(path)
        if p.exists():
            size = p.stat().st_size
            log_success(f"{name}: {size} bytes")
            results.append(True)
        else:
            log_error(f"{name}: not found")
            results.append(False)
    
    return all(results)

def main():
    """Run all checks."""
    print("")
    print("╔" + "=" * 78 + "╗")
    print("║" + "IT4931 CONDA ENVIRONMENT VERIFICATION".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    
    checks = [
        ("Python Version", check_python_version),
        ("Required Packages", check_required_packages),
        ("Project Modules", check_project_modules),
        ("System Requirements", check_system_requirements),
        ("Project Directories", check_directories),
        ("Configuration Files", check_configuration),
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        try:
            result = check_func()
            if isinstance(result, list):
                # For checks that return lists
                results[check_name] = any(result)
            else:
                results[check_name] = result
        except Exception as e:
            log_error(f"Exception in {check_name}: {e}")
            results[check_name] = False
    
    # Summary
    print_header("VERIFICATION SUMMARY")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for check_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status:8} {check_name}")
    
    print()
    print(f"Total: {passed}/{total} checks passed\n")
    
    if passed == total:
        log_success("Environment is ready to use! 🎉")
        print()
        print("Next steps:")
        print("  1. Start Kafka: docker-compose up -d")
        print("  2. Run ingestion: python -m ingestion.main")
        print("  3. Run streaming: python -m streaming.main")
        return 0
    else:
        log_warn(f"Please fix {total - passed} issue(s) above")
        print()
        print("Common fixes:")
        print("  - Install missing packages: pip install <package>")
        print("  - Install Docker: https://docs.docker.com/get-docker/")
        print("  - Install Java: sudo apt install openjdk-11-jdk")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
