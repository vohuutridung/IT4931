# Spark Structured Streaming Module

Processes normalized social media posts from Kafka using Apache Spark Structured Streaming.

## Overview

This module:
- Reads real-time and batch data from Kafka topics
- Deserializes Avro-encoded posts
- Applies transformations (engagement scoring, deduplication, trending analysis)
- Writes results to multiple sinks (Parquet, console, Kafka)

## Architecture

```
Kafka Topics
  ├─ social-raw-batch      (historical data)
  └─ social-raw-realtime   (streaming data)
           ↓
Spark Structured Streaming
  ├─ Read: Deserialize Avro
  ├─ Transform: 
  │   ├─ Engagement aggregation
  │   ├─ Viral detection
  │   └─ Deduplication
  └─ Write:
      ├─ Parquet (analytics)
      ├─ Console (debugging)
      └─ Kafka (downstream)
           ↓
Data Lake / Dashboards
```

## Directory Structure

```
streaming/
├── __init__.py
├── main.py                    # Entry point
├── config/
│   └── spark_settings.py     # Configuration
├── processors/
│   ├── social_processor.py   # Post transformations
│   └── engagement_agg.py     # Engagement aggregations
├── sinks/
│   ├── parquet_sink.py       # Parquet output
│   ├── kafka_sink.py         # Kafka output
│   └── console_sink.py       # Console output (debug)
└── utils/
    ├── avro_helper.py        # Avro deserialization
    └── metrics.py            # Monitoring metrics
```

## Prerequisites

### Python Packages
```bash
pip install -r requirements.txt
```

### Spark with Kafka & Avro
```bash
# Install Kafka and Avro support (required for Spark Streaming)
# Option 1: Via spark-submit
spark-submit --packages \
  org.apache.spark:spark-avro_2.12:3.5.0,\
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/main.py

# Option 2: Add to ~/.bashrc or ~/.zshrc
export SPARK_PACKAGES="org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
```

### Running Services
```bash
# Start Kafka stack (should already be running from ingestion)
docker-compose up -d

# Verify services
docker-compose ps
```

## Configuration

### Environment Variables

Create `.env` file in project root:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Output
OUTPUT_DIR=/tmp/streaming-output
CHECKPOINT_DIR=/tmp/spark-checkpoints

# Processing
WINDOW_DURATION="10 minutes"
WATERMARK_DELAY="5 minutes"

# Spark
SPARK_MASTER="local[*]"  # local[*] for dev, yarn for prod
LOG_LEVEL=INFO
```

### spark_settings.py

Modify streaming/config/spark_settings.py for:
- Memory configuration
- Batch sizes/intervals
- Partition counts
- Checkpoint locations

## Running

### Development (Local)

```bash
# Option 1: Direct Python
python -m streaming.main

# Option 2: With spark-submit (recommended)
spark-submit \
  --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 4g \
  streaming/main.py

# Option 3: With environment variables
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
WINDOW_DURATION="5 minutes" \
spark-submit \
  --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/main.py
```

### Production (Cluster/YARN)

```bash
# Submit to YARN cluster
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/main.py
```

## Monitoring

### Spark UI
Access at: http://localhost:4040

Metrics:
- Active streaming queries
- Batch duration
- Records processed per second
- Memory usage
- Task execution times

### Console Output
While running locally, see data flowing through in console:
```
[timestamp] TOPIC1        | offset: 12345 | ...
[timestamp] TOPIC2        | offset: 12346 | ...
...
```

### Parquet Output
Check processed data:
```bash
# List output directory
ls -la /tmp/streaming-output/

# Read Parquet in Python
import pandas as pd
df = pd.read_parquet("/tmp/streaming-output/raw/topic=social-raw-realtime/")
print(df.head())
```

## Transformations

### SocialProcessor
- **engagement_score**: likes\*2 + comments\*3 + shares\*5
- **content_metrics**: length, hashtag_count, url_present
- **deduplication**: Remove duplicate post_ids
- **engagement_category**: low/medium/high/viral

### EngagementAggregator
- **by_source_and_time**: Group by source + time window
- **trending_hashtags**: Top hashtags per window
- **source_comparison**: Engagement comparison across platforms

## Common Issues

### Issue: Cannot find spark-avro package
```
Error: org.apache.spark:spark-avro not found
```
Solution:
```bash
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0,...
```

### Issue: Kafka connection refused
```
Error: Address already in use or Kafka not reachable
```
Solution:
```bash
# Check Kafka is running
docker-compose ps | grep kafka

# Check bootstrap servers in config
grep KAFKA_BOOTSTRAP streaming/config/spark_settings.py
```

### Issue: Checkpoint location error
```
Error: Checkpoint directory is corrupted
```
Solution:
```bash
# Remove old checkpoint and restart
rm -rf /tmp/spark-checkpoints
python -m streaming.main
```

### Issue: Out of memory
```
Error: Java heap space
```
Solution: Increase memory in spark-submit
```bash
spark-submit --driver-memory 8g --executor-memory 8g ...
```

## Performance Tuning

### Batch Interval
Adjust micro-batch interval (default: as fast as possible)
```python
.trigger(processingTime='10 seconds')  # 10-second batches
```

### Kafka Throttle
Limit records per batch:
```python
.option("maxOffsetsPerTrigger", "100000")  # 100k records per batch
```

### Partitioning
Increase shuffle partitions for better parallelism:
```python
"spark.sql.shuffle.partitions": "200"
```

### Memory Configuration
```bash
spark-submit \
  --driver-memory 8g \
  --executor-memory 8g \
  --driver-cores 2 \
  ...
```

## Advanced Usage

### Custom Processors
Add new processors in `processors/`:
```python
from pyspark.sql import DataFrame

class MyProcessor:
    @staticmethod
    def process(df: DataFrame) -> DataFrame:
        # Custom logic
        return df.withColumn("my_metric", ...)
```

### Additional Sinks
Add new sinks in `sinks/`:
```python
def write_to_database(df, connection_string, table):
    # Write to PostgreSQL, MongoDB, etc.
    return df.write.jdbc(connection_string, table, mode="append")
```

### Stateful Operations
Use window functions for state management:
```python
from pyspark.sql.functions import window
from pyspark.sql.window import Window

df.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("source")
).agg(count("*"))
```

## Testing

### Dry Run with Sample Data
```python
# Use .option("startingOffsets", "earliest") for batch processing
# Or limit data with maxOffsetsPerTrigger
```

### Unit Tests
```bash
# Add tests in tests/ directory
python -m pytest tests/
```

## Documentation

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Avro in Spark](https://github.com/confluentinc/spark-avro)

## Troubleshooting

### Enable debug logging
```python
import logging
logging.getLogger("org.apache.spark").setLevel(logging.DEBUG)
logging.getLogger("streaming").setLevel(logging.DEBUG)
```

### Check Spark logs
```bash
tail -f /tmp/spark-logs/
```

### Validate Kafka connection
```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics --describe --topic social-raw-realtime --bootstrap-server localhost:9092
```

## Future Enhancements

- [ ] Sentiment analysis integration
- [ ] Machine learning model scoring
- [ ] Real-time alerts/anomaly detection
- [ ] Stream-stream joins
- [ ] Session window aggregations
- [ ] Custom metrics publishing to monitoring systems
