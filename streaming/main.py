"""
Spark Structured Streaming job for social media data processing.

Reads normalized social media posts from Kafka topics, applies transformations,
and writes to multiple sinks (Parquet, console, etc).

Usage:
  python -m streaming.main
  
  Or with spark-submit:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    streaming/main.py
"""

import logging
import sys
import time
from typing import List
from pathlib import Path

# Add project root to Python path for spark-submit
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, avg, count, from_json, get_json_object, to_timestamp, from_unixtime,
    max as spark_max, min as spark_min,
)
from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, StringType, StructField, StructType,
)

from streaming.config.spark_settings import (
    SPARK_CONFIG, SPARK_MASTER, KAFKA_BOOTSTRAP_SERVERS,
    SOURCE_TOPICS, PROCESSED_TOPIC,
    WINDOW_DURATION, WATERMARK_DELAY, OUTPUT_DIR, CHECKPOINT_DIR,
    LOG_LEVEL,
)
from streaming.processors.social_processor import SocialProcessor
from streaming.processors.engagement_agg import EngagementAggregator
from streaming.sinks.parquet_sink import ParquetSink
from streaming.sinks.kafka_sink import write_to_kafka
from streaming.sinks.console_sink import write_to_console

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


POST_JSON_SCHEMA = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("post_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("ingest_time", LongType(), True),
    StructField("author_id", StringType(), True),
    StructField("author_name", StringType(), True),
    StructField("content", StringType(), True),
    StructField("url", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("engagement", StructType([
        StructField("likes", LongType(), True),
        StructField("comments", LongType(), True),
        StructField("shares", LongType(), True),
        StructField("score", LongType(), True),
        StructField("video_views", LongType(), True),
        StructField("comments_normalized_count", IntegerType(), True),
    ]), True),
    StructField("comments", ArrayType(StructType([
        StructField("comment_id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("author_id", StringType(), True),
        StructField("author", StringType(), True),
        StructField("text", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("created_at", LongType(), True),
        StructField("depth", IntegerType(), True),
        StructField("extra", StringType(), True),
    ])), True),
    StructField("extra", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session.
    
    Includes Kafka support via packages.
    
    Returns:
        Configured SparkSession
    """
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder
    
    # Apply configuration
    for key, value in SPARK_CONFIG.items():
        spark = spark.config(key, str(value))
    
    spark = spark.master(SPARK_MASTER)
    spark = spark.appName("SocialMediaStreaming")
    
    return spark.getOrCreate()


def read_from_kafka(spark: SparkSession) -> DataFrame:
    """
    Read streaming data from Kafka topics.
    
    Args:
        spark: SparkSession
        
    Returns:
        Streaming DataFrame with raw Kafka data
    """
    logger.info("Connecting to Kafka brokers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topics: %s", SOURCE_TOPICS)
    
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ",".join(SOURCE_TOPICS))
        .option("startingOffsets", "earliest")  # Read from beginning to get all messages
        .option("maxOffsetsPerTrigger", "50000")  # Throttle: 50k records per batch
        .load()
    )


def deserialize_json_posts(df_raw: DataFrame) -> DataFrame:
    """
    Deserialize Kafka JSON bytes to nested DataFrame columns.

    The ingestion producer writes UTF-8 JSON bytes.
    
    Args:
        df_raw: Raw Kafka DataFrame with "value" (JSON bytes)
        
    Returns:
        DataFrame with deserialized post data
    """
    logger.info("Configuring JSON deserialization...")

    parsed = (
        df_raw
        .select(
            col("topic").alias("kafka_topic"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
        )
        .select(
            "kafka_topic",
            "kafka_timestamp",
            "kafka_offset",
            "kafka_key",
            "json_value",
            from_json(col("json_value"), POST_JSON_SCHEMA).alias("post"),
        )
        .filter(col("post").isNotNull())
        .select("kafka_topic", "kafka_timestamp", "kafka_offset", "kafka_key", "json_value", "post.*")
        .withColumn("extra", get_json_object(col("json_value"), "$.extra"))
        .drop("json_value")
        .filter(col("post_id").isNotNull() & col("event_time").isNotNull())
    )

    return parsed.withColumn(
        "event_timestamp",
        to_timestamp(from_unixtime((col("event_time") / 1000).cast("double"))),
    )


# Fix #13: default column name phải là "event_timestamp" (column thực tế được tạo)
def apply_watermark(df: DataFrame, event_time_col: str = "event_timestamp") -> DataFrame:
    """
    Apply watermark for handling late-arriving data.
    
    Data arriving later than watermark delay will be dropped.
    
    Args:
        df: Input DataFrame
        event_time_col: Column with event timestamps
        
    Returns:
        DataFrame with watermark applied
    """
    logger.info("Applying watermark: %s late arrival tolerance", WATERMARK_DELAY)
    
    return df.withWatermark(event_time_col, WATERMARK_DELAY)



def aggregate_engagement(df: DataFrame) -> DataFrame:
    """
    Aggregate engagement metrics by source and time window.
    
    Calculates:
    - Number of posts
    - Average likes, comments, shares
    - Max/min engagement
    
    Args:
        df: Input DataFrame with engagement metrics
        
    Returns:
        Aggregated DataFrame
    """
    logger.info("Setting up engagement aggregation (window: %s)", WINDOW_DURATION)
    
    return EngagementAggregator.aggregate_by_source_and_time(df, WINDOW_DURATION)


def main():
    """
    Main streaming job.
    
    Orchestrates:
    1. Kafka source connection
    2. JSON deserialization
    3. Schema validation
    4. Watermarking
    5. Transformations (engagement aggregation, deduplication)
    6. Multi-sink output (Parquet, console, optional Kafka)
    """
    logger.info("=" * 80)
    logger.info("SOCIAL MEDIA SPARK STREAMING PIPELINE")
    logger.info("=" * 80)
    
    try:
        # ── Initialize Spark ───────────────────────────────────────────────────
        spark = create_spark_session()
        logger.info("Spark Version: %s", spark.version)
        logger.info("Master: %s", spark.sparkContext.master)
        
        # ── Read from Kafka ───────────────────────────────────────────────────
        df_raw = read_from_kafka(spark)
        logger.info("Connected to Kafka")
        
        # ── Deserialize JSON ──────────────────────────────────────────────────
        df_posts = deserialize_json_posts(df_raw)
        logger.info("JSON deserialization configured")
        
        # ── Apply Watermark ───────────────────────────────────────────────────
        df_posts = apply_watermark(df_posts, "event_timestamp")
        
        # ── Transformations ───────────────────────────────────────────────────
        logger.info("Setting up transformations...")
        
        processor = SocialProcessor()
        df_posts = processor.apply(df_posts)
        
        aggregator = EngagementAggregator()
        # aggregate_by_source_and_time là @staticmethod, gọi trực tiếp qua class
        df_agg = EngagementAggregator.aggregate_by_source_and_time(df_posts, WINDOW_DURATION)
        
        # ── Output Sinks ──────────────────────────────────────────────────────
        logger.info("Setting up output sinks...")
        
        queries = []
        
        # 1. Console output (for debugging)
        logger.info("Starting console sink...")
        q_console = write_to_console(
            df_posts,
            str(CHECKPOINT_DIR / "console"),
            mode="append",
            num_rows=10,
        )
        queries.append(("console", q_console))
        
        # 2. Parquet output (for analytics/archival)
        logger.info("Starting Parquet sink to %s", OUTPUT_DIR / 'clean')
        q_parquet = ParquetSink.write(
            df_posts,
            str(OUTPUT_DIR / "clean"),
            str(CHECKPOINT_DIR / "parquet"),
            partition_cols=["source", "event_year", "event_month", "event_date"],
        )
        queries.append(("parquet-clean", q_parquet))
        
        # 3. Aggregated engagement metrics
        # outputMode="append" hợp lệ với windowed aggregation + watermark
        logger.info("Starting aggregated engagement sink...")
        q_agg = ParquetSink.write(
            df_agg,
            str(OUTPUT_DIR / "aggregated"),
            str(CHECKPOINT_DIR / "aggregated"),
            partition_cols=["source"],
            mode="append",
        )
        queries.append(("parquet-aggregated", q_agg))
        
        # 4. Kafka output for monitoring via UI
        logger.info("Starting Kafka sink to %s", PROCESSED_TOPIC)
        q_kafka = write_to_kafka(
            df_posts,
            KAFKA_BOOTSTRAP_SERVERS,
            PROCESSED_TOPIC,
            str(CHECKPOINT_DIR / "kafka")
        )
        queries.append(("kafka-processed", q_kafka))
        
        # ── Monitor Streams ────────────────────────────────────────────────────
        logger.info("=" * 80)
        logger.info("Streaming job running with %d sinks", len(queries))
        logger.info("=" * 80)
        logger.info("Press Ctrl+C to shutdown gracefully...")
        logger.info("")
        
        # Print sink information
        for name, q in queries:
            logger.info("  [%s] id=%s", name, q.id)
        
        # Wait for any query to terminate
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("\nShutdown signal received...")
        
    except Exception as e:
        logger.error("Error in streaming job: %s", e, exc_info=True)
        sys.exit(1)
        
    finally:
        # ── Cleanup ──────────────────────────────────────────────────────────
        logger.info("Cleaning up...")
        
        # Stop all queries
        if 'queries' in locals():
            for name, q in queries:
                if q:
                    logger.info("Stopping sink: %s", name)
                    q.stop()
                    # Wait for clean shutdown
                    q.awaitTermination(timeout=10)
        
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
        
        logger.info("Spark Streaming job terminated.")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
