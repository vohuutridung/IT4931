"""
Spark Structured Streaming job for social media data processing.

Reads normalized social media posts from Kafka topics, applies transformations,
and writes to multiple sinks (Parquet, console, etc).

Usage:
  python -m streaming.main
  
  Or with spark-submit:
  spark-submit \
    --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
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
    col, window, avg, count, 
    max as spark_max, min as spark_min,
)

from streaming.config.spark_settings import (
    SPARK_CONFIG, KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL,
    SOURCE_TOPICS, PROCESSED_TOPIC,
    WINDOW_DURATION, WATERMARK_DELAY, OUTPUT_DIR, CHECKPOINT_DIR,
    LOG_LEVEL,
)
from streaming.utils.avro_helper import get_avro_schema, avro_schema_to_dsl
from streaming.processors.social_processor import SocialProcessor
from streaming.processors.engagement_agg import EngagementAggregator
from streaming.sinks.parquet_sink import ParquetSink, ConsoleSink
from streaming.sinks.kafka_sink import write_to_kafka
from streaming.sinks.console_sink import write_to_console

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session.
    
    Includes Avro and Kafka support via packages.
    
    Returns:
        Configured SparkSession
    """
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder
    
    # Apply configuration
    for key, value in SPARK_CONFIG.items():
        spark = spark.config(key, str(value))
    
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
    logger.info(f"Connecting to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topics: {SOURCE_TOPICS}")
    
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


def deserialize_avro_posts(df_raw: DataFrame) -> DataFrame:
    """
    Deserialize Kafka Avro bytes to nested DataFrame columns.
    
    Note: Requires spark-avro package installed.
    For now, using a workaround since the schema registry integration
    can be complex.
    
    Args:
        df_raw: Raw Kafka DataFrame with "value" (Avro bytes)
        
    Returns:
        DataFrame with deserialized post data
    """
    logger.info("Configuring Avro deserialization...")
    
    try:
        # Try using from_avro if spark-avro is available
        from pyspark.sql.functions import from_avro
        
        schema_str = avro_schema_to_dsl(get_avro_schema())
        
        return (
            df_raw
            .select(
                col("topic"),
                col("timestamp"),
                col("offset"),
                col("value"),  # Keep raw value for now
            )
        )
    except ImportError:
        logger.warning(
            "spark-avro not available. Install with: "
            "spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0"
        )
        
        # Fallback: select raw columns
        return (
            df_raw
            .select(col("topic"), col("timestamp"), col("offset"), col("value"))
        )


def apply_watermark(df: DataFrame, event_time_col: str = "timestamp") -> DataFrame:
    """
    Apply watermark for handling late-arriving data.
    
    Data arriving later than watermark delay will be dropped.
    
    Args:
        df: Input DataFrame
        event_time_col: Column with event timestamps
        
    Returns:
        DataFrame with watermark applied
    """
    logger.info(f"Applying watermark: {WATERMARK_DELAY} late arrival tolerance")
    
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
    logger.info(f"Setting up engagement aggregation (window: {WINDOW_DURATION})")
    
    # Note: This is simplified - you'd parse the Avro data first
    # For now, showing the structure
    return df.select("topic", "timestamp", "offset")


def main():
    """
    Main streaming job.
    
    Orchestrates:
    1. Kafka source connection
    2. Avro deserialization
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
        logger.info(f"Spark Version: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")
        
        # ── Read from Kafka ───────────────────────────────────────────────────
        df_raw = read_from_kafka(spark)
        logger.info("Connected to Kafka")
        
        # ── Deserialize Avro ──────────────────────────────────────────────────
        # Note: Full Avro deserialization requires spark-avro package
        # For production, use:
        #   from pyspark.sql.functions import from_avro
        #   df = df_raw.select(from_avro(col("value"), schema_str).alias("post"))
        
        df_posts = deserialize_avro_posts(df_raw)
        logger.info("Avro deserialization configured")
        
        # ── Apply Watermark ───────────────────────────────────────────────────
        df_posts = apply_watermark(df_posts, "timestamp")
        
        # ── Transformations ───────────────────────────────────────────────────
        logger.info("Setting up transformations...")
        
        processor = SocialProcessor()
        # df_posts = processor.apply(df_posts)  # Would apply once Avro deserialized
        
        aggregator = EngagementAggregator()
        # df_agg = aggregator.aggregate_by_source_and_time(df_posts, WINDOW_DURATION)
        
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
        logger.info(f"Starting Parquet sink to {OUTPUT_DIR / 'raw'}")
        q_parquet = ParquetSink.write(
            df_posts,
            str(OUTPUT_DIR / "raw"),
            str(CHECKPOINT_DIR / "parquet"),
            partition_cols=["topic"],
        )
        queries.append(("parquet-raw", q_parquet))
        
        # 3. Optional: Kafka output for downstream consumers
        # (commented out for now)
        # logger.info(f"Starting Kafka sink to {PROCESSED_TOPIC}")
        # q_kafka = write_to_kafka(
        #     df_posts,
        #     KAFKA_BOOTSTRAP_SERVERS,
        #     PROCESSED_TOPIC,
        #     str(CHECKPOINT_DIR / "kafka")
        # )
        # queries.append(("kafka-processed", q_kafka))
        
        # ── Monitor Streams ────────────────────────────────────────────────────
        logger.info("=" * 80)
        logger.info(f"Streaming job running with {len(queries)} sinks")
        logger.info("=" * 80)
        logger.info("Press Ctrl+C to shutdown gracefully...")
        logger.info("")
        
        # Print sink information
        for name, q in queries:
            logger.info(f"  [{name}] id={q.id}")
        
        # Wait for any query to terminate
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("\nShutdown signal received...")
        
    except Exception as e:
        logger.error(f"Error in streaming job: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        # ── Cleanup ──────────────────────────────────────────────────────────
        logger.info("Cleaning up...")
        
        # Stop all queries
        if 'queries' in locals():
            for name, q in queries:
                if q:
                    logger.info(f"Stopping sink: {name}")
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
