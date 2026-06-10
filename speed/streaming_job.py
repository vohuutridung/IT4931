#!/usr/bin/env python3
"""Spark Structured Streaming job for the SOP Speed Layer."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from config.settings import (
    KAFKA_ALL_SOURCE_TOPICS,
    KAFKA_BOOTSTRAP_SERVERS,
    SPARK_MASTER,
    SPEED_WRITE_BATCH_SIZE,
    STREAM_STARTING_OFFSETS,
    STREAM_TRIGGER_SECS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("speed_streaming")

POST_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("platform", StringType()),
    StructField("source_id", StringType()),
    StructField("author_id", StringType()),
    StructField("content", StringType()),
    StructField("title", StringType()),
    StructField("media_urls", ArrayType(StringType())),
    StructField("hashtags", ArrayType(StringType())),
    StructField("created_at", StringType()),
    StructField("ingested_at", StringType()),
    StructField("metrics", StructType([
        StructField("likes", LongType()),
        StructField("comments", LongType()),
        StructField("shares", LongType()),
        StructField("views", LongType()),
    ])),
])


def create_spark() -> SparkSession:
    from config.spark import configure_s3a
    builder = (
        SparkSession.builder
        .appName("SocialLambdaSpeedLayer")
        .master(SPARK_MASTER)
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
    )
    return configure_s3a(builder).getOrCreate()


_writer = None
_producer = None


def foreach_batch(df, batch_id: int) -> None:
    import json
    from confluent_kafka import Producer
    from speed.nlp_pipeline import enrich_post
    from speed.realtime_stores import RealtimeViewWriter

    global _writer, _producer
    if _writer is None:
        _writer = RealtimeViewWriter()
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    chunk: list[dict] = []
    total = 0
    for spark_row in df.collect():
        row = spark_row.asDict(recursive=True)
        enriched = dict(row, enrichment=enrich_post(row))
        chunk.append(enriched)
        
        _producer.produce(
            "social.enriched.posts",
            key=str(enriched.get("post_id", "")).encode("utf-8"),
            value=json.dumps(enriched).encode("utf-8")
        )
        
        if len(chunk) >= SPEED_WRITE_BATCH_SIZE:
            _writer.write(chunk)
            _producer.flush()
            total += len(chunk)
            chunk.clear()
            
    if chunk:
        _writer.write(chunk)
        _producer.flush()
        total += len(chunk)
        
    logger.info("Processed and stored speed micro-batch %s | records=%d", batch_id, total)


def main() -> None:
    try:
        from warehouse.clickhouse_loader import ensure_realtime_table
        ensure_realtime_table()
    except Exception as exc:
        logger.warning("Could not ensure ClickHouse schema: %s", exc)

    spark = create_spark()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ",".join(KAFKA_ALL_SOURCE_TOPICS))
        .option("startingOffsets", STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json("json_value", POST_SCHEMA).alias("post"), "json_value")
    )
    ts = F.col("post.created_at").cast("timestamp")
    good = parsed.filter(
        F.col("post.post_id").isNotNull()
        & F.col("post.platform").isNotNull()
        & F.col("post.created_at").isNotNull()
        & (ts >= F.lit("2026-01-01 00:00:00"))
    ).select("post.*")
    bad = parsed.filter(
        F.col("post.post_id").isNull()
        | F.col("post.platform").isNull()
        | F.col("post.created_at").isNull()
        | ts.isNull()
        | (ts < F.lit("2026-01-01 00:00:00"))
    )

    (
        bad.selectExpr("CAST(json_value AS STRING) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "social.dlq")
        .option("checkpointLocation", "s3a://social-lake/checkpoints/speed/dlq")
        .start()
    )

    (
        good.writeStream.foreachBatch(foreach_batch)
        .trigger(processingTime=f"{STREAM_TRIGGER_SECS} seconds")
        .option("checkpointLocation", "s3a://social-lake/checkpoints/speed/enriched")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
