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
    StructField("comments", ArrayType(StructType([
        StructField("comment_id", StringType()),
        StructField("post_id", StringType()),
        StructField("parent_id", StringType()),
        StructField("author_id", StringType()),
        StructField("author", StringType()),
        StructField("text", StringType()),
        StructField("likes", LongType()),
        StructField("depth", LongType()),
        StructField("created_at", LongType()),
        StructField("extra", StringType()),
    ]))),
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
    return SparkSession.builder.appName("SocialLambdaSpeedLayer").master(SPARK_MASTER).getOrCreate()


def foreach_batch(df, batch_id: int) -> None:
    from speed.nlp_pipeline import enrich_post
    from speed.realtime_stores import RealtimeViewWriter

    rows = [row.asDict(recursive=True) for row in df.collect()]
    enriched = [dict(row, enrichment=enrich_post(row)) for row in rows]
    RealtimeViewWriter().write(enriched)
    logger.info("Processed and stored speed micro-batch %s | records=%d", batch_id, len(enriched))


def main() -> None:
    spark = create_spark()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ",".join(KAFKA_ALL_SOURCE_TOPICS))
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json("json_value", POST_SCHEMA).alias("post"), "json_value")
    )
    good = parsed.filter(
        F.col("post.post_id").isNotNull()
        & F.col("post.platform").isNotNull()
        & F.col("post.created_at").isNotNull()
    ).select("post.*")
    bad = parsed.filter(
        F.col("post.post_id").isNull()
        | F.col("post.platform").isNull()
        | F.col("post.created_at").isNull()
    )

    bad.writeStream.format("parquet").option("path", "/tmp/social-speed/bad_records").option(
        "checkpointLocation", "/tmp/social-speed/checkpoints/bad_records"
    ).start()

    (
        good.writeStream.foreachBatch(foreach_batch)
        .trigger(processingTime=f"{STREAM_TRIGGER_SECS} seconds")
        .option("checkpointLocation", "/tmp/social-speed/checkpoints/enriched")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
