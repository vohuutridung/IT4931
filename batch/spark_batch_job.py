#!/usr/bin/env python3
"""Spark batch recomputation job for Lambda Batch Views."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from config.settings import SPARK_MASTER, STORAGE_BATCH_VIEWS_BASE, STORAGE_RAW_BASE
from config.spark import configure_s3a

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("spark_batch_job")


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SocialLambdaBatchViews")
        .master(SPARK_MASTER)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return configure_s3a(builder).getOrCreate()


def read_raw(spark: SparkSession, platform: str | None, date: str | None) -> DataFrame:
    base = STORAGE_RAW_BASE.rstrip("/")
    path = f"{base}/{platform}" if platform else base
    df = spark.read.option("basePath", base).parquet(path)
    if date:
        year, month, day = date.split("-")
        df = df.filter(
            (F.col("year") == int(year))
            & (F.col("month") == int(month))
            & (F.col("day") == int(day))
        )
    return add_common_columns(df)


def add_common_columns(df: DataFrame) -> DataFrame:
    sentiment = F.coalesce(F.col("sentiment_score"), F.lit(0.0)) if "sentiment_score" in df.columns else F.lit(0.0)
    engagement = (
        F.coalesce(F.col("likes"), F.lit(0))
        + F.coalesce(F.col("comments"), F.lit(0)) * 2
        + F.coalesce(F.col("shares"), F.lit(0)) * 3
    )
    return (
        df
        .withColumn("event_ts", F.col("created_at").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
        .withColumn("event_week", F.date_trunc("week", F.col("event_ts")))
        .withColumn("sentiment_score", sentiment.cast("double"))
        .withColumn("engagement_score", engagement.cast("long"))
    )


def write_view(df: DataFrame, name: str, partition_cols: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    path = f"{STORAGE_BATCH_VIEWS_BASE.rstrip('/')}/{name}"
    writer.parquet(path)
    logger.info("Wrote batch view %s to %s", name, path)


def platform_daily_stats(df: DataFrame) -> DataFrame:
    return df.groupBy("platform", "event_date").agg(
        F.count("*").alias("post_count"),
        F.avg("sentiment_score").alias("avg_sentiment"),
        F.sum("engagement_score").alias("total_engagement"),
    )


def top_hashtags_weekly(df: DataFrame) -> DataFrame:
    exploded = df.select("platform", "event_week", F.explode_outer("hashtags").alias("hashtag"))
    ranked = (
        exploded
        .filter(F.col("hashtag").isNotNull() & (F.length("hashtag") > 0))
        .groupBy("platform", "event_week", F.lower("hashtag").alias("hashtag"))
        .agg(F.count("*").alias("frequency"))
        .withColumn("rank", F.row_number().over(Window.partitionBy("platform", "event_week").orderBy(F.desc("frequency"))))
    )
    return ranked.filter(F.col("rank") <= 100)


def author_activity(df: DataFrame) -> DataFrame:
    return df.groupBy("platform", "author_id").agg(
        F.count("*").alias("post_count"),
        F.avg("sentiment_score").alias("avg_sentiment"),
        F.sum("engagement_score").alias("total_reach"),
    )


def sentiment_time_series(df: DataFrame) -> DataFrame:
    return df.groupBy("platform", "event_hour").agg(F.avg("sentiment_score").alias("avg_sentiment"))


def top_posts(df: DataFrame) -> DataFrame:
    ranked = df.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("engagement_score"))))
    return ranked.filter(F.col("rank") <= 1000).select(
        "rank", "post_id", "platform", "event_ts", "author_id", "content", "engagement_score"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD partition to recompute")
    parser.add_argument("--platform", choices=["reddit", "facebook", "instagram"])
    args = parser.parse_args()

    spark = create_spark()
    try:
        raw = read_raw(spark, args.platform, args.date).cache()
        write_view(platform_daily_stats(raw), "platform_daily_stats", ["platform"])
        write_view(top_hashtags_weekly(raw), "top_hashtags_weekly", ["platform"])
        write_view(author_activity(raw), "author_activity", ["platform"])
        write_view(sentiment_time_series(raw), "sentiment_time_series", ["platform"])
        write_view(top_posts(raw), "top_posts")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
