"""
Social media post processor with advanced transformations.

Mirrors the batch ETL clean/enrich logic for streaming DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array, coalesce, col, current_timestamp, dayofweek, from_unixtime, hour,
    length, lit, lower, month, regexp_replace, row_number, size, to_date,
    to_timestamp, transform, trim, when, year,
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, LongType


class SocialProcessor:
    """Apply the same core clean/enrich steps as batch/etl/spark_etl.py."""

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Apply streaming-safe equivalent of batch ETL clean().

        Args:
            df: Input DataFrame with social posts

        Returns:
            DataFrame with clean/enriched columns aligned with batch ETL
        """
        df = self.flatten_engagement(df)
        df = self.normalize_time_columns(df)
        df = self.deduplicate(df)
        df = self.filter_empty(df)
        df = self.normalize_text(df)
        df = self.add_derived_columns(df)
        df = self.add_engagement_tier(df)
        return df

    @staticmethod
    def flatten_engagement(df: DataFrame) -> DataFrame:
        """Flatten normalized nested engagement into batch-compatible columns."""
        return (
            df
            .withColumn("likes", coalesce(col("engagement.likes"), lit(0)).cast(LongType()))
            .withColumn("comments_count", coalesce(col("engagement.comments"), lit(0)).cast(LongType()))
            .withColumn("shares", coalesce(col("engagement.shares"), lit(0)).cast(LongType()))
            .withColumn("score", coalesce(col("engagement.score"), lit(0)).cast(LongType()))
            .withColumn("video_views", coalesce(col("engagement.video_views"), lit(0)).cast(LongType()))
            .withColumn(
                "comments_normalized_count",
                coalesce(col("engagement.comments_normalized_count"), lit(0)).cast(IntegerType()),
            )
            .drop("engagement", "comments")
            .withColumnRenamed("comments_count", "comments")
        )

    @staticmethod
    def normalize_time_columns(df: DataFrame) -> DataFrame:
        """Convert Kafka JSON millisecond timestamps into Spark TimestampType."""
        ingest_ts = to_timestamp(from_unixtime((col("ingest_time") / 1000).cast("double")))
        if "event_timestamp" not in df.columns:
            df = df.withColumn(
                "event_timestamp",
                to_timestamp(from_unixtime((col("event_time") / 1000).cast("double"))),
            )
        return (
            df
            .withColumn("event_time", col("event_timestamp"))
            .withColumn("ingest_time", ingest_ts)
        )

    @staticmethod
    def deduplicate(df: DataFrame, key_col: str = "post_id") -> DataFrame:
        """
        Remove duplicate posts based on key.
        
        Keeps one occurrence per key. Streaming DataFrames cannot use row_number
        windows, so they use Spark's stateful dropDuplicates API.
        
        Args:
            df: Input DataFrame
            key_col: Column to deduplicate on (default: post_id)
            
        Returns:
            Deduplicated DataFrame
        """
        if df.isStreaming:
            if hasattr(df, "dropDuplicatesWithinWatermark"):
                return df.dropDuplicatesWithinWatermark([key_col])
            subset = [key_col]
            if "event_timestamp" in df.columns:
                subset.append("event_timestamp")
            return df.dropDuplicates(subset)

        window_spec = Window.partitionBy(key_col).orderBy(col("ingest_time").desc())

        return (
            df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

    @staticmethod
    def filter_empty(df: DataFrame) -> DataFrame:
        return df.filter(
            col("content").isNotNull() &
            (length(trim(col("content"))) > 5)
        )

    @staticmethod
    def normalize_text(df: DataFrame) -> DataFrame:
        url_pattern = r"https?://\S+"
        return (
            df
            .withColumn("content_clean", regexp_replace(trim(col("content")), url_pattern, " "))
            .withColumn("content_clean", regexp_replace(col("content_clean"), r"\s+", " "))
            .withColumn("content_clean", trim(col("content_clean")))
            .withColumn("hashtags", transform(coalesce(col("hashtags"), array()), lower))
        )

    @staticmethod
    def add_derived_columns(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("event_date", to_date(col("event_time")))
            .withColumn("event_year", year(col("event_time")))
            .withColumn("event_month", month(col("event_time")))
            .withColumn("event_hour", hour(col("event_time")))
            .withColumn("event_weekday", dayofweek(col("event_time")))
            .withColumn("content_len", length(col("content_clean")))
            .withColumn("hashtag_count", size(col("hashtags")))
            .withColumn("total_engagement", col("likes") + col("comments") + col("shares"))
            .withColumn("etl_version", lit("1.0"))
            .withColumn("processed_time", current_timestamp())
        )

    @staticmethod
    def add_engagement_tier(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "engagement_tier",
            when(col("total_engagement") > 1000, lit("viral"))
            .when(col("total_engagement") > 100, lit("high"))
            .when(col("total_engagement") > 10, lit("medium"))
            .otherwise(lit("low"))
        )
