"""
spark_etl.py
─────────────
Spark batch job:
  1. Đọc Parquet từ MinIO (social-raw/raw/*)
  2. Clean & enrich
  3. Ghi Parquet sạch vào MinIO (social-clean/clean/*)
  4. Upsert vào MongoDB

Chạy:
  spark-submit --master local[*] \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,\
               org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    batch/etl/spark_etl.py [--date 2026-03-25] [--source reddit]
"""
from typing import Optional
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET_RAW, MINIO_BUCKET_CLEAN,
    MINIO_RAW_PREFIX, MINIO_CLEAN_PREFIX,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION,
    SPARK_APP_NAME, SPARK_MASTER, S3A_ENDPOINT,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s")
logger = logging.getLogger("spark_etl")


# ── 1. SparkSession ───────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)

        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")

        # credentials
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Mongo
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)

        # overwrite partition
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── 2. Read raw Parquet ───────────────────────────────────────────────────────

def read_raw(spark: SparkSession, date: Optional[str], source: Optional[str]) -> DataFrame:

    if date and source:
        dt = datetime.strptime(date, "%Y-%m-%d")
        path = (
            f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/{source}/"
            f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        )

    elif source:
        path = f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/{source}/"

    else:
        path = f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/"

    base_path = f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/"

    logger.warning("⚠️ Reading path: %s", path)

    df = (
        spark.read
            .option("basePath", base_path)
            .option("recursiveFileLookup", "true")
            .parquet(path)
    )

    logger.info(">>> COUNT START")
    cnt = df.count()
    logger.info(">>> RAW COUNT = %d", cnt)

    return df


# ── 3. ETL Transformations ────────────────────────────────────────────────────

def clean(df: DataFrame) -> DataFrame:
    df = _drop_duplicates(df)
    df = _filter_empty(df)
    df = _normalize_text(df)
    df = _add_derived_columns(df)
    df = _add_engagement_tier(df)
    return df


def _drop_duplicates(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("post_id").orderBy(F.desc("ingest_time"))
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def _filter_empty(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("content").isNotNull() &
        (F.length(F.trim(F.col("content"))) > 5)
    )


def _normalize_text(df: DataFrame) -> DataFrame:
    url_pattern = r"https?://\S+"
    return (
        df
        .withColumn("content_clean",
                    F.regexp_replace(F.trim(F.col("content")), url_pattern, " "))
        .withColumn("content_clean",
                    F.regexp_replace(F.col("content_clean"), r"\s+", " "))
        .withColumn("content_clean", F.trim(F.col("content_clean")))
        .withColumn("hashtags", F.transform(F.col("hashtags"), F.lower))
    )


def _add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_date",       F.to_date(F.col("event_time")))
        .withColumn("event_year",       F.year(F.col("event_time")))
        .withColumn("event_month",      F.month(F.col("event_time")))
        .withColumn("event_hour",       F.hour(F.col("event_time")))
        .withColumn("event_weekday",    F.dayofweek(F.col("event_time")))
        .withColumn("content_len",      F.length(F.col("content_clean")))
        .withColumn("hashtag_count",    F.size(F.col("hashtags")))
        .withColumn("total_engagement",
                    F.col("likes") + F.col("comments") + F.col("shares"))
        .withColumn("etl_version",      F.lit("1.0"))
        .withColumn("processed_time",   F.current_timestamp())
    )


def _add_engagement_tier(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "engagement_tier",
        F.when(F.col("total_engagement") > 1000, F.lit("viral"))
         .when(F.col("total_engagement") > 100,  F.lit("high"))
         .when(F.col("total_engagement") > 10,   F.lit("medium"))
         .otherwise(F.lit("low"))
    )


# ── 4. Write clean → MinIO ────────────────────────────────────────────────────

def write_clean_minio(df: DataFrame) -> None:
    out_path = f"s3a://{MINIO_BUCKET_CLEAN}/{MINIO_CLEAN_PREFIX}/"
    logger.info("Writing clean data → %s", out_path)
    (
        df.coalesce(2)  
        .write
        .mode("append")
        .partitionBy("source", "event_year", "event_month", "event_date")
        .parquet(out_path)
    )
    logger.info("Clean data written to MinIO.")


# ── 5. Upsert → MongoDB ───────────────────────────────────────────────────────

def write_mongodb(df: DataFrame) -> None:
    mongo_cols = [
        "post_id", "source",
        "event_time", "event_date", "event_year", "event_month",
        "event_hour", "event_weekday",
        "author_id", "author_name",
        "content_clean", "url", "hashtags", "hashtag_count",
        "likes", "comments", "shares", "score",
        "total_engagement", "engagement_tier",
        "etl_version", "processed_time",
    ]
    available   = set(df.columns)
    select_cols = [c for c in mongo_cols if c in available]
    mongo_df    = df.select(*select_cols).withColumnRenamed("content_clean", "content")

    logger.info("Writing to MongoDB %s.%s", MONGO_DB, MONGO_COLLECTION)
    (
        mongo_df.write
                .format("mongodb")
                .option("database",       MONGO_DB)
                .option("collection",     MONGO_COLLECTION)
                .option("upsertDocument", "true")
                .option("idFieldList",    "post_id")
                .mode("append")
                .save()
    )
    logger.info("MongoDB write complete.")


# ── 6. Stats ──────────────────────────────────────────────────────────────────

def print_stats(raw_count: int, clean_df: DataFrame) -> None:
    clean_count = clean_df.count()
    dropped     = raw_count - clean_count
    logger.info("=" * 55)
    logger.info("ETL STATS")
    logger.info("  Raw records   : %d", raw_count)
    logger.info("  Clean records : %d", clean_count)
    logger.info("  Dropped       : %d (%.1f%%)",
                dropped, dropped / raw_count * 100 if raw_count else 0)
    logger.info("  Breakdown by source:")
    clean_df.groupBy("source").count().show(truncate=False)
    logger.info("  Engagement tier distribution:")
    clean_df.groupBy("source", "engagement_tier").count() \
            .orderBy("source", "count") \
            .show(truncate=False)
    logger.info("=" * 55)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Social Batch ETL")
    parser.add_argument("--date",   help="Chỉ xử lý ngày cụ thể (YYYY-MM-DD)")
    parser.add_argument("--source", choices=["facebook", "instagram", "reddit"])
    parser.add_argument("--skip-mongo", action="store_true")
    args = parser.parse_args()

    spark    = create_spark()
    raw_df   = read_raw(spark, args.date, args.source)

    raw_df.cache()
    raw_count = raw_df.count()
    logger.info("Raw records: %d", raw_count)

    clean_df = clean(raw_df)
    clean_df.cache()

    write_clean_minio(clean_df)

    if not args.skip_mongo:
        write_mongodb(clean_df)

    print_stats(raw_count, clean_df)

    raw_df.unpersist()
    clean_df.unpersist()
    spark.stop()
    logger.info("ETL job complete.")


if __name__ == "__main__":
    main()
