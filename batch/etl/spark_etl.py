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
import os
import json
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType
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


COMMENT_SCHEMA = ArrayType(StructType([
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
]), True)


# ── Incremental Processing ────────────────────────────────────────────────────

# Trong docker-compose, airflow-scheduler mount ./:/opt/social_pipeline:ro
# Nên dùng /tmp/ với fallback, nhưng chưa được mount volume riêng. 
# Dùng env var ETL_TRACKER_FILE để override khi cài sằn trong docker-compose.
PROCESSED_TRACKER_FILE = os.getenv(
    "ETL_TRACKER_FILE",
    "/opt/social_pipeline/data/.etl_state/processed_dates.json"
)


def load_processed_dates() -> dict:
    """Load processed (source, date) from tracker file."""
    if os.path.exists(PROCESSED_TRACKER_FILE):
        with open(PROCESSED_TRACKER_FILE, "r") as f:
            return json.load(f)
    return {}


def save_processed_dates(processed: dict) -> None:
    """Save processed dates to tracker file."""
    # Fix #5: tự tạo thư mục cha nếu chưa tồn tại
    tracker_path = Path(PROCESSED_TRACKER_FILE)
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    with open(PROCESSED_TRACKER_FILE, "w") as f:
        json.dump(processed, f, indent=2)


def should_process(source: str, date: str) -> bool:
    """Check if (source, date) should be processed based on tracker."""
    processed = load_processed_dates()
    key = f"{source}_{date}"
    
    if key not in processed:
        return True  # Never processed
    
    # Do chỉ có 1 bộ data tĩnh, nếu đã xử lý rồi thì bỏ qua luôn (không chạy lại)
    return False


# ── 1. SparkSession ───────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)

        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)
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

def read_raw(spark: SparkSession, date: Optional[str], source: Optional[str]) -> Optional[DataFrame]:

    if date and source:
        dt = datetime.strptime(date, "%Y-%m-%d")
        path = (
            f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/{source}/"
            f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        )

        # Incremental: Skip if already processed recently
        if not should_process(source, date):
            logger.info("Skipping already processed: %s %s", source, date)
            return None

    elif date:
        dt = datetime.strptime(date, "%Y-%m-%d")
        path = (
            f"s3a://{MINIO_BUCKET_RAW}/{MINIO_RAW_PREFIX}/*/"
            f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        )
        
        # Incremental: Skip if already processed
        if not should_process("ALL", date):
            logger.info("Skipping already processed: ALL %s", date)
            return None

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

    # Optimize: Cache immediately after read
    df.cache()

    return df


# ── 3. ETL Transformations ────────────────────────────────────────────────────

def clean(df: DataFrame) -> DataFrame:
    # Optimize: Chain transformations efficiently
    df = _drop_duplicates(df)
    df = _filter_empty(df)
    df = _normalize_text(df)
    df = _add_derived_columns(df)
    df = _add_engagement_tier(df)
    return df


def _drop_duplicates(df: DataFrame) -> DataFrame:
    # WARNING: Vì DAG chạy dạng incremental theo từng ngày (--date), 
    # hàm này chỉ có tác dụng deduplicate các bài post trong CÙNG 1 NGÀY.
    # Nếu bài post trùng lặp rơi vào 2 ngày khác nhau, nó vẫn sẽ lọt qua
    # và tạo file rác trên bucket social-clean. 
    # Tuy nhiên, bước ghi vào MongoDB dùng upsert nên database cuối vẫn sạch.
    # Để fix triệt để MinIO, cần cân nhắc sử dụng Delta Lake / Apache Iceberg.
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
        .withColumn("hashtags", F.transform(F.coalesce(F.col("hashtags"), F.array()), F.lower))
    )


def _add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Thêm các cột phái sinh từ thời gian, nội dung và engagement.

    weighted_engagement:
      likes × 1  +  comments × 2  +  shares × 4
      + cast(log1p(video_views) × 10, LongType)

      Lý do trọng số:
        - share   : mang content ra ngoài network, viral signal mạnh nhất (~4×)
        - comment : engagement chủ động, mạnh hơn like (~2×)
        - like    : passive engagement, base weight = 1
        - views   : log-scale (tránh video triệu view lấn át hoàn toàn)

      total_engagement giữ lại để backward compat.
    """
    _likes    = F.coalesce(F.col("likes"),       F.lit(0)).cast("long")
    _comments = F.coalesce(F.col("comments"),    F.lit(0)).cast("long")
    _shares   = F.coalesce(F.col("shares"),      F.lit(0)).cast("long")
    _views    = F.coalesce(F.col("video_views"), F.lit(0)).cast("double")

    _weighted = (
        _likes
        + _comments * 2
        + _shares * 4
        + (F.log1p(_views) * 10).cast("long")
    )

    return (
        df
        .withColumn("event_date",         F.to_date(F.col("event_time")))
        .withColumn("event_year",         F.year(F.col("event_time")))
        .withColumn("event_month",        F.month(F.col("event_time")))
        .withColumn("event_hour",         F.hour(F.col("event_time")))
        .withColumn("event_weekday",      F.dayofweek(F.col("event_time")))
        .withColumn("content_len",        F.length(F.col("content_clean")))
        .withColumn("hashtag_count",      F.size(F.col("hashtags")))
        .withColumn("total_engagement",   _likes + _comments + _shares)  # backward compat
        .withColumn("weighted_engagement", _weighted)
        .withColumn("etl_version",        F.lit("1.0"))
        .withColumn("processed_time",     F.current_timestamp())
    )


def _add_engagement_tier(df: DataFrame) -> DataFrame:
    """
    Phân loại viral tier dựa trên weighted_engagement và platform.

    Tier  | Reddit | Instagram | Facebook/other
    ------|--------|-----------|---------------
    viral | ≥ 8000 |  ≥ 10000  |   ≥ 2000
    high  | ≥  800 |  ≥  1000  |   ≥  200
    medium| ≥   80 |  ≥   100  |   ≥   20
    low   |  else  |    else   |    else
    """
    we  = F.col("weighted_engagement")
    src = F.col("source")

    is_reddit = (src == "reddit")
    is_ig     = (src == "instagram")
    is_other  = ~is_reddit & ~is_ig

    return df.withColumn(
        "engagement_tier",
        F.when(
            (is_reddit & (we >= 8_000)) |
            (is_ig     & (we >= 10_000)) |
            (is_other  & (we >= 2_000)),
            F.lit("viral")
        ).when(
            (is_reddit & (we >= 800)) |
            (is_ig     & (we >= 1_000)) |
            (is_other  & (we >= 200)),
            F.lit("high")
        ).when(
            (is_reddit & (we >= 80)) |
            (is_ig     & (we >= 100)) |
            (is_other  & (we >= 20)),
            F.lit("medium")
        ).otherwise(F.lit("low"))
    )


# ── 4. Write clean → MinIO ────────────────────────────────────────────────────

def write_clean_minio(df: DataFrame) -> None:
    out_path = f"s3a://{MINIO_BUCKET_CLEAN}/{MINIO_CLEAN_PREFIX}/"
    logger.info("Writing clean data → %s", out_path)
    (
        df.repartition("source", "event_year", "event_month", "event_date")
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
        "comments_list",
        "likes", "comments", "shares", "score", "video_views",
        "total_engagement", "weighted_engagement", "engagement_tier",
        "etl_version", "processed_time",
    ]
    available   = set(df.columns)
    select_cols = [c for c in mongo_cols if c in available]
    mongo_df    = df.select(*select_cols).withColumnRenamed("content_clean", "content")

    if "comments_list" in available:
        mongo_df = mongo_df.withColumn(
            "comments_list",
            F.from_json(F.col("comments_list"), COMMENT_SCHEMA)
        )

    mongo_df = mongo_df.withColumn("_id", F.col("post_id"))

    logger.info("Writing to MongoDB %s.%s", MONGO_DB, MONGO_COLLECTION)
    (
        mongo_df.write
                .format("mongodb")
                .option("database",       MONGO_DB)
                .option("collection",     MONGO_COLLECTION)
                .option("upsertDocument", "true")
                .option("idFieldList",    "_id")
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
    if raw_df is None:
        spark.stop()
        logger.info("No ETL work needed.")
        return

    raw_df.cache()
    raw_count = raw_df.count()
    logger.info("Raw records: %d", raw_count)

    clean_df = clean(raw_df).cache()

    write_clean_minio(clean_df)

    if not args.skip_mongo:
        write_mongodb(clean_df)

    print_stats(raw_count, clean_df)

    # Incremental: Mark as processed
    if args.date:
        processed = load_processed_dates()
        src_key = args.source if args.source else "ALL"
        key = f"{src_key}_{args.date}"
        processed[key] = datetime.now().isoformat()
        save_processed_dates(processed)
        logger.info("Marked as processed: %s", key)

    raw_df.unpersist()
    clean_df.unpersist()
    spark.stop()
    logger.info("ETL job complete.")


if __name__ == "__main__":
    main()
