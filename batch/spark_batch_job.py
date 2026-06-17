#!/usr/bin/env python3
"""Spark batch recomputation job for Lambda Batch Views."""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark import StorageLevel
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from config.settings import (
    EVENT_TIME_MAX,
    EVENT_TIME_MIN,
    SENTIMENT_ARTIFACTS_DIR,
    SPARK_MASTER,
    STORAGE_BATCH_VIEWS_BASE,
    STORAGE_RAW_BASE,
)
from config.spark import configure_s3a
from shared.sentiment import lexicon_sentiment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("spark_batch_job")

DEFAULT_BATCH_INPUT_PARTITIONS = int(os.getenv("BATCH_INPUT_PARTITIONS", "8"))   # Giảm từ 64 → 8 (phù hợp 1 core)
DEFAULT_BATCH_SHUFFLE_PARTITIONS = int(os.getenv("BATCH_SHUFFLE_PARTITIONS", "4"))  # Giảm từ 64 → 4
BATCH_COLUMNS = [
    "post_id",
    "platform",
    "event_ts",
    "event_date",
    "event_hour",
    "event_week",
    "author_id",
    "content",
    "hashtags",
    "sentiment_score",
    "engagement_score",
]


def _apply_event_time_bounds(df: DataFrame) -> DataFrame:
    bounded = df
    if EVENT_TIME_MIN:
        bounded = bounded.filter(F.col("event_ts") >= F.lit(EVENT_TIME_MIN))
    if EVENT_TIME_MAX:
        max_value = EVENT_TIME_MAX
        if len(max_value) == 10 and max_value[4] == "-" and max_value[7] == "-":
            max_value = f"{max_value} 23:59:59"
        bounded = bounded.filter(F.col("event_ts") <= F.lit(max_value))
    return bounded


def _lexicon_sentiment_batch(text: str) -> float:
    """Lexicon-based sentiment — delegates to shared module for consistency."""
    return lexicon_sentiment(text)


def create_spark(shuffle_partitions: int) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SocialLambdaBatchViews")
        .master(SPARK_MASTER)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        # Tolerate files that appear in listing but disappear before read
        # (race condition with concurrent object-store-writer ingestion)
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        # --- Resource limits: tránh chiếm toàn bộ worker ---
        .config("spark.cores.max", "1")                      # Chỉ dùng 1 core
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "512m")
        .config("spark.dynamicAllocation.enabled", "false")  # Tắt dynamic alloc
        # --- Timeout: tránh zombie app chiếm slot mãi mãi ---
        .config("spark.network.timeout", "300s")             # 5 phút network timeout
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.rpc.askTimeout", "120s")
        .config("spark.sql.broadcastTimeout", "120")
    )
    return configure_s3a(builder).getOrCreate()


def read_raw(spark: SparkSession, platform: str | None, date: str | None) -> DataFrame:
    base = STORAGE_RAW_BASE.rstrip("/")
    platforms = [platform] if platform else ["reddit", "facebook", "instagram"]
    dfs = []
    read_errors = []
    for p in platforms:
        path = f"{base}/{p}"
        if not _spark_path_exists(spark, path):
            logger.warning("Raw path does not exist, skipping platform=%s path=%s", p, path)
            continue
        try:
            df_p = spark.read.option("basePath", path).parquet(path)
            dfs.append(df_p)
        except Exception as exc:
            read_errors.append((p, path, exc))
    if read_errors:
        details = "; ".join(f"{p} path={path}: {exc}" for p, path, exc in read_errors)
        raise RuntimeError(f"Failed to read raw parquet data: {details}")
    if not dfs:
        raise FileNotFoundError(f"No raw data paths found under {base} for platforms={platforms}")
    else:
        from functools import reduce
        df = reduce(DataFrame.unionByName, dfs)
        df = df.dropDuplicates(["post_id"])
    if date:
        year, month, day = date.split("-")
        df = df.filter(
            (F.col("year") == int(year))
            & (F.col("month") == int(month))
            & (F.col("day") == int(day))
        )
    return add_common_columns(df)


def _spark_path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hadoop_conf)
    return bool(fs.exists(fs_path))


_sentiment_pipeline = None
_transformers_available = None


def _get_transformers_sentiment():
    global _sentiment_pipeline, _transformers_available
    if _transformers_available is False:
        return None
    if _sentiment_pipeline is None:
        import os
        model_path = os.path.join(SENTIMENT_ARTIFACTS_DIR, "fine_tuned_phobert")
        if not os.path.exists(model_path):
            _transformers_available = False
            return None
        try:
            from transformers import pipeline
            _sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=model_path,
                tokenizer=model_path,
            )
            _transformers_available = True
        except Exception:
            _transformers_available = False
    return _sentiment_pipeline


def _phobert_sentiment_batch(text: str) -> float:
    nlp = _get_transformers_sentiment()
    if nlp is not None:
        try:
            res = nlp(text[:512])[0]
            label = res["label"].upper()
            score = float(res["score"])
            if label in ("POS", "POSITIVE", "LABEL_2"):
                return score
            elif label in ("NEG", "NEGATIVE", "LABEL_0"):
                return -score
            else:
                return 0.0
        except Exception:
            return _lexicon_sentiment_batch(text)
    return _lexicon_sentiment_batch(text)


def add_common_columns(df: DataFrame) -> DataFrame:
    # Create UDF for sentiment analysis (fine-tuned PhoBERT with lexicon fallback)
    @F.udf(returnType=DoubleType())
    def compute_sentiment(text: str) -> float:
        if not text:
            return 0.0
        try:
            return _phobert_sentiment_batch(text)
        except Exception:
            return 0.0
    
    # Use existing sentiment_score or compute from content
    if "sentiment_score" in df.columns:
        sentiment = F.coalesce(F.col("sentiment_score"), compute_sentiment(F.col("content")))
    else:
        sentiment = compute_sentiment(F.col("content")) if "content" in df.columns else F.lit(0.0)
    
    engagement = (
        F.coalesce(F.col("likes"), F.lit(0))
        + F.coalesce(F.col("comments"), F.lit(0)) * 2
        + F.coalesce(F.col("shares"), F.lit(0)) * 3
    )
    enriched = (
        df
        .withColumn("event_ts", F.col("created_at").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
        .withColumn("event_week", F.date_trunc("week", F.col("event_ts")))
        .withColumn("sentiment_score", sentiment.cast("double"))
        .withColumn("engagement_score", engagement.cast("long"))
    )
    return _apply_event_time_bounds(enriched)


def project_batch_columns(df: DataFrame) -> DataFrame:
    """Keep only columns needed by batch views before persisting."""
    return df.select(*(col for col in BATCH_COLUMNS if col in df.columns))


def prepare_cached_raw(df: DataFrame, input_partitions: int) -> DataFrame:
    projected = project_batch_columns(df)
    if input_partitions > 0:
        projected = projected.coalesce(input_partitions)
    cached = projected.persist(StorageLevel.DISK_ONLY)
    rows = cached.count()
    logger.info(
        "Prepared batch input | rows=%d partitions=%d storage=DISK_ONLY",
        rows,
        cached.rdd.getNumPartitions(),
    )
    if rows == 0:
        raise ValueError("No raw rows found for selected batch input")
    return cached


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
        .withColumn("rank", F.row_number().over(Window.partitionBy("platform", "event_week").orderBy(F.desc("frequency"), F.asc("hashtag"))))
    )
    return ranked.filter(F.col("rank") <= 100)


def author_activity(df: DataFrame) -> DataFrame:
    return df.groupBy("platform", "author_id").agg(
        F.count("*").alias("post_count"),
        F.avg("sentiment_score").alias("avg_sentiment"),
        F.sum("engagement_score").alias("total_reach"),
    )


def sentiment_time_series(df: DataFrame) -> DataFrame:
    eng = F.sum(F.col("engagement_score").cast("double"))
    weighted = F.sum(F.col("sentiment_score").cast("double") * F.col("engagement_score").cast("double"))
    return df.groupBy("platform", "event_hour").agg(
        F.avg("sentiment_score").alias("avg_sentiment"),
        F.count("*").alias("post_count"),
        F.sum(F.when(F.col("sentiment_score") > 0.03, 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("sentiment_score") < -0.03, 1).otherwise(0)).alias("negative_count"),
        F.sum(F.when(
            (F.col("sentiment_score") >= -0.03) & (F.col("sentiment_score") <= 0.03), 1
        ).otherwise(0)).alias("neutral_count"),
        F.when(eng > 0, weighted / eng).otherwise(F.avg("sentiment_score")).alias("weighted_sentiment"),
    )


def top_posts(df: DataFrame) -> DataFrame:
    ranked = df.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("engagement_score"), F.asc("post_id"))))
    return ranked.filter(F.col("rank") <= 1000).select(
        "rank",
        "post_id",
        "platform",
        "event_ts",
        "author_id",
        "content",
        "sentiment_score",
        "engagement_score",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD partition to recompute")
    parser.add_argument("--platform", choices=["reddit", "facebook", "instagram"])
    parser.add_argument("--input-partitions", type=int, default=DEFAULT_BATCH_INPUT_PARTITIONS)
    parser.add_argument("--shuffle-partitions", type=int, default=DEFAULT_BATCH_SHUFFLE_PARTITIONS)
    args = parser.parse_args()

    spark = create_spark(args.shuffle_partitions)
    raw = None
    try:
        raw = prepare_cached_raw(read_raw(spark, args.platform, args.date), args.input_partitions)
        write_view(platform_daily_stats(raw), "platform_daily_stats", ["platform"])
        write_view(top_hashtags_weekly(raw), "top_hashtags_weekly", ["platform"])
        write_view(author_activity(raw), "author_activity", ["platform"])
        write_view(sentiment_time_series(raw), "sentiment_time_series", ["platform"])
        write_view(top_posts(raw), "top_posts")
    finally:
        if raw is not None:
            raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
