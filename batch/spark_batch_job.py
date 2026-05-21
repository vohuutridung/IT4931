#!/usr/bin/env python3
"""Spark batch recomputation job for Lambda Batch Views."""

from __future__ import annotations

import argparse
import logging
import re

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from config.settings import SPARK_MASTER, STORAGE_BATCH_VIEWS_BASE, STORAGE_RAW_BASE
from config.spark import configure_s3a

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("spark_batch_job")


# Sentiment lexicons (for use in UDF)
POSITIVE = {
    "amazing", "awesome", "beautiful", "benefit", "best", "better", "bullish", "calm",
    "clear", "confident", "constructive", "cute", "enjoy", "excellent", "gain", "gains",
    "good", "great", "growth", "happy", "hope", "hopeful", "improve", "improved",
    "like", "love", "positive", "profit", "profits", "recover", "recovery", "safe",
    "strong", "support", "useful", "win", "winner",
    "ổn", "tốt", "hay", "vui", "thích", "yêu", "đẹp", "xinh", "đỉnh", "tuyệt",
    "tuyệtvời", "hạnhphúc", "ủnghộ", "lãi", "tăng", "mạnh", "khỏe", "an toàn",
}
NEGATIVE = {
    "angry", "awful", "bad", "bearish", "beware", "catastrophic", "concern", "crack",
    "crash", "crisis", "cut", "cuts", "decline", "debt", "drop", "fall", "falling",
    "fear", "fraud", "gap", "hate", "inflation", "loss", "losses", "losing", "miss",
    "negative", "poor", "problem", "risk", "sad", "scam", "terrible", "weak", "worse",
    "worst", "worried",
    "buồn", "tệ", "xấu", "ghét", "chán", "khóc", "giận", "lo", "rủi ro", "lỗ",
    "giảm", "sập", "khủng hoảng", "thất vọng", "đau", "kém",
}
POSITIVE_EMOJI = {"😀", "😃", "😄", "😁", "😊", "😍", "🥰", "❤️", "❤", "👍", "🔥", "✨"}
NEGATIVE_EMOJI = {"😢", "😭", "😡", "😠", "💔", "👎", "😞", "😔", "😟", "😨"}


def _normalize_text(text: str) -> str:
    """Normalize Vietnamese text for sentiment analysis."""
    replacements = {
        "áàảãạăắằẳẵặâấầẩẫậ": "a",
        "éèẻẽẹêếềểễệ": "e",
        "íìỉĩị": "i",
        "óòỏõọôốồổỗộơớờởỡợ": "o",
        "úùủũụưứừửữự": "u",
        "ýỳỷỹỵ": "y",
        "đ": "d",
    }
    output = text.lower()
    for chars, replacement in replacements.items():
        for char in chars:
            output = output.replace(char, replacement)
    return re.sub(r"\s+", " ", output)


def _lexicon_sentiment_batch(text: str) -> float:
    """Lightweight lexicon-based sentiment analysis (for batch layer)."""
    if not text:
        return 0.0
    
    normalized = _normalize_text(text)
    tokens = re.findall(r"[a-z0-9_]+", normalized)
    token_count = max(len(tokens), 1)
    token_set = set(tokens)
    
    positive = len(token_set & {_normalize_text(word) for word in POSITIVE if " " not in word})
    negative = len(token_set & {_normalize_text(word) for word in NEGATIVE if " " not in word})

    for phrase in POSITIVE:
        normalized_phrase = _normalize_text(phrase)
        if " " in phrase and normalized_phrase in normalized:
            positive += 1
    for phrase in NEGATIVE:
        normalized_phrase = _normalize_text(phrase)
        if " " in phrase and normalized_phrase in normalized:
            negative += 1

    positive += sum(text.count(item) for item in POSITIVE_EMOJI)
    negative += sum(text.count(item) for item in NEGATIVE_EMOJI)

    exclamation_boost = min(text.count("!"), 3) * 0.03
    raw = (positive - negative) / max(token_count**0.5, 1)
    if raw > 0:
        raw += exclamation_boost
    elif raw < 0:
        raw -= exclamation_boost
    return max(-1.0, min(1.0, raw))


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
    platforms = [platform] if platform else ["reddit", "facebook", "instagram"]
    dfs = []
    for p in platforms:
        path = f"{base}/{p}"
        try:
            df_p = spark.read.option("basePath", path).parquet(path)
            dfs.append(df_p)
        except Exception:
            pass
    if not dfs:
        path = f"{base}/reddit"
        df = spark.read.option("basePath", path).parquet(path).limit(0)
    else:
        from functools import reduce
        df = reduce(DataFrame.unionByName, dfs)
    if date:
        year, month, day = date.split("-")
        df = df.filter(
            (F.col("year") == int(year))
            & (F.col("month") == int(month))
            & (F.col("day") == int(day))
        )
    return add_common_columns(df)


def add_common_columns(df: DataFrame) -> DataFrame:
    # Create UDF for lightweight sentiment analysis (lexicon-based)
    @F.udf(returnType=DoubleType())
    def compute_sentiment(text: str) -> float:
        if not text:
            return 0.0
        try:
            return _lexicon_sentiment_batch(text)
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
    return df.groupBy("platform", "event_hour").agg(F.avg("sentiment_score").alias("avg_sentiment"))


def top_posts(df: DataFrame) -> DataFrame:
    ranked = df.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("engagement_score"), F.asc("post_id"))))
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
