"""
Engagement aggregation processors.

Window-based aggregations for real-time engagement tracking.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, min as spark_min,
    window, stddev_pop,
)


class EngagementAggregator:
    """Aggregate engagement metrics by time window and source."""

    @staticmethod
    def _time_col(df: DataFrame) -> str:
        return "event_timestamp" if "event_timestamp" in df.columns else "event_time"
    
    @staticmethod
    def aggregate_by_source_and_time(
        df: DataFrame, window_duration: str = "10 minutes"
    ) -> DataFrame:
        """
        Aggregate engagement metrics by source and time window.
        
        Calculates:
        - num_posts: Number of posts
        - avg_likes, avg_comments, avg_shares
        - max_likes, min_likes
        - stddev of likes (variation)
        
        Args:
            df: Input DataFrame with event_time
            window_duration: Window duration (e.g., "10 minutes")
            
        Returns:
            Aggregated DataFrame with columns:
            - time_start, time_end
            - source
            - num_posts
            - avg_likes, avg_comments, avg_shares
            - max_likes, min_likes
            - stddev_likes
        """
        return (
            df
            .groupBy(
                window(col(EngagementAggregator._time_col(df)), window_duration),
                col("source")
            )
            .agg(
                count("*").alias("num_posts"),
                avg(col("likes")).alias("avg_likes"),
                avg(col("comments")).alias("avg_comments"),
                avg(col("shares")).alias("avg_shares"),
                spark_max(col("likes")).alias("max_likes"),
                spark_min(col("likes")).alias("min_likes"),
                stddev_pop(col("likes")).alias("stddev_likes"),
            )
            .select(
                col("window.start").alias("time_start"),
                col("window.end").alias("time_end"),
                col("source"),
                col("num_posts"),
                col("avg_likes"),
                col("avg_comments"),
                col("avg_shares"),
                col("max_likes"),
                col("min_likes"),
                col("stddev_likes"),
            )
        )
    
    @staticmethod
    def trending_hashtags(
        df: DataFrame, window_duration: str = "10 minutes", top_n: int = 10
    ) -> DataFrame:
        """
        Identify trending hashtags in each time window.
        
        Args:
            df: Input DataFrame with hashtags array
            window_duration: Window duration
            top_n: Number of top hashtags to return (default: 10)
            
        Returns:
            DataFrame with hashtag trends
        """
        from pyspark.sql.functions import explode, row_number
        from pyspark.sql.window import Window
        
        time_col = EngagementAggregator._time_col(df)
        window_spec = Window.partitionBy(
            window(col(time_col), window_duration)
        ).orderBy(col("count").desc())
        
        return (
            df
            .withColumn("hashtag", explode(col("hashtags")))
            .filter(col("hashtag").isNotNull())
            .groupBy(
                window(col(time_col), window_duration),
                col("hashtag")
            )
            .agg(count("*").alias("count"))
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= top_n)
            .select(
                col("window.start").alias("time_start"),
                col("window.end").alias("time_end"),
                col("hashtag"),
                col("count"),
                col("rank"),
            )
            # Fix: xóa .orderBy() — không hợp lệ trong Structured Streaming
        )
    
    @staticmethod
    def source_comparison(
        df: DataFrame, window_duration: str = "10 minutes"
    ) -> DataFrame:
        """
        Compare engagement across sources in same time window.
        
        Shows which platform has highest engagement metrics.
        
        Args:
            df: Input DataFrame
            window_duration: Window duration
            
        Returns:
            Comparison DataFrame
        """
        agg_df = EngagementAggregator.aggregate_by_source_and_time(
            df, window_duration
        )
        
        return (
            agg_df
            .select("time_start", "time_end", "source", "num_posts", "avg_likes")
        )
