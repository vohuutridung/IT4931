"""
Social media post processor with advanced transformations.

Handles engagement scoring, viral detection, deduplication, and custom aggregations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, min as spark_min,
    when, regexp_extract, size, split,
    row_number, window,
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType


class SocialProcessor:
    """Apply business logic transformations to social media posts."""
    
    def __init__(self):
        """Initialize processor."""
        self.post_count = 0
    
    def apply(self, df: DataFrame) -> DataFrame:
        """
        Apply full processing pipeline.
        
        Args:
            df: Input DataFrame with social posts
            
        Returns:
            Processed DataFrame with additional columns
        """
        df = self.add_engagement_score(df)
        df = self.add_content_metrics(df)
        df = self.deduplicate(df)
        return df
    
    @staticmethod
    def add_engagement_score(df: DataFrame) -> DataFrame:
        """
        Calculate engagement score based on engagement metrics.
        
        Formula: likes*2 + comments*3 + shares*5
        
        Rationale:
        - Comments indicate deeper engagement than likes
        - Shares are the strongest signal of value
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with new 'engagement_score' column
        """
        return df.withColumn(
            "engagement_score",
            (
                col("engagement.likes").cast(IntegerType()) * 2
                + col("engagement.comments").cast(IntegerType()) * 3
                + col("engagement.shares").cast(IntegerType()) * 5
            ).cast(IntegerType())
        )
    
    @staticmethod
    def add_content_metrics(df: DataFrame) -> DataFrame:
        """
        Extract content-level metrics.
        
        Adds:
        - content_length: Characters in content
        - hashtag_count: Number of hashtags
        - url_present: Whether URL is present
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with content metrics
        """
        from pyspark.sql.functions import length, size, when, col
        
        return (
            df
            .withColumn(
                "content_length",
                length(col("content")).cast(IntegerType())
            )
            .withColumn(
                "hashtag_count",
                size(col("hashtags")).cast(IntegerType())
            )
            .withColumn(
                "url_present",
                when(col("url").isNotNull(), True).otherwise(False)
            )
        )
    
    @staticmethod
    def deduplicate(df: DataFrame, key_col: str = "post_id") -> DataFrame:
        """
        Remove duplicate posts based on key.
        
        Keeps the first occurrence (earliest ingest_time).
        
        Args:
            df: Input DataFrame
            key_col: Column to deduplicate on (default: post_id)
            
        Returns:
            Deduplicated DataFrame
        """
        window_spec = Window.partitionBy(key_col).orderBy("ingest_time")
        
        return (
            df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )
    
    @staticmethod
    def filter_by_engagement(df: DataFrame, min_score: int = 10) -> DataFrame:
        """
        Filter posts by minimum engagement score.
        
        Args:
            df: Input DataFrame
            min_score: Minimum engagement score (default: 10)
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(col("engagement_score") >= min_score)
    
    @staticmethod
    def categorize_engagement(df: DataFrame) -> DataFrame:
        """
        Categorize posts based on engagement level.
        
        Categories:
        - low: score < 20
        - medium: 20 <= score < 100
        - high: 100 <= score < 500
        - viral: score >= 500
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with 'engagement_category' column
        """
        return df.withColumn(
            "engagement_category",
            when(col("engagement_score") < 20, "low")
            .when(col("engagement_score") < 100, "medium")
            .when(col("engagement_score") < 500, "high")
            .when(col("engagement_score") >= 500, "viral")
            .otherwise("unknown")
        )
