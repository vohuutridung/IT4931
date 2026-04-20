"""
Output sinks for streaming data.

Write DataFrames to various destinations: console, Parquet, Kafka, etc.
"""

import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ParquetSink:
    """Write DataFrame to Parquet format."""
    
    @staticmethod
    def write(
        df: DataFrame,
        path: str,
        checkpoint_dir: str,
        partition_cols: Optional[list] = None,
        mode: str = "append",
    ):
        """
        Write streaming DataFrame to Parquet.
        
        Args:
            df: Input DataFrame
            path: Output path for Parquet files
            checkpoint_dir: Checkpoint directory for streaming query
            partition_cols: Columns to partition by (e.g., ["source", "time_start"])
            mode: Output mode (default: append)
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Setting up Parquet sink to {path}")
        
        query = df.writeStream.format("parquet").outputMode(mode)
        
        if partition_cols:
            query = query.partitionBy(*partition_cols)
        
        return (
            query
            .option("path", path)
            .option("checkpointLocation", str(checkpoint_dir))
            .start()
        )


class ConsoleSink:
    """Write DataFrame to console for debugging."""
    
    @staticmethod
    def write(
        df: DataFrame,
        checkpoint_dir: str,
        truncate: bool = False,
        num_rows: int = 20,
    ):
        """
        Write streaming DataFrame to console.
        
        Args:
            df: Input DataFrame
            checkpoint_dir: Checkpoint directory
            truncate: Whether to truncate long strings
            num_rows: Number of rows to display
            
        Returns:
            StreamingQuery object
        """
        logger.info("Setting up console sink for debugging")
        
        return (
            df.writeStream
            .format("console")
            .option("truncate", truncate)
            .option("numRows", num_rows)
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_dir))
            .start()
        )


class KafkaSink:
    """Write DataFrame back to Kafka."""
    
    @staticmethod
    def write(
        df: DataFrame,
        bootstrap_servers: str,
        topic: str,
        checkpoint_dir: str,
        key_col: Optional[str] = None,
    ):
        """
        Write streaming DataFrame to Kafka topic.
        
        Note: DataFrame is automatically converted to JSON.
        
        Args:
            df: Input DataFrame
            bootstrap_servers: Kafka bootstrap servers
            topic: Target Kafka topic
            checkpoint_dir: Checkpoint directory
            key_col: Optional column to use as message key
            
        Returns:
            StreamingQuery object
        """
        from pyspark.sql.functions import to_json, struct, col
        
        logger.info(f"Setting up Kafka sink to topic {topic}")
        
        # Convert to JSON
        df_json = df.select(to_json(struct("*")).alias("value"))
        
        if key_col and key_col in df.columns:
            df_json = df_json.select(col(key_col).alias("key"), col("value"))
        
        return (
            df_json.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", topic)
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_dir))
            .start()
        )


class MultiSink:
    """Helper to write to multiple sinks."""
    
    def __init__(self):
        self.queries = []
    
    def add_parquet(
        self,
        df: DataFrame,
        path: str,
        checkpoint_dir: str,
        partition_cols: Optional[list] = None,
    ):
        """Add Parquet sink."""
        query = ParquetSink.write(df, path, checkpoint_dir, partition_cols)
        self.queries.append(query)
        return self
    
    def add_console(
        self,
        df: DataFrame,
        checkpoint_dir: str,
        truncate: bool = False,
            num_rows: int = 20,
    ):
        """Add console sink."""
        query = ConsoleSink.write(df, checkpoint_dir, truncate, num_rows)
        self.queries.append(query)
        return self
    
    def add_kafka(
        self,
        df: DataFrame,
        bootstrap_servers: str,
        topic: str,
        checkpoint_dir: str,
        key_col: Optional[str] = None,
    ):
        """Add Kafka sink."""
        query = KafkaSink.write(df, bootstrap_servers, topic, checkpoint_dir, key_col)
        self.queries.append(query)
        return self
    
    def get_queries(self):
        """Get all StreamingQuery objects."""
        return self.queries
    
    def stop_all(self):
        """Stop all queries."""
        for query in self.queries:
            if query:
                query.stop()
        logger.info(f"Stopped {len(self.queries)} queries")
