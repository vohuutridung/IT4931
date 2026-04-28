"""
Kafka sink implementation for streaming output.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, col

logger = logging.getLogger(__name__)


def write_to_kafka(
    df: DataFrame,
    bootstrap_servers: str,
    topic: str,
    checkpoint_dir: str,
    key_column: Optional[str] = None,
    value_serializer: str = "json",
):
    """
    Write streaming DataFrame to Kafka.
    
    Args:
        df: Input DataFrame
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Target Kafka topic
        checkpoint_dir: Checkpoint directory for recovery
        key_column: Optional column to use as message key
        value_serializer: Serialization format. Only "json" is supported.
        
    Returns:
        StreamingQuery object
        
    Example:
        >>> query = write_to_kafka(
        ...     df,
        ...     "localhost:9092",
        ...     "social-processed",
        ...     "/tmp/checkpoints/kafka"
        ... )
    """
    logger.info("Configuring Kafka sink: topic=%s", topic)
    
    if value_serializer != "json":
        raise ValueError("Only JSON serialization is supported")

    if key_column and key_column in df.columns:
        df_output = df.select(
            col(key_column).cast("string").alias("key"),
            to_json(struct("*")).alias("value"),
        )
    else:
        df_output = df.select(to_json(struct("*")).alias("value"))
    
    return (
        df_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
