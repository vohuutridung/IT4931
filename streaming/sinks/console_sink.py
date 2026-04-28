"""
Console sink for debugging and monitoring.
"""

import logging

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_to_console(
    df: DataFrame,
    checkpoint_dir: str,
    mode: str = "append",
    truncate: bool = False,
    num_rows: int = 20,
):
    """
    Write streaming DataFrame to console.
    
    Useful for debugging and monitoring in development/testing.
    
    Args:
        df: Input DataFrame
        checkpoint_dir: Checkpoint directory
        mode: Output mode ("append", "complete", or "update")
        truncate: Whether to truncate long strings (default: False)
        num_rows: Number of rows to display (default: 20)
        
    Returns:
        StreamingQuery object
        
    Example:
        >>> query = write_to_console(df, "/tmp/checkpoints/console")
        >>> # Monitor output, then stop
        >>> query.stop()
    """
    logger.info("Setting up console sink (mode=%s, rows=%d)", mode, num_rows)
    
    return (
        df.writeStream
        .format("console")
        .outputMode(mode)
        .option("truncate", truncate)
        .option("numRows", num_rows)
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
