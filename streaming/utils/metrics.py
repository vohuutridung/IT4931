"""
Streaming metrics and monitoring utilities.
"""

import time
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class StreamingMetrics:
    """Container for streaming job metrics."""
    
    total_records: int = 0
    total_batches: int = 0
    records_per_second: float = 0.0
    avg_batch_duration_ms: float = 0.0
    last_batch_timestamp: Optional[float] = None
    start_time: float = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = time.time()
    
    def update(self, batch_records: int, batch_duration_ms: float):
        """Update metrics with new batch information."""
        self.total_records += batch_records
        self.total_batches += 1
        self.last_batch_timestamp = time.time()
        
        elapsed = self.last_batch_timestamp - self.start_time
        self.records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        if self.total_batches > 0:
            self.avg_batch_duration_ms = (
                (self.avg_batch_duration_ms * (self.total_batches - 1) + batch_duration_ms)
                / self.total_batches
            )
    
    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)
    
    def __repr__(self):
        return (
            f"StreamingMetrics("
            f"total_records={self.total_records}, "
            f"batches={self.total_batches}, "
            f"rps={self.records_per_second:.2f}, "
            f"avg_batch_ms={self.avg_batch_duration_ms:.2f})"
        )


class MetricsCollector:
    """Collect and report streaming metrics."""
    
    def __init__(self):
        self.metrics = StreamingMetrics()
    
    def record_batch(self, num_records: int, duration_ms: float):
        """Record a completed batch."""
        self.metrics.update(num_records, duration_ms)
    
    def get_metrics(self) -> StreamingMetrics:
        """Get current metrics."""
        return self.metrics
    
    def print_summary(self):
        """Print metrics summary."""
        m = self.metrics
        print(
            f"\n{'='*60}\n"
            f"STREAMING METRICS SUMMARY\n"
            f"{'='*60}\n"
            f"Total Records:        {m.total_records:,}\n"
            f"Total Batches:        {m.total_batches:,}\n"
            f"Records/Second:       {m.records_per_second:.2f}\n"
            f"Avg Batch Duration:   {m.avg_batch_duration_ms:.2f} ms\n"
            f"Uptime:               {(time.time() - m.start_time):.0f} seconds\n"
            f"{'='*60}\n"
        )
