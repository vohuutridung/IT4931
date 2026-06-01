#!/usr/bin/env python3
"""
Temporal feature extraction for Facebook Post Virality Prediction.

All features are derived from the `created_at` (UTC timestamp) field.
Cyclical encoding (sin/cos) is used for hour, weekday, and month to
avoid discontinuity at boundaries (e.g. hour 23 → 0).

Features produced:
    hour_sin, hour_cos          : Hour of day (0–23), cyclically encoded.
    dow_sin, dow_cos            : Day of week (0=Mon … 6=Sun), cyclically encoded.
    month_sin, month_cos        : Month (1–12), cyclically encoded.
    is_weekend                  : 1 if Saturday or Sunday.
    is_prime_time               : 1 if hour in [18, 22) — Vietnamese social media peak.
    is_morning                  : 1 if hour in [6, 9).
    is_late_night               : 1 if hour in [22, 24) or [0, 5).
    quarter                     : 1–4 quarter of year.
    days_since_epoch            : Days since 2026-01-01 (trend proxy).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Epoch anchor for the "days_since_epoch" trend feature
_EPOCH_ANCHOR = pd.Timestamp("2026-01-01", tz="UTC")


def _cyclical(value: pd.Series, period: float) -> tuple[pd.Series, pd.Series]:
    """Encode a periodic value as (sin, cos) pair."""
    angle = 2 * np.pi * value / period
    return np.sin(angle), np.cos(angle)


def extract_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract all temporal features from the `created_at` column.

    Args:
        df: DataFrame containing a `created_at` column (UTC-aware datetime).

    Returns:
        DataFrame of numeric temporal features, one row per post.
    """
    ts = pd.to_datetime(df["created_at"], utc=True)

    hour  = ts.dt.hour
    dow   = ts.dt.dayofweek     # 0 = Monday, 6 = Sunday
    month = ts.dt.month
    day   = ts.dt.day

    hour_sin,  hour_cos  = _cyclical(hour,  24.0)
    dow_sin,   dow_cos   = _cyclical(dow,   7.0)
    month_sin, month_cos = _cyclical(month - 1, 12.0)   # shift month to 0-based

    is_weekend    = (dow >= 5).astype(int)
    is_prime_time = ((hour >= 18) & (hour < 22)).astype(int)
    is_morning    = ((hour >= 6)  & (hour < 9)).astype(int)
    is_late_night = ((hour >= 22) | (hour < 5)).astype(int)

    quarter = ts.dt.quarter

    days_since_epoch = (ts - _EPOCH_ANCHOR).dt.total_seconds() / 86400.0

    features = pd.DataFrame(
        {
            "hour_sin":          hour_sin.values,
            "hour_cos":          hour_cos.values,
            "dow_sin":           dow_sin.values,
            "dow_cos":           dow_cos.values,
            "month_sin":         month_sin.values,
            "month_cos":         month_cos.values,
            "is_weekend":        is_weekend.values,
            "is_prime_time":     is_prime_time.values,
            "is_morning":        is_morning.values,
            "is_late_night":     is_late_night.values,
            "quarter":           quarter.values,
            "days_since_epoch":  days_since_epoch.values,
        },
        index=df.index,
    )
    return features


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample = pd.DataFrame({
        "created_at": [
            "2026-02-11T21:00:33+07:00",   # prime time, Wednesday
            "2026-02-14T08:30:00+07:00",   # morning, Saturday
            "2026-03-01T00:15:00+07:00",   # late night, Sunday
        ]
    })
    print(extract_temporal_features(sample).to_string())
