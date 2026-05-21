"""Spark configuration helpers for MinIO/S3A storage."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import (
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS,
    S3_SECRET_KEY,
)


def configure_s3a(builder: SparkSession.Builder) -> SparkSession.Builder:
    return (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", S3_PATH_STYLE_ACCESS)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
