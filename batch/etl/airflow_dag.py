"""
Airflow DAG: social_batch_pipeline
────────────────────────────────────
FIX M2: check_minio_has_data dùng đúng prefix raw/{source}/{year}/{month}/{day}/
FIX M3: verify_mongo_count so sánh event_date dạng string "YYYY-MM-DD"

Schedule: mỗi ngày lúc 02:00 UTC
Flow:
  check_minio_has_data → spark_etl → verify_mongo_count → log_done
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_HOME    = "/opt/spark"
PIPELINE_DIR  = "/opt/social_pipeline"
MONGO_CONN_ID = "mongo_social"
SOURCES       = ["facebook", "instagram", "reddit"]

SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
])


# ── Helper functions ──────────────────────────────────────────────────────────

def check_minio_has_data(**ctx) -> bool:
    """
    FIX M2: Tìm file theo đúng cấu trúc raw/{source}/{year}/{month}/{day}/
    Cũ: prefix = f"raw/{year}/{month}/{day}/" → không bao giờ tìm thấy vì thiếu {source}
    """
    from minio import Minio
    import os

    ds  = ctx["ds"]
    dt  = datetime.strptime(ds, "%Y-%m-%d")

    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )

    total_files = 0
    for source in SOURCES:
        prefix = f"raw/{source}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        objects = list(client.list_objects("social-raw", prefix=prefix, recursive=True))
        total_files += len(objects)
        print(f"[MinIO] {source}: {len(objects)} files at {prefix}")

    if total_files == 0:
        print(f"[ShortCircuit] No MinIO data for {ds} — skipping DAG run.")
        return False

    print(f"[ShortCircuit] Found {total_files} total files for {ds} — proceeding.")
    return True


def verify_mongo_count(**ctx) -> None:
    """
    FIX M3: Spark lưu event_date dạng string "YYYY-MM-DD" (DateType → string trong Mongo).
    Cũ: query dùng datetime object → không khớp kiểu → count = 0 → false alarm.
    """
    from pymongo import MongoClient
    import os

    ds  = ctx["ds"]
    dt  = datetime.strptime(ds, "%Y-%m-%d")
    # Ngày tiếp theo để dùng $lt (exclusive)
    ds_next = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    coll   = client["social_db"]["posts_clean"]

    # So sánh string "YYYY-MM-DD" — khớp với cách Spark MongoDB connector lưu DateType
    count = coll.count_documents({
        "event_date": {"$gte": ds, "$lt": ds_next}
    })
    client.close()

    print(f"MongoDB records for {ds}: {count}")
    if count == 0:
        raise ValueError(f"No records in MongoDB for {ds} — ETL may have failed!")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="social_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Kafka batch → MinIO → Spark ETL → MongoDB",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=1,
    tags=["social", "batch", "etl"],
) as dag:

    check_data = ShortCircuitOperator(
        task_id="check_minio_has_data",
        python_callable=check_minio_has_data,
    )

    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command=(
            f"{SPARK_HOME}/bin/spark-submit "
            f"--master local[*] "
            f"--packages {SPARK_PACKAGES} "
            f"--conf spark.executor.memory=2g "
            f"--conf spark.driver.memory=2g "
            f"{PIPELINE_DIR}/batch/etl/spark_etl.py "
            "--date {{ ds }}"
        ),
        env={
            "MINIO_ENDPOINT":   "{{ var.value.minio_endpoint }}",
            "MINIO_ACCESS_KEY": "{{ var.value.minio_access_key }}",
            "MINIO_SECRET_KEY": "{{ var.value.minio_secret_key }}",
            "MONGO_URI":        "{{ var.value.mongo_uri }}",
        },
    )

    verify = PythonOperator(
        task_id="verify_mongo_count",
        python_callable=verify_mongo_count,
    )

    notify = BashOperator(
        task_id="log_done",
        bash_command='echo "Batch pipeline done for {{ ds }} at $(date -u)"',
    )

    check_data >> spark_etl >> verify >> notify
