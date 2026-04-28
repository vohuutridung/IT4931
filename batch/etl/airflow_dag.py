"""
Airflow DAG: social_batch_pipeline
────────────────────────────────────

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

    from minio import Minio
    from airflow.models import Variable

    ds  = str(ctx.get("yesterday_ds", ctx["ds"]))
    dt  = datetime.strptime(ds, "%Y-%m-%d")

    client = Minio(
        Variable.get("minio_endpoint", "minio:9000"),
        access_key=Variable.get("minio_access_key", "minioadmin"),
        secret_key=Variable.get("minio_secret_key", "minioadmin"),
        secure=False,
    )

    total_files = 0
    for source in SOURCES:
        prefix = f"raw/{source}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        count = sum(1 for _ in client.list_objects("social-raw", prefix=prefix, recursive=True))
        total_files += count
        print(f"[MinIO] {source}: {count} files at {prefix}")

    if total_files == 0:
        print(f"[ShortCircuit] No MinIO data for {ds} — skipping DAG run.")
        return False

    print(f"[ShortCircuit] Found {total_files} total files for {ds} — proceeding.")
    return True


def verify_mongo_count(**ctx) -> None:

    from pymongo import MongoClient
    from airflow.models import Variable

    ds  = str(ctx.get("yesterday_ds", ctx["ds"]))
    dt  = datetime.strptime(ds, "%Y-%m-%d")
    dt_next = dt + timedelta(days=1)

    client = MongoClient(Variable.get("mongo_uri", "mongodb://mongodb:27017"))
    coll   = client["social_db"]["posts_clean"]

    count = coll.count_documents({
        "event_date": {"$gte": dt, "$lt": dt_next}
    })
    client.close()

    print(f"MongoDB records for {ds}: {count}")
    if count == 0:
        print(f"WARNING: No records in MongoDB for {ds} — possible delay or no data")
    elif count < 10:
        print(f"WARNING: Low record count ({count}) for {ds} — verify ETL")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="social_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Kafka batch → MinIO → Spark ETL → MongoDB",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
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
            f"--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES} "
            f"--conf spark.executor.memory=2g "
            f"--conf spark.driver.memory=1g "
            f"--conf spark.cores.max=2 "
            f"{PIPELINE_DIR}/batch/etl/spark_etl.py "
            "--date {{ yesterday_ds }}"
        ),
        env={
            "MINIO_ENDPOINT":   "{{ var.value.minio_endpoint }}",
            "MINIO_ACCESS_KEY": "{{ var.value.minio_access_key }}",
            "MINIO_SECRET_KEY": "{{ var.value.minio_secret_key }}",
            "MONGO_URI":        "{{ var.value.mongo_uri }}",
            "S3A_ENDPOINT":     "http://{{ var.value.minio_endpoint }}",
            "PYTHONPATH":       PIPELINE_DIR,
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


# ── DAG Khởi tạo lịch sử (Full Load) ──────────────────────────────────────────

with DAG(
    dag_id="social_batch_init_pipeline",
    default_args=DEFAULT_ARGS,
    description="Khởi tạo toàn bộ dữ liệu lịch sử (Chạy thủ công 1 lần)",
    schedule_interval=None,  # Chỉ chạy thủ công
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["social", "batch", "init", "full-load"],
) as init_dag:

    # Bỏ tham số --date để spark_etl quét toàn bộ thư mục raw/
    spark_etl_full_load = BashOperator(
        task_id="spark_etl_full_load",
        bash_command=(
            f"{SPARK_HOME}/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--packages {SPARK_PACKAGES} "
            f"--conf spark.executor.memory=2g "
            f"--conf spark.driver.memory=1g "
            f"--conf spark.cores.max=2 "
            f"{PIPELINE_DIR}/batch/etl/spark_etl.py "
        ),
        env={
            "MINIO_ENDPOINT":   "{{ var.value.minio_endpoint }}",
            "MINIO_ACCESS_KEY": "{{ var.value.minio_access_key }}",
            "MINIO_SECRET_KEY": "{{ var.value.minio_secret_key }}",
            "MONGO_URI":        "{{ var.value.mongo_uri }}",
            "S3A_ENDPOINT":     "http://{{ var.value.minio_endpoint }}",
            "PYTHONPATH":       PIPELINE_DIR,
        },
    )

    log_init_done = BashOperator(
        task_id="log_init_done",
        bash_command='echo "Historical Full Load completed at $(date -u)"',
    )

    spark_etl_full_load >> log_init_done
