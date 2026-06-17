from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.models import Variable

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except Exception:  # pragma: no cover
    SparkSubmitOperator = None


def slack_failure_callback(context):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    BashOperator(
        task_id="send_slack_failure_inline",
        bash_command="python -c \"import os, requests; requests.post(os.environ['SLACK_WEBHOOK_URL'], json={'text': 'Batch pipeline failed'})\"",
    ).execute(context)


RAW_DATA_MARKER_VARIABLE = "social_lambda_latest_raw_object"
SPARK_MASTER = os.getenv("AIRFLOW_SPARK_MASTER", "local[2]")


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )


def _latest_raw_marker() -> str | None:
    bucket = os.environ.get("S3_BUCKET", "social-lake")
    paginator = _s3_client().get_paginator("list_objects_v2")
    latest_key = None
    latest_modified = None
    for page in paginator.paginate(Bucket=bucket, Prefix="data/raw/"):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"]
            if latest_modified is None or modified > latest_modified:
                latest_modified = modified
                latest_key = obj["Key"]
    if latest_key is None or latest_modified is None:
        return None
    return f"{latest_modified.isoformat()}::{latest_key}"


def has_new_raw_data(**context) -> bool:
    latest = _latest_raw_marker()
    if latest is None:
        return False
    context["ti"].xcom_push(key="raw_marker", value=latest)
    previous = Variable.get(RAW_DATA_MARKER_VARIABLE, default_var="")
    return latest != previous


def mark_raw_data_processed(**context) -> None:
    marker = context["ti"].xcom_pull(task_ids="check_new_data", key="raw_marker")
    if marker:
        Variable.set(RAW_DATA_MARKER_VARIABLE, marker)


with DAG(
    dag_id="social_lambda_batch_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    on_failure_callback=slack_failure_callback,
) as dag:
    check_new_data = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=has_new_raw_data,
    )

    if SparkSubmitOperator:
        run_spark_batch = SparkSubmitOperator(
            task_id="run_spark_batch",
            application="/opt/social_pipeline/batch/spark_batch_job.py",
            conn_id=os.getenv("SPARK_CONN_ID", "spark_default"),
            driver_memory="512m",
            executor_memory="512m",
            total_executor_cores=1,
            conf={
                "spark.driver.host": os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"),
                "spark.cores.max": "1",
            },
            env_vars={
                "PYTHONPATH": "/opt/social_pipeline",
                "SPARK_MASTER": SPARK_MASTER,
            },
        )
    else:
        run_spark_batch = BashOperator(
            task_id="run_spark_batch",
            bash_command="spark-submit --conf spark.driver.host=$SPARK_LOCAL_IP --total-executor-cores 1 /opt/social_pipeline/batch/spark_batch_job.py",
        )

    clickhouse_load = BashOperator(
        task_id="clickhouse_load",
        bash_command=(
            "export PYTHONPATH=/opt/social_pipeline SPARK_MASTER='local[2]' && "
            "cd /opt/social_pipeline && "
            "python -m warehouse.clickhouse_loader"
        ),
    )

    elasticsearch_load = BashOperator(
        task_id="elasticsearch_load",
        bash_command=(
            "export PYTHONPATH=/opt/social_pipeline SPARK_MASTER='local[2]' && "
            "cd /opt/social_pipeline && "
            "python -m serving.es_indexer --ensure && "
            "spark-submit batch/index_batch_views.py"
        ),
    )

    send_slack_alert = BashOperator(
        task_id="send_slack_alert",
        bash_command=(
            "FAILED_UPSTREAM=\"{{ '1' if "
            "dag_run.get_task_instance('check_new_data').state in ['failed', 'upstream_failed'] or "
            "dag_run.get_task_instance('run_spark_batch').state in ['failed', 'upstream_failed'] or "
            "dag_run.get_task_instance('clickhouse_load').state in ['failed', 'upstream_failed'] or "
            "dag_run.get_task_instance('elasticsearch_load').state in ['failed', 'upstream_failed'] "
            "else '0' }}\"; "
            "test \"$FAILED_UPSTREAM\" = \"0\" -o -z \"$SLACK_WEBHOOK_URL\" || "
            "python -c \"import os, requests; requests.post(os.environ['SLACK_WEBHOOK_URL'], json={'text': 'Batch pipeline failed'})\""
        ),
        trigger_rule="all_done",
    )

    run_network_analysis = BashOperator(
        task_id="run_network_analysis",
        bash_command=(
            "export PYTHONPATH=/opt/social_pipeline NETWORK_ANALYSIS_SPARK_MASTER='local[2]' && "
            "cd /opt/social_pipeline && "
            "python -m batch.network_analysis"
        ),
    )

    mark_processed = PythonOperator(
        task_id="mark_raw_data_processed",
        python_callable=mark_raw_data_processed,
    )

    check_new_data >> run_spark_batch >> [clickhouse_load, elasticsearch_load] >> run_network_analysis >> mark_processed
    [check_new_data, run_spark_batch, clickhouse_load, elasticsearch_load, run_network_analysis, mark_processed] >> send_slack_alert
