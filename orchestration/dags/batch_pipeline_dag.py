from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

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


CHECK_NEW_DATA_COMMAND = (
    "python -c \""
    "import os, sys, boto3; "
    "bucket=os.environ.get('S3_BUCKET','social-lake'); "
    "client=boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT','http://minio:9000'), "
    "aws_access_key_id=os.environ.get('S3_ACCESS_KEY','minioadmin'), "
    "aws_secret_access_key=os.environ.get('S3_SECRET_KEY','minioadmin'), "
    "region_name=os.environ.get('S3_REGION','us-east-1')); "
    "r=client.list_objects_v2(Bucket=bucket, Prefix='data/raw/', MaxKeys=1); "
    "sys.exit(0 if r.get('KeyCount', 0) > 0 else 1)"
    "\""
)


with DAG(
    dag_id="social_lambda_batch_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False,
    on_failure_callback=slack_failure_callback,
) as dag:
    check_new_data = BashSensor(
        task_id="check_new_data",
        bash_command=CHECK_NEW_DATA_COMMAND,
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    if SparkSubmitOperator:
        run_spark_batch = SparkSubmitOperator(
            task_id="run_spark_batch",
            application="/opt/social_pipeline/batch/spark_batch_job.py",
            conn_id=os.getenv("SPARK_CONN_ID", "spark_default"),
            env_vars={
                "PYTHONPATH": "/opt/social_pipeline",
                "SPARK_MASTER": "local[2]",
            },
        )
    else:
        run_spark_batch = BashOperator(
            task_id="run_spark_batch",
            bash_command="spark-submit /opt/social_pipeline/batch/spark_batch_job.py",
        )

    refresh_serving_layer = BashOperator(
        task_id="refresh_serving_layer",
        bash_command=(
            "export PYTHONPATH=/opt/social_pipeline SPARK_MASTER=local[2] && "
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
            "dag_run.get_task_instance('refresh_serving_layer').state in ['failed', 'upstream_failed'] "
            "else '0' }}\"; "
            "test \"$FAILED_UPSTREAM\" = \"0\" -o -z \"$SLACK_WEBHOOK_URL\" || "
            "python -c \"import os, requests; requests.post(os.environ['SLACK_WEBHOOK_URL'], json={'text': 'Batch pipeline failed'})\""
        ),
        trigger_rule="all_done",
    )

    check_new_data >> run_spark_batch >> refresh_serving_layer
    [check_new_data, run_spark_batch, refresh_serving_layer] >> send_slack_alert
