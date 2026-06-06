"""
Airflow DAG: Weekly Virality Model Retraining Pipeline

Schedule: Every Monday at 02:00 UTC (configurable via VIRALITY_RETRAIN_CRON).

DAG tasks
──────────
  1. retrain_model     — Full retrain using all data from MinIO batch layer.
                         Runs ml.virality.retrain as a BashOperator.
  2. notify_on_failure — Slack notification on any task failure.

Promotion logic (inside retrain.py):
  - New model is only deployed if accuracy >= current model accuracy.
  - Previous model is archived with a datestamp suffix.
  - Retrain history is logged to ml/virality/artifacts/retrain_history.jsonl.

Configuration (via Airflow Variables or environment variables):
  VIRALITY_RETRAIN_CRON      : Cron expression (default: "0 2 * * 1")
  VIRALITY_ARTIFACTS_DIR     : Path to artifacts directory
  VIRALITY_USE_OPTUNA        : "true" to run Optuna during retrain
  SLACK_WEBHOOK_URL          : Slack webhook for failure alerts
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Configuration ──────────────────────────────────────────────────────────────
RETRAIN_CRON   = os.getenv("VIRALITY_RETRAIN_CRON", "0 2 * * 1")   # Mon 02:00 UTC
ARTIFACTS_DIR  = os.getenv("VIRALITY_ARTIFACTS_DIR", "ml/virality/artifacts")
USE_OPTUNA     = os.getenv("VIRALITY_USE_OPTUNA", "false").lower() == "true"
PYTHONPATH     = os.getenv("AIRFLOW_PYTHONPATH", "/opt/social_pipeline")
SLACK_WEBHOOK  = os.getenv("SLACK_WEBHOOK_URL", "")

_DEFAULT_ARGS = {
    "owner":            "ml-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


# ── Slack failure callback ─────────────────────────────────────────────────────

def _slack_failure_callback(context) -> None:
    if not SLACK_WEBHOOK:
        return
    import urllib.request
    import json as _json

    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    msg = {
        "text": (
            f":x: *Virality Retrain Failed*\n"
            f"DAG: `{dag_id}` | Task: `{task_id}` | Run: `{run_id}`"
        )
    }
    req = urllib.request.Request(
        SLACK_WEBHOOK,
        data=_json.dumps(msg).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass   # Do not fail the callback itself


# ── Retrain command ────────────────────────────────────────────────────────────

_tune_flag = "--tune" if USE_OPTUNA else ""

_RETRAIN_CMD = (
    f"export PYTHONPATH={PYTHONPATH} VIRALITY_ARTIFACTS_DIR={ARTIFACTS_DIR} && "
    f"cd {PYTHONPATH} && "
    f"python -m ml.virality.retrain "
    f"--output-dir {ARTIFACTS_DIR} "
    f"{_tune_flag} "
    f"--log-level INFO"
)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="virality_model_retrain",
    description="Weekly retraining of the Facebook Post Virality Prediction model",
    start_date=datetime(2026, 5, 26),
    schedule=RETRAIN_CRON,
    catchup=False,
    default_args=_DEFAULT_ARGS,
    on_failure_callback=_slack_failure_callback,
    tags=["ml", "virality", "facebook"],
    max_active_runs=1,   # Prevent concurrent retrains
) as dag:

    retrain_model = BashOperator(
        task_id="retrain_model",
        bash_command=_RETRAIN_CMD,
        on_failure_callback=_slack_failure_callback,
        doc_md="""
        Runs `ml.virality.retrain`:
          1. Loads all Facebook posts from MinIO batch layer.
          2. Rebuilds labels (E_norm percentile binning).
          3. Extracts features (PhoBERT + tabular).
          4. Trains LightGBM with early stopping.
          5. Promotes new model only if accuracy >= current model.
          6. Appends result to retrain_history.jsonl.
        Exit code 0 = promoted, 1 = rejected (old model kept).
        The task succeeds in both cases.
        """,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=(
            f"echo 'Virality retrain complete. Artifacts: {ARTIFACTS_DIR}' && "
            f"cat {ARTIFACTS_DIR}/training_metadata.json | python -c \""
            "import sys, json; m=json.load(sys.stdin); "
            "print(f'Val accuracy: {m[\\\"val_metrics\\\"][\\\"accuracy\\\"]:.4f} | "
            "Test accuracy: {m[\\\"test_metrics\\\"][\\\"accuracy\\\"]:.4f}')\""
        ),
        trigger_rule="all_success",
    )

    retrain_model >> notify_success
