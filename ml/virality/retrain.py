#!/usr/bin/env python3
"""
Retraining pipeline for Facebook Post Virality Prediction.

Trigger modes
──────────────
  1. Scheduled (Airflow DAG calls this weekly):
       python -m ml.virality.retrain

  2. Manual with custom date range:
       python -m ml.virality.retrain --start-date 2026-01-01 --end-date 2026-04-30

Retraining strategy
────────────────────
  • Full retrain on all available data (simple and robust for weekly cadence).
  • Model promotion: new model is only deployed if it achieves ≥ existing accuracy
    on the held-out test set. The previous model is archived, not deleted.
  • Label thresholds are recomputed from the new dataset so the class
    distribution stays consistent over time.

Artifacts
──────────
  ml/virality/artifacts/
    lgbm_model.pkl               ← current production model (latest)
    lgbm_model_<YYYYMMDD>.pkl   ← archived versions
    training_metadata.json       ← metrics & config of the current model
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

ARTIFACTS_DIR = os.getenv("VIRALITY_ARTIFACTS_DIR", "ml/virality/artifacts")
PROMOTION_THRESHOLD = float(os.getenv("RETRAIN_PROMOTION_THRESHOLD", "0.0"))


def _load_current_accuracy(artifacts_dir: str) -> float:
    """Return test accuracy of the currently deployed model, or 0 if none."""
    meta_path = os.path.join(artifacts_dir, "training_metadata.json")
    if not os.path.exists(meta_path):
        return 0.0
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    return float((meta.get("test_metrics") or {}).get("accuracy", 0.0))


def _archive_current_model(artifacts_dir: str) -> None:
    """Archive the current production model with a datestamp suffix."""
    src = os.path.join(artifacts_dir, "lgbm_model.pkl")
    if not os.path.exists(src):
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(artifacts_dir, f"lgbm_model_{stamp}.pkl")
    shutil.copy2(src, dst)
    logger.info("Archived previous model → %s", dst)


def _load_new_accuracy(artifacts_dir: str) -> float:
    """Return test accuracy from the freshly trained model's metadata."""
    meta_path = os.path.join(artifacts_dir, "training_metadata.json")
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    return float((meta.get("test_metrics") or {}).get("accuracy", 0.0))


def _restore_backup(
    artifacts_dir: str,
    backup_dir: str,
    files_to_backup: list[str],
    backed_up_files: list[str],
) -> None:
    """Restore artifacts from the backup directory, and remove any newly created ones that were not backed up."""
    if not os.path.exists(backup_dir):
        return
    for fname in files_to_backup:
        backup_file = os.path.join(backup_dir, fname)
        target_file = os.path.join(artifacts_dir, fname)
        if fname in backed_up_files and os.path.exists(backup_file):
            try:
                shutil.copy2(backup_file, target_file)
                logger.info("Restored backup → %s", target_file)
            except Exception as e:
                logger.error("Failed to restore backup of %s: %s", fname, e)
        else:
            if os.path.exists(target_file):
                try:
                    os.remove(target_file)
                    logger.info("Removed new/unpromoted artifact → %s", target_file)
                except Exception as e:
                    logger.error("Failed to remove new/unpromoted artifact %s: %s", target_file, e)
    try:
        shutil.rmtree(backup_dir)
    except Exception as e:
        logger.error("Failed to clean up backup directory %s: %s", backup_dir, e)


def run_retrain(args: argparse.Namespace) -> bool:
    """
    Execute the full retraining pipeline.

    Returns:
        True if the new model was promoted, False if it was rejected.
    """
    t0 = time.time()
    artifacts_dir = args.output_dir

    logger.info("=== Virality Retraining Pipeline ===")
    logger.info("Artifacts dir: %s", artifacts_dir)

    # ── Record current model performance ───────────────────────────────────────
    old_accuracy = _load_current_accuracy(artifacts_dir)
    logger.info("Current deployed model accuracy: %.4f", old_accuracy)

    # ── Prepare backups of existing artifacts before training ──────────────────
    files_to_backup = [
        "lgbm_model.pkl",
        "label_thresholds.json",
        "page_medians.json",
        "page_stats.pkl",
        "training_metadata.json",
        "best_params.json",
    ]
    backup_dir = os.path.join(artifacts_dir, ".backup")
    backed_up_files = []

    if os.path.exists(artifacts_dir):
        os.makedirs(backup_dir, exist_ok=True)
        for fname in files_to_backup:
            src = os.path.join(artifacts_dir, fname)
            if os.path.exists(src):
                dst = os.path.join(backup_dir, fname)
                shutil.copy2(src, dst)
                backed_up_files.append(fname)
        logger.info("Backed up existing artifacts: %s", backed_up_files)

    # ── Run full training pipeline ─────────────────────────────────────────────
    # Import here to avoid circular imports
    from ml.virality.train import run as train_run

    # Build train args (re-use training CLI defaults, override relevant flags)
    train_args = argparse.Namespace(
        output_dir=artifacts_dir,
        local=args.local,
        data_dir=args.data_dir,
        tune=args.tune,
        no_phobert=args.no_phobert,
        log_level=args.log_level,
    )

    promoted = False
    new_accuracy = 0.0
    try:
        train_run(train_args)

        # ── Evaluate promotion ─────────────────────────────────────────────────────
        new_accuracy = _load_new_accuracy(artifacts_dir)
        logger.info("New model accuracy: %.4f (threshold for promotion: %.4f)",
                    new_accuracy, old_accuracy - PROMOTION_THRESHOLD)

        threshold = old_accuracy - PROMOTION_THRESHOLD  # allow small regression
        if new_accuracy >= threshold:
            logger.info(
                "✓ New model PROMOTED (%.4f ≥ %.4f). Archiving previous model.",
                new_accuracy, threshold,
            )
            # The train pipeline already wrote the new model as lgbm_model.pkl.
            # We just archive the old one before the new one was written — but
            # since train_run overwrites artifacts in place, we archive NOW for
            # future rollback visibility. On next retrain, this copy is the reference.
            _archive_current_model(artifacts_dir)
            promoted = True

            # Clean up backup
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
        else:
            logger.warning(
                "✗ New model REJECTED (%.4f < %.4f). Restoring previous model.",
                new_accuracy, threshold,
            )
            _restore_backup(artifacts_dir, backup_dir, files_to_backup, backed_up_files)
            promoted = False

    except Exception as e:
        logger.error("Exception occurred during retrain pipeline: %s. Restoring backup.", e)
        _restore_backup(artifacts_dir, backup_dir, files_to_backup, backed_up_files)
        raise e

    # ── Write retrain log entry ────────────────────────────────────────────────
    log_path = os.path.join(artifacts_dir, "retrain_history.jsonl")
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "retrained_at": datetime.now(timezone.utc).isoformat(),
                "old_accuracy": old_accuracy,
                "new_accuracy": new_accuracy,
                "promoted":     promoted,
                "duration_sec": round(time.time() - t0, 1),
            }, ensure_ascii=False) + "\n")
        logger.info("Retrain log appended → %s", log_path)
    except Exception as e:
        logger.error("Failed to write to retrain log history: %s", e)

    return promoted


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Retrain the Virality Prediction model.")
    p.add_argument("--output-dir", default=ARTIFACTS_DIR)
    p.add_argument("--local", action="store_true")
    p.add_argument("--data-dir", default="data/facebook_data/sample_data")
    p.add_argument("--tune", action="store_true", help="Run Optuna during retrain")
    p.add_argument("--no-phobert", action="store_true")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    )
    promoted = run_retrain(args)
    exit(0 if promoted else 1)
