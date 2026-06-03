#!/usr/bin/env python3
"""
Retraining and promotion pipeline for PhoBERT Sentiment Classifier.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
os.makedirs("tmp", exist_ok=True)
os.environ["TMPDIR"] = "tmp"
os.environ["HF_HOME"] = "ml/sentiment/artifacts/.cache"
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

from config.settings import SENTIMENT_ARTIFACTS_DIR
PROMOTION_THRESHOLD = float(os.getenv("RETRAIN_PROMOTION_THRESHOLD", "0.0"))


def _load_current_metric(artifacts_dir: str) -> float:
    meta_path = os.path.join(artifacts_dir, "training_metadata.json")
    if not os.path.exists(meta_path):
        return 0.0
    try:
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        return float((meta.get("test_metrics") or {}).get("weighted_f1", 0.0))
    except Exception:
        return 0.0


def _archive_current_model(artifacts_dir: str) -> None:
    # No-op: we do not archive older model versions to prevent disk usage issues.
    pass


def _load_new_metric(artifacts_dir: str) -> float:
    meta_path = os.path.join(artifacts_dir, "training_metadata.json")
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    return float((meta.get("test_metrics") or {}).get("weighted_f1", 0.0))


def _restore_backup(
    artifacts_dir: str,
    backup_dir: str,
    backed_up_files: list[str],
) -> None:
    model_src = os.path.join(backup_dir, "fine_tuned_phobert")
    model_dst = os.path.join(artifacts_dir, "fine_tuned_phobert")
    
    # Restore model dir
    if "fine_tuned_phobert" in backed_up_files and os.path.exists(model_src):
        if os.path.exists(model_dst):
            shutil.rmtree(model_dst)
        shutil.copytree(model_src, model_dst)
        logger.info("Restored model directory from backup")
    elif os.path.exists(model_dst):
        shutil.rmtree(model_dst)
        logger.info("Removed new unpromoted model directory")
        
    # Restore metadata
    meta_src = os.path.join(backup_dir, "training_metadata.json")
    meta_dst = os.path.join(artifacts_dir, "training_metadata.json")
    if "training_metadata.json" in backed_up_files and os.path.exists(meta_src):
        shutil.copy2(meta_src, meta_dst)
        logger.info("Restored metadata from backup")
    elif os.path.exists(meta_dst):
        os.remove(meta_dst)
        logger.info("Removed new unpromoted metadata")

    try:
        shutil.rmtree(backup_dir)
    except Exception:
        pass


def run_retrain(args: argparse.Namespace) -> bool:
    t0 = time.time()
    artifacts_dir = args.output_dir
    Path(artifacts_dir).mkdir(parents=True, exist_ok=True)

    # Clean up any existing archived model versions to save disk space
    if os.path.exists(artifacts_dir):
        for name in os.listdir(artifacts_dir):
            if name.startswith("fine_tuned_phobert_") and os.path.isdir(os.path.join(artifacts_dir, name)):
                try:
                    shutil.rmtree(os.path.join(artifacts_dir, name))
                    logger.info("Removed stale model archive directory: %s", name)
                except Exception as e:
                    logger.warning("Failed to remove stale archive directory %s: %s", name, e)

    logger.info("=== Sentiment Retraining Pipeline ===")
    logger.info("Artifacts dir: %s", artifacts_dir)

    # 1. Record current model performance
    old_f1 = _load_current_metric(artifacts_dir)
    logger.info("Current deployed model F1-score: %.4f", old_f1)

    # 2. Back up existing model & metadata before retraining
    backup_dir = os.path.join(artifacts_dir, ".backup")
    backed_up_files = []
    
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
        
    os.makedirs(backup_dir, exist_ok=True)
    
    model_dir = os.path.join(artifacts_dir, "fine_tuned_phobert")
    if os.path.exists(model_dir):
        shutil.copytree(model_dir, os.path.join(backup_dir, "fine_tuned_phobert"))
        backed_up_files.append("fine_tuned_phobert")
        
    meta_file = os.path.join(artifacts_dir, "training_metadata.json")
    if os.path.exists(meta_file):
        shutil.copy2(meta_file, os.path.join(backup_dir, "training_metadata.json"))
        backed_up_files.append("training_metadata.json")

    logger.info("Backed up existing artifacts: %s", backed_up_files)

    # 3. Run training pipeline
    from ml.sentiment.train import run as train_run
    
    train_args = argparse.Namespace(
        output_dir=artifacts_dir,
        local=args.local,
        data_dir=args.data_dir,
        epochs=args.epochs,
        batch_size=args.batch_size,
        no_cuda=args.no_cuda,
        smoke_test=args.smoke_test,
        log_level=args.log_level,
    )

    promoted = False
    new_f1 = 0.0
    try:
        train_run(train_args)

        # 4. Evaluate promotion (new F1 vs old F1)
        new_f1 = _load_new_metric(artifacts_dir)
        logger.info("New model F1: %.4f (promotion threshold: %.4f)", new_f1, old_f1 - PROMOTION_THRESHOLD)

        if new_f1 >= (old_f1 - PROMOTION_THRESHOLD):
            logger.info("✓ New model PROMOTED (%.4f >= %.4f). Keeping only the best version (not archiving old version to save space).", new_f1, old_f1 - PROMOTION_THRESHOLD)
            promoted = True
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
        else:
            logger.warning("✗ New model REJECTED (%.4f < %.4f). Restoring backup.", new_f1, old_f1 - PROMOTION_THRESHOLD)
            _restore_backup(artifacts_dir, backup_dir, backed_up_files)
            promoted = False

    except Exception as exc:
        logger.error("Exception during retraining: %s. Restoring backup.", exc)
        _restore_backup(artifacts_dir, backup_dir, backed_up_files)
        raise exc

    # 5. Write history log
    log_path = os.path.join(artifacts_dir, "retrain_history.jsonl")
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "retrained_at": datetime.now(timezone.utc).isoformat(),
                "old_weighted_f1": old_f1,
                "new_weighted_f1": new_f1,
                "promoted": promoted,
                "duration_sec": round(time.time() - t0, 1),
            }, ensure_ascii=False) + "\n")
        logger.info("Retrain log appended → %s", log_path)
    except Exception as e:
        logger.error("Failed to write to retrain history: %s", e)

    return promoted


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Retrain the PhoBERT Sentiment Classifier model.")
    p.add_argument("--output-dir", default=SENTIMENT_ARTIFACTS_DIR)
    p.add_argument("--local", action="store_true")
    p.add_argument("--data-dir", default="data/facebook_data/raw_data")
    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--batch-size", type=int, default=8)
    p.add_argument("--no-cuda", action="store_true")
    p.add_argument("--smoke-test", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    )
    promoted = run_retrain(args)
    exit(0 if promoted else 1)
