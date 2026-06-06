#!/usr/bin/env python3
"""
Training pipeline for fine-tuning PhoBERT on Vietnamese sentiment data.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
# Sanitize SSL environment variables if they point to non-existent files/directories
for var in ["SSL_CERT_FILE", "SSL_CERT_DIR"]:
    if var in os.environ and not os.path.exists(os.environ[var]):
        del os.environ[var]

os.makedirs("tmp", exist_ok=True)
os.environ["TMPDIR"] = "tmp"
os.environ["HF_HOME"] = "ml/sentiment/artifacts/.cache"
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import accuracy_score, f1_score
from transformers import AutoModelForSequenceClassification, AutoTokenizer, Trainer, TrainingArguments

logger = logging.getLogger(__name__)

from config.settings import SENTIMENT_ARTIFACTS_DIR
from ml.sentiment.data.loader import load
from speed.nlp_pipeline import _lexicon_sentiment

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"


class SentimentDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item["labels"] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item

    def __len__(self):
        return len(self.labels)


def compute_metrics(eval_pred) -> dict:
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    acc = accuracy_score(labels, predictions)
    f1 = f1_score(labels, predictions, average="weighted")
    return {"accuracy": acc, "weighted_f1": f1}


def pseudo_label(text: str) -> Optional[int]:
    """
    Pseudo-labeling using weak supervision from lexicon:
      - 2: Positive (lexicon score >= 0.20)
      - 0: Negative (lexicon score <= -0.20)
      - 1: Neutral (lexicon score in [-0.05, 0.05])
      - None: Skip intermediate scores
    """
    score = _lexicon_sentiment(text)
    if score >= 0.20:
        return 2  # POS
    elif score <= -0.20:
        return 0  # NEG
    elif -0.05 <= score <= 0.05:
        return 1  # NEU
    return None


def temporal_split(
    df: pd.DataFrame,
    train_frac: float = 0.90,
    val_frac: float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    n_train = int(n * train_frac)
    n_val = int(n * val_frac)

    train = df.iloc[:n_train].copy()
    val = df.iloc[n_train: n_train + n_val].copy()
    test = df.iloc[n_train + n_val:].copy()

    logger.info(
        "Temporal split: train=%d (%.0f%%) | val=%d (%.0f%%) | test=%d (%.0f%%)",
        len(train), 100 * len(train) / n,
        len(val), 100 * len(val) / n,
        len(test), 100 * len(test) / n,
    )
    return train, val, test


def run(args: argparse.Namespace) -> None:
    t0 = time.time()
    output_dir = args.output_dir
    model_save_path = os.path.join(output_dir, "fine_tuned_phobert")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # ── Step 1: Load data ──────────────────────────────────────────────────────
    logger.info("=== Step 1: Loading raw texts ===")
    df = load(
        use_local=args.local,
        local_data_dir=args.data_dir if args.local else None,
    )

    # ── Step 2: Pseudo-label data ──────────────────────────────────────────────
    logger.info("=== Step 2: Pseudo-labeling via lexicon ===")
    if args.smoke_test:
        logger.info("Running in smoke-test mode — limiting data size to 200 rows before pseudo-labeling.")
        df = df.head(200).copy()

    df["label"] = df["text"].apply(pseudo_label)
    df = df.dropna(subset=["label"]).copy()
    df["label"] = df["label"].astype(int)

    logger.info("Label distribution after weak supervision filters:")
    dist = df["label"].value_counts().sort_index()
    for label_idx, count in dist.items():
        label_name = "NEG" if label_idx == 0 else "NEU" if label_idx == 1 else "POS"
        logger.info("  Class %d (%s): %d", label_idx, label_name, count)

    if len(df) == 0:
        raise ValueError("No training samples found after applying pseudo-label thresholds.")

    # ── Step 3: Split chronologically ──────────────────────────────────────────
    logger.info("=== Step 3: Temporal splitting ===")
    train_df, val_df, test_df = temporal_split(df)

    if args.smoke_test:
        logger.info("Limiting split datasets for smoke-test training loop.")
        train_df = train_df.head(10)
        val_df = val_df.head(5)
        test_df = test_df.head(5)

    train_texts, train_labels = train_df["text"].tolist(), train_df["label"].tolist()
    val_texts, val_labels = val_df["text"].tolist(), val_df["label"].tolist()
    test_texts, test_labels = test_df["text"].tolist(), test_df["label"].tolist()

    # ── Step 4: Tokenization ───────────────────────────────────────────────────
    logger.info("=== Step 4: Tokenizing text ===")
    tokenizer = AutoTokenizer.from_pretrained("vinai/phobert-base", use_fast=False)
    
    train_encodings = tokenizer(train_texts, truncation=True, padding=True, max_length=256)
    val_encodings = tokenizer(val_texts, truncation=True, padding=True, max_length=256)
    test_encodings = tokenizer(test_texts, truncation=True, padding=True, max_length=256)

    train_dataset = SentimentDataset(train_encodings, train_labels)
    val_dataset = SentimentDataset(val_encodings, val_labels)
    test_dataset = SentimentDataset(test_encodings, test_labels)

    # ── Step 5: Initialize Model ───────────────────────────────────────────────
    logger.info("=== Step 5: Initializing vinai/phobert-base classification model ===")
    device = "cuda" if torch.cuda.is_available() and not args.no_cuda else "mps" if (
        hasattr(torch.backends, "mps") and torch.backends.mps.is_available() and not args.no_cuda
    ) else "cpu"
    logger.info("Training device: %s", device)

    model = AutoModelForSequenceClassification.from_pretrained("vinai/phobert-base", num_labels=3)
    model.to(device)

    # ── Step 6: Define Training Args ───────────────────────────────────────────
    logger.info("=== Step 6: Training configurations ===")
    epochs = 1 if args.smoke_test else args.epochs
    batch_size = 2 if args.smoke_test else args.batch_size
    
    training_args = TrainingArguments(
        output_dir=os.path.join(output_dir, "results"),
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        warmup_steps=10 if args.smoke_test else 100,
        weight_decay=0.01,
        logging_dir=os.path.join(output_dir, "logs"),
        logging_steps=1 if args.smoke_test else 10,
        eval_strategy="epoch",
        save_strategy="epoch",
        save_total_limit=1,
        load_best_model_at_end=True,
        metric_for_best_model="weighted_f1",
        greater_is_better=True,
        report_to="none",
        use_cpu=(device == "cpu"),
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        compute_metrics=compute_metrics,
    )

    # ── Step 7: Train Model ────────────────────────────────────────────────────
    logger.info("=== Step 7: Fine-tuning the PhoBERT classifier ===")
    trainer.train()

    # ── Step 8: Evaluate on Test Set ───────────────────────────────────────────
    logger.info("=== Step 8: Evaluation on holdout test set ===")
    eval_result = trainer.evaluate(eval_dataset=test_dataset)
    logger.info("Test results: %s", eval_result)

    test_accuracy = eval_result.get("eval_accuracy", 0.0)
    test_f1 = eval_result.get("eval_weighted_f1", 0.0)

    # ── Step 9: Save Fine-tuned Model Checkpoint ───────────────────────────────
    logger.info("=== Step 9: Saving model artifacts ===")
    trainer.save_model(model_save_path)
    tokenizer.save_pretrained(model_save_path)

    # Save training metadata
    meta = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "base_model": "vinai/phobert-base",
        "n_train": len(train_df),
        "n_val": len(val_df),
        "n_test": len(test_df),
        "test_metrics": {
            "accuracy": round(test_accuracy, 4),
            "weighted_f1": round(test_f1, 4),
        },
        "training_duration_sec": round(time.time() - t0, 1),
        "smoke_test": args.smoke_test,
    }
    
    meta_path = os.path.join(output_dir, "training_metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    logger.info("Saved training metadata → %s", meta_path)

    # Clean up intermediate checkpoints to save disk space
    import shutil
    results_dir = os.path.join(output_dir, "results")
    if os.path.exists(results_dir):
        shutil.rmtree(results_dir, ignore_errors=True)
        logger.info("Cleaned up intermediate training checkpoints from %s", results_dir)

    logger.info("✓ Fine-tuning completed in %.1f seconds.", time.time() - t0)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fine-tune PhoBERT model for sentiment analysis.")
    p.add_argument("--output-dir", default=SENTIMENT_ARTIFACTS_DIR)
    p.add_argument("--local", action="store_true", help="Load data from local files instead of MinIO")
    p.add_argument("--data-dir", default="data/facebook_data/raw_data")
    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--batch-size", type=int, default=8)
    p.add_argument("--no-cuda", action="store_true", help="Force CPU training")
    p.add_argument("--smoke-test", action="store_true", help="Run a fast 1-epoch test on tiny data subset")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    )
    run(args)
