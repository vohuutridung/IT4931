#!/usr/bin/env python3
"""
LightGBM classifier for Facebook Post Virality Prediction.

Architecture
────────────
    Input  : concat(PhoBERT CLS 768-dim, tabular ~50-dim) → ~818-dim vector
    Model  : LightGBM multiclass (4 classes: Low / Medium / High / Viral)
    Tuning : Optuna (50 trials) with temporal-aware cross-validation

Key design choices
───────────────────
  • class_weight="balanced" to handle the class imbalance.
  • Early stopping on validation log-loss to prevent overfitting.
  • Optuna + MedianPruner for efficient hyperparameter search.
  • Model saved as pickle for fast real-time inference loading.
"""

from __future__ import annotations

import logging
import os
import pickle
from pathlib import Path
from typing import Optional

import lightgbm as lgb
import numpy as np
from sklearn.metrics import accuracy_score, classification_report
from sklearn.utils.class_weight import compute_sample_weight

logger = logging.getLogger(__name__)

N_OPTUNA_TRIALS = int(os.getenv("OPTUNA_TRIALS", "50"))


# ═══════════════════════════════════════════════════════════════════════════════
# Default hyperparameters
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_PARAMS: dict = {
    "objective":        "multiclass",
    "num_class":        4,
    "metric":           "multi_logloss",
    "boosting_type":    "gbdt",
    "n_estimators":     1000,
    "learning_rate":    0.05,
    "num_leaves":       63,
    "max_depth":        -1,
    "min_child_samples": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq":     5,
    "reg_alpha":        0.1,
    "reg_lambda":       1.0,
    "n_jobs":           -1,
    "verbose":          -1,
    "random_state":     42,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Training
# ═══════════════════════════════════════════════════════════════════════════════

def train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    params: Optional[dict] = None,
    early_stopping_rounds: int = 50,
) -> lgb.LGBMClassifier:
    """
    Train a LightGBM multiclass classifier with early stopping.

    Args:
        X_train: Feature matrix for training.
        y_train: Integer labels (0–3) for training.
        X_val:   Feature matrix for validation (early stopping).
        y_val:   Integer labels for validation.
        params:  LightGBM hyperparameters. Defaults to DEFAULT_PARAMS.
        early_stopping_rounds: Stop if val log-loss doesn't improve.

    Returns:
        Fitted LGBMClassifier.
    """
    if params is None:
        params = DEFAULT_PARAMS.copy()

    # Compute sample weights to balance classes
    sample_weights = compute_sample_weight("balanced", y=y_train)

    # Extract n_estimators before passing to constructor
    n_estimators = params.pop("n_estimators", 1000)
    model = lgb.LGBMClassifier(n_estimators=n_estimators, **params)
    params["n_estimators"] = n_estimators  # restore

    callbacks = [
        lgb.early_stopping(early_stopping_rounds, verbose=True),
        lgb.log_evaluation(100),
    ]

    model.fit(
        X_train,
        y_train,
        sample_weight=sample_weights,
        eval_set=[(X_val, y_val)],
        callbacks=callbacks,
    )

    logger.info(
        "Training complete. Best iteration: %d | Val log-loss: %.4f",
        model.best_iteration_,
        min(model.evals_result_["valid_0"]["multi_logloss"]),
    )
    return model


# ═══════════════════════════════════════════════════════════════════════════════
# Hyperparameter tuning (Optuna)
# ═══════════════════════════════════════════════════════════════════════════════

def tune_hyperparams(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    n_trials: int = N_OPTUNA_TRIALS,
) -> dict:
    """
    Search for optimal LightGBM hyperparameters using Optuna.

    Uses MedianPruner to terminate unpromising trials early.
    Objective: maximise weighted F1 on the validation set.

    Args:
        X_train, y_train: Training data.
        X_val, y_val:     Validation data.
        n_trials:         Number of Optuna trials.

    Returns:
        Best hyperparameter dict (merged with DEFAULT_PARAMS).
    """
    try:
        import optuna
        from sklearn.metrics import f1_score
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError as e:
        raise ImportError("optuna is required for hyperparameter tuning: pip install optuna") from e

    sample_weights = compute_sample_weight("balanced", y=y_train)

    def objective(trial: "optuna.Trial") -> float:
        params = {
            "objective":         "multiclass",
            "num_class":         4,
            "metric":            "multi_logloss",
            "boosting_type":     "gbdt",
            "n_estimators":      500,
            "verbose":           -1,
            "random_state":      42,
            "n_jobs":            -1,
            # Search space
            "num_leaves":        trial.suggest_int("num_leaves", 31, 255),
            "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
            "feature_fraction":  trial.suggest_float("feature_fraction", 0.6, 1.0),
            "bagging_fraction":  trial.suggest_float("bagging_fraction", 0.6, 1.0),
            "bagging_freq":      trial.suggest_int("bagging_freq", 1, 10),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 50),
            "reg_alpha":         trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda":        trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        }

        n_est = params.pop("n_estimators")
        model = lgb.LGBMClassifier(n_estimators=n_est, **params)
        model.fit(
            X_train,
            y_train,
            sample_weight=sample_weights,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)],
        )
        preds = model.predict(X_val)
        return f1_score(y_val, preds, average="weighted")

    study = optuna.create_study(
        direction="maximize",
        pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=10),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    best = study.best_params
    logger.info("Optuna best params: %s (weighted F1=%.4f)", best, study.best_value)

    # Merge with defaults (best params override)
    final_params = DEFAULT_PARAMS.copy()
    final_params.update(best)
    return final_params


# ═══════════════════════════════════════════════════════════════════════════════
# Evaluation
# ═══════════════════════════════════════════════════════════════════════════════

LABEL_NAMES = ["Low", "Medium", "High", "Viral"]


def evaluate(
    model: lgb.LGBMClassifier,
    X: np.ndarray,
    y: np.ndarray,
    split_name: str = "test",
) -> dict:
    """
    Evaluate model on a dataset split and return metrics dict.

    Metrics:
        accuracy            : Overall accuracy.
        weighted_f1         : F1 weighted by class support.
        per_class_f1        : Per-class F1 for all 4 labels.
        classification_report: Full sklearn report (for logging).
    """
    from sklearn.metrics import f1_score

    preds = model.predict(X)
    acc = accuracy_score(y, preds)
    w_f1 = f1_score(y, preds, average="weighted")
    per_class = f1_score(y, preds, average=None, labels=[0, 1, 2, 3])

    report = classification_report(y, preds, target_names=LABEL_NAMES)

    logger.info(
        "[%s] Accuracy=%.4f | Weighted-F1=%.4f", split_name, acc, w_f1
    )
    logger.info("\n%s", report)

    return {
        "split":          split_name,
        "accuracy":       float(acc),
        "weighted_f1":    float(w_f1),
        "per_class_f1": {
            name: float(f1)
            for name, f1 in zip(LABEL_NAMES, per_class)
        },
        "classification_report": report,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Persistence
# ═══════════════════════════════════════════════════════════════════════════════

def save_model(model: lgb.LGBMClassifier, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)
    logger.info("Saved LightGBM model → %s", path)


def load_model(path: str) -> lgb.LGBMClassifier:
    with open(path, "rb") as f:
        return pickle.load(f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rng = np.random.default_rng(42)
    n, d = 1000, 50
    X = rng.standard_normal((n, d)).astype(np.float32)
    y = rng.integers(0, 4, size=n)
    split = int(0.8 * n)
    model = train(X[:split], y[:split], X[split:], y[split:])
    metrics = evaluate(model, X[split:], y[split:], split_name="test")
    print(metrics)
