#!/usr/bin/env python3
"""
Real-time sentiment predictor using custom fine-tuned PhoBERT.
"""

from __future__ import annotations

import logging
import os
os.makedirs("tmp", exist_ok=True)
os.environ["TMPDIR"] = "tmp"
os.environ["HF_HOME"] = "ml/sentiment/artifacts/.cache"
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import torch
import numpy as np
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)

LABEL_NAMES = ["negative", "neutral", "positive"]
from config.settings import SENTIMENT_ARTIFACTS_DIR


class SentimentPredictor:
    def __init__(self, artifacts_dir: str = SENTIMENT_ARTIFACTS_DIR) -> None:
        self._artifacts_dir = artifacts_dir
        self._model_path = os.path.join(self._artifacts_dir, "fine_tuned_phobert")
        
        if not os.path.exists(self._model_path):
            raise FileNotFoundError(
                f"Fine-tuned PhoBERT model not found at {self._model_path}. "
                "Please run training first."
            )

        self._tokenizer = AutoTokenizer.from_pretrained(self._model_path, use_fast=False)
        
        self._device = "cuda" if torch.cuda.is_available() else "mps" if (
            hasattr(torch.backends, "mps") and torch.backends.mps.is_available()
        ) else "cpu"
        
        self._model = AutoModelForSequenceClassification.from_pretrained(self._model_path)
        self._model.to(self._device)
        self._model.eval()
        
        logger.info("SentimentPredictor loaded fine-tuned model on device=%s", self._device)

    def predict(self, text: str) -> dict:
        """
        Predict sentiment for a given Vietnamese text string.
        
        Returns:
            Dict containing:
                prediction      : int  (0=negative, 1=neutral, 2=positive)
                label           : str  ("negative", "neutral", "positive")
                score           : float (signed probability: positive for POS, negative for NEG, 0.0 for NEU)
                confidence      : float (probability of predicted label)
                probabilities   : dict  {label: probability}
        """
        if not text or not text.strip():
            return {
                "prediction": 1,
                "label": "neutral",
                "score": 0.0,
                "confidence": 1.0,
                "probabilities": {"negative": 0.0, "neutral": 1.0, "positive": 0.0}
            }

        inputs = self._tokenizer(
            text,
            max_length=256,
            truncation=True,
            padding=True,
            return_tensors="pt"
        )
        inputs = {k: v.to(self._device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self._model(**inputs)
            
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1)[0].cpu().numpy()
        pred = int(np.argmax(probs))
        confidence = float(probs[pred])
        
        # Calculate signed score: POS is positive, NEG is negative, NEU is 0
        if pred == 2:      # POS
            score = confidence
        elif pred == 0:    # NEG
            score = -confidence
        else:              # NEU
            score = 0.0

        return {
            "prediction": pred,
            "label": LABEL_NAMES[pred],
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "probabilities": {
                name: round(float(p), 4)
                for name, p in zip(LABEL_NAMES, probs)
            }
        }
