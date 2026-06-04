import os
import json
import shutil
import pytest
import argparse
from ml.sentiment.retrain import run_retrain

def test_sentiment_retrain_promoted(tmp_path, monkeypatch):
    # Setup artifacts directory
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    
    # Create old artifacts
    old_model_dir = artifacts_dir / "fine_tuned_phobert"
    old_model_dir.mkdir()
    (old_model_dir / "pytorch_model.bin").write_bytes(b"old model bin")
    
    old_meta = artifacts_dir / "training_metadata.json"
    old_meta.write_text(json.dumps({"test_metrics": {"weighted_f1": 0.8}}))

    # Create a pre-existing stale archive directory to verify it gets cleaned up
    stale_archive_dir = artifacts_dir / "fine_tuned_phobert_20260101_000000"
    stale_archive_dir.mkdir()
    (stale_archive_dir / "pytorch_model.bin").write_bytes(b"stale model bin")
    
    # Mock train_run to simulate a successful run that overwrites the model and metadata
    def mock_train_run(args):
        # In actual training, results directory might be created and then deleted by train.py
        results_dir = os.path.join(args.output_dir, "results")
        os.makedirs(results_dir, exist_ok=True)
        with open(os.path.join(results_dir, "checkpoint-1"), "w") as f:
            f.write("checkpoint")
            
        # Save fine-tuned model
        model_save_path = os.path.join(args.output_dir, "fine_tuned_phobert")
        shutil.rmtree(model_save_path, ignore_errors=True)
        os.makedirs(model_save_path, exist_ok=True)
        with open(os.path.join(model_save_path, "pytorch_model.bin"), "wb") as f:
            f.write(b"new model bin")
            
        with open(os.path.join(args.output_dir, "training_metadata.json"), "w") as f:
            json.dump({"test_metrics": {"weighted_f1": 0.95}}, f)
            
        # Clean up results
        shutil.rmtree(results_dir, ignore_errors=True)

    monkeypatch.setattr("ml.sentiment.train.run", mock_train_run)
    
    args = argparse.Namespace(
        output_dir=str(artifacts_dir),
        local=True,
        data_dir="dummy",
        epochs=1,
        batch_size=2,
        no_cuda=True,
        smoke_test=True,
        log_level="INFO"
    )
    
    promoted = run_retrain(args)
    assert promoted is True
    
    # The active model should be the new model
    assert (old_model_dir / "pytorch_model.bin").read_bytes() == b"new model bin"
    
    # No archive directory should be created (fine_tuned_phobert_*)
    archive_dirs = [f for f in os.listdir(artifacts_dir) if f.startswith("fine_tuned_phobert_") and os.path.isdir(artifacts_dir / f)]
    assert len(archive_dirs) == 0
    
    # Stale archive should be cleaned up
    assert not stale_archive_dir.exists()
    
    # Backup dir should be cleaned up
    assert not (artifacts_dir / ".backup").exists()
    
    # Checkpoints results dir should not exist
    assert not (artifacts_dir / "results").exists()


def test_sentiment_retrain_rejected(tmp_path, monkeypatch):
    # Setup artifacts directory
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    
    # Create old artifacts
    old_model_dir = artifacts_dir / "fine_tuned_phobert"
    old_model_dir.mkdir()
    (old_model_dir / "pytorch_model.bin").write_bytes(b"old model bin")
    
    old_meta = artifacts_dir / "training_metadata.json"
    old_meta.write_text(json.dumps({"test_metrics": {"weighted_f1": 0.8}}))
    
    # Mock train_run to write a worse model
    def mock_train_run(args):
        model_save_path = os.path.join(args.output_dir, "fine_tuned_phobert")
        shutil.rmtree(model_save_path, ignore_errors=True)
        os.makedirs(model_save_path, exist_ok=True)
        with open(os.path.join(model_save_path, "pytorch_model.bin"), "wb") as f:
            f.write(b"worse model bin")
        with open(os.path.join(args.output_dir, "training_metadata.json"), "w") as f:
            json.dump({"test_metrics": {"weighted_f1": 0.5}}, f)

    monkeypatch.setattr("ml.sentiment.train.run", mock_train_run)
    
    args = argparse.Namespace(
        output_dir=str(artifacts_dir),
        local=True,
        data_dir="dummy",
        epochs=1,
        batch_size=2,
        no_cuda=True,
        smoke_test=True,
        log_level="INFO"
    )
    
    promoted = run_retrain(args)
    assert promoted is False
    
    # The active model should be reverted to the old model!
    assert (old_model_dir / "pytorch_model.bin").read_bytes() == b"old model bin"
    
    # The active metadata should also be reverted
    with open(artifacts_dir / "training_metadata.json") as f:
        meta = json.load(f)
    assert meta["test_metrics"]["weighted_f1"] == 0.8
    
    # No archive dir should be created
    archive_dirs = [f for f in os.listdir(artifacts_dir) if f.startswith("fine_tuned_phobert_") and os.path.isdir(artifacts_dir / f)]
    assert len(archive_dirs) == 0
    
    # Backup dir should be cleaned up
    assert not (artifacts_dir / ".backup").exists()
