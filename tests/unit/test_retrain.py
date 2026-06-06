import os
import json
import pytest
import argparse
from ml.virality.retrain import run_retrain

def test_run_retrain_promoted(tmp_path, monkeypatch):
    # Setup artifacts directory
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    
    # Create old artifacts
    old_model = artifacts_dir / "lgbm_model.pkl"
    old_model.write_bytes(b"old model content")
    
    old_meta = artifacts_dir / "training_metadata.json"
    old_meta.write_text(json.dumps({"test_metrics": {"accuracy": 0.8}}))
    
    # Mock train_run to simulate a successful run that overwrites the model and metadata
    def mock_train_run(args):
        with open(os.path.join(args.output_dir, "lgbm_model.pkl"), "wb") as f:
            f.write(b"new model content")
        with open(os.path.join(args.output_dir, "training_metadata.json"), "w") as f:
            json.dump({"test_metrics": {"accuracy": 0.95}}, f)

    monkeypatch.setattr("ml.virality.train.run", mock_train_run)
    
    args = argparse.Namespace(
        output_dir=str(artifacts_dir),
        local=True,
        data_dir="dummy",
        tune=False,
        no_phobert=True,
        log_level="INFO"
    )
    
    promoted = run_retrain(args)
    assert promoted is True
    
    # The active model should be the new model
    assert (artifacts_dir / "lgbm_model.pkl").read_bytes() == b"new model content"
    
    # An archive file should be created (lgbm_model_*.pkl)
    archive_files = [f for f in os.listdir(artifacts_dir) if f.startswith("lgbm_model_") and f.endswith(".pkl")]
    assert len(archive_files) == 1
    assert (artifacts_dir / archive_files[0]).read_bytes() == b"new model content"
    
    # The backup dir should be cleaned up
    assert not (artifacts_dir / ".backup").exists()


def test_run_retrain_rejected(tmp_path, monkeypatch):
    # Setup artifacts directory
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    
    # Create old artifacts
    old_model = artifacts_dir / "lgbm_model.pkl"
    old_model.write_bytes(b"old model content")
    
    old_meta = artifacts_dir / "training_metadata.json"
    old_meta.write_text(json.dumps({"test_metrics": {"accuracy": 0.8}}))
    
    # Mock train_run to write a worse model
    def mock_train_run(args):
        with open(os.path.join(args.output_dir, "lgbm_model.pkl"), "wb") as f:
            f.write(b"worse model content")
        with open(os.path.join(args.output_dir, "training_metadata.json"), "w") as f:
            json.dump({"test_metrics": {"accuracy": 0.5}}, f)

    monkeypatch.setattr("ml.virality.train.run", mock_train_run)
    
    args = argparse.Namespace(
        output_dir=str(artifacts_dir),
        local=True,
        data_dir="dummy",
        tune=False,
        no_phobert=True,
        log_level="INFO"
    )
    
    promoted = run_retrain(args)
    assert promoted is False
    
    # The active model should be reverted to the old model!
    assert (artifacts_dir / "lgbm_model.pkl").read_bytes() == b"old model content"
    
    # The active metadata should also be reverted
    with open(artifacts_dir / "training_metadata.json") as f:
        meta = json.load(f)
    assert meta["test_metrics"]["accuracy"] == 0.8
    
    # No archive file should be created
    archive_files = [f for f in os.listdir(artifacts_dir) if f.startswith("lgbm_model_") and f.endswith(".pkl")]
    assert len(archive_files) == 0
    
    # The backup dir should be cleaned up
    assert not (artifacts_dir / ".backup").exists()


def test_run_retrain_exception(tmp_path, monkeypatch):
    # Setup artifacts directory
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    
    # Create old artifacts
    old_model = artifacts_dir / "lgbm_model.pkl"
    old_model.write_bytes(b"old model content")
    
    old_meta = artifacts_dir / "training_metadata.json"
    old_meta.write_text(json.dumps({"test_metrics": {"accuracy": 0.8}}))
    
    # Mock train_run to raise an exception midway
    def mock_train_run(args):
        with open(os.path.join(args.output_dir, "lgbm_model.pkl"), "wb") as f:
            f.write(b"corrupted model content")
        raise RuntimeError("Something went wrong during training")

    monkeypatch.setattr("ml.virality.train.run", mock_train_run)
    
    args = argparse.Namespace(
        output_dir=str(artifacts_dir),
        local=True,
        data_dir="dummy",
        tune=False,
        no_phobert=True,
        log_level="INFO"
    )
    
    with pytest.raises(RuntimeError, match="Something went wrong during training"):
        run_retrain(args)
        
    # The active model should be reverted to the old model!
    assert (artifacts_dir / "lgbm_model.pkl").read_bytes() == b"old model content"
    
    # The backup dir should be cleaned up
    assert not (artifacts_dir / ".backup").exists()
