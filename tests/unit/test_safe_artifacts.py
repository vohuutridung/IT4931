import pytest

from ml.virality.safe_artifacts import safe_pickle_path


def test_safe_pickle_path_rejects_outside_allowed_roots(monkeypatch, tmp_path):
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    monkeypatch.setenv("ML_ARTIFACT_ROOTS", str(allowed))

    with pytest.raises(ValueError):
        safe_pickle_path(outside / "model.pkl")


def test_safe_pickle_path_allows_configured_root(monkeypatch, tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "model.pkl"
    monkeypatch.setenv("ML_ARTIFACT_ROOTS", str(allowed))

    assert safe_pickle_path(target) == target.resolve()
