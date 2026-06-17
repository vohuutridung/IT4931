import pytest
from fastapi import HTTPException

import api.main as api


def test_require_admin_token_rejects_when_token_unconfigured(monkeypatch):
    monkeypatch.setattr(api, "API_ADMIN_TOKEN", "")
    with pytest.raises(HTTPException) as exc:
        api.require_admin_token("anything")
    assert exc.value.status_code == 403


def test_require_admin_token_rejects_wrong_token(monkeypatch):
    monkeypatch.setattr(api, "API_ADMIN_TOKEN", "expected")
    with pytest.raises(HTTPException) as exc:
        api.require_admin_token("wrong")
    assert exc.value.status_code == 401


def test_resolve_train_data_dir_rejects_paths_outside_roots(monkeypatch, tmp_path):
    allowed = tmp_path / "data"
    allowed.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(api, "_TRAIN_DATA_ROOTS", (allowed.resolve(),))

    with pytest.raises(HTTPException) as exc:
        api._resolve_train_data_dir(str(outside))
    assert exc.value.status_code == 400
