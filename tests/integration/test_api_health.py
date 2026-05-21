import os

import pytest
import requests


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="requires running docker compose services")
def test_api_health_endpoint():
    response = requests.get(os.getenv("API_URL", "http://localhost:8000/health"), timeout=5)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
