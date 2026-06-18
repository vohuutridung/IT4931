import os

import pytest
import requests


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="requires running pipeline services")
def test_api_health_endpoint():
    api_url = os.getenv("API_URL", "http://localhost:8000")
    if not api_url.endswith("/health"):
        api_url = api_url.rstrip("/") + "/health"
    response = requests.get(api_url, timeout=5)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
