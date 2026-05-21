import os

import pytest
import requests


@pytest.mark.skipif(os.getenv("RUN_E2E") != "1", reason="requires full pipeline services and seeded Kafka data")
def test_realtime_stats_contract():
    api_url = os.getenv("API_URL", "http://api:8000")
    response = requests.get(f"{api_url}/api/v1/stats/realtime", timeout=5)
    response.raise_for_status()
    payload = response.json()
    assert "stats" in payload
