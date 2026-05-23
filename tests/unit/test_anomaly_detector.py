import pytest
from collections import deque
from ml.anomaly_detector import engagement_value, detect_once, warmup_history, run_loop


def test_engagement_value():
    stats = {
        "stats": [
            {"post_count": 10},
            {"post_count": "20"},
            {"post_count": "invalid"},
            {}
        ]
    }
    assert engagement_value(stats) == 30.0


def test_detect_once_normal(monkeypatch):
    history = deque([10.0] * 24, maxlen=168)
    monkeypatch.setattr("ml.anomaly_detector.service.query_realtime_stats", lambda: {"stats": [{"post_count": 10.0}]})
    
    alerts_written = []
    monkeypatch.setattr("ml.anomaly_detector.write_alert", lambda session, alert: alerts_written.append(alert))
    
    detect_once(history, None)
    
    assert len(history) == 25
    assert history[-1] == 10.0
    assert len(alerts_written) == 0


def test_detect_once_anomaly(monkeypatch):
    # Mean is 10.0, std dev is 0.0 -> fallback to std dev = 1.0. 30.0 is > 3 * 1.0 away from 10.0.
    history = deque([10.0] * 24, maxlen=168)
    monkeypatch.setattr("ml.anomaly_detector.service.query_realtime_stats", lambda: {"stats": [{"post_count": 30.0}]})
    
    alerts_written = []
    monkeypatch.setattr("ml.anomaly_detector.write_alert", lambda session, alert: alerts_written.append(alert))
    
    detect_once(history, None)
    
    assert len(history) == 25
    assert history[-1] == 30.0
    assert len(alerts_written) == 1
    assert alerts_written[0]["metric"] == "engagement"
    assert alerts_written[0]["value"] == 30.0


def test_warmup_history(monkeypatch):
    history = deque(maxlen=168)
    
    # Mock requests.post to return ES response
    class FakeResponse:
        def raise_for_status(self):
            pass
        def json(self):
            # We want to return 288 buckets.
            # Let's say all have doc_count = 5.
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)
            start = now - timedelta(minutes=288)
            start_ms = int(start.timestamp() * 1000)
            buckets = []
            for i in range(288):
                buckets.append({
                    "key": start_ms + i * 60 * 1000,
                    "doc_count": 5
                })
            return {"aggregations": {"minutes": {"buckets": buckets}}}
            
    monkeypatch.setattr("requests.post", lambda url, json, timeout: FakeResponse())
    
    warmup_history(history)
    
    # Each minute has count = 5. Sliding window of 120 minutes -> sum is 600.
    # We should have 168 history points, each being 600.
    assert len(history) == 168
    assert all(val == 600 for val in history)


def test_warmup_history_failure(monkeypatch):
    history = deque(maxlen=168)
    
    def fake_post(url, json, timeout):
        raise RuntimeError("ES connection error")
        
    monkeypatch.setattr("requests.post", fake_post)
    
    # Should log warning but not crash, history remains empty
    warmup_history(history)
    assert len(history) == 0


def test_run_loop_handles_exception(monkeypatch):
    # Mock warmup to do nothing
    monkeypatch.setattr("ml.anomaly_detector.warmup_history", lambda hist: None)
    
    # Mock detect_once to raise exception
    call_count = 0
    def fake_detect_once(hist, session):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("iteration error")
        else:
            # stop the loop by raising KeyboardInterrupt
            raise KeyboardInterrupt()
            
    monkeypatch.setattr("ml.anomaly_detector.detect_once", fake_detect_once)
    monkeypatch.setattr("time.sleep", lambda secs: None)
    
    with pytest.raises(KeyboardInterrupt):
        run_loop(interval_seconds=1)
        
    assert call_count == 2
