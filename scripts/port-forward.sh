#!/usr/bin/env bash
# Auto-reconnect port-forward script
set -euo pipefail

NS="social-pipeline"

forward_service() {
  local name="$1"
  local svc="$2"
  local ports="$3"
  while true; do
    echo "[$(date '+%H:%M:%S')] Starting port-forward: $name ($ports)"
    kubectl port-forward -n "$NS" svc/"$svc" $ports 2>&1 || true
    echo "[$(date '+%H:%M:%S')] $name disconnected, retrying in 3s..."
    sleep 3
  done
}

echo "=== Starting port-forwards ==="
echo "  Dashboard  : http://localhost:8084"
echo "  API        : http://localhost:8000"
echo "  Airflow    : http://localhost:8085"
echo "  Press Ctrl+C to stop"
echo ""

forward_service "dashboard"  "dashboard-service"          "8084:80"   &
forward_service "api"        "api-service"                "8000:8000" &
forward_service "airflow"    "airflow-webserver-service"  "8085:8080" &

wait
