#!/bin/bash
set -e

echo "$(date) - CNPJ Pipeline starting..."

# Start cron daemon in background (for monthly updates)
cron

# Start API server in background
echo "$(date) - Starting API server on port 8000..."
uvicorn api:app --host 0.0.0.0 --port 8000 --log-level info &

# Run initial pipeline (first load)
echo "$(date) - Running initial data load..."
python main.py 2>&1 | tee /var/log/cnpj-pipeline.log

echo "$(date) - Initial load complete. Cron scheduled for day 5 of each month at 3am."
echo "$(date) - API running on port 8000. Container staying alive..."

# Keep container running
wait
