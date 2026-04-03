#!/bin/bash
set -e

echo "$(date) - CNPJ Pipeline starting..."

# STAB-06: Export env vars for cron jobs
env >> /etc/environment

# Start cron daemon in background (for monthly updates)
cron

# STAB-07: Use exec so uvicorn receives signals (SIGTERM, SIGINT) directly
# Run initial pipeline first, then hand off PID 1 to uvicorn
echo "$(date) - Running initial data load..."
python main.py 2>&1 | tee /var/log/cnpj-pipeline.log || true

echo "$(date) - Initial load complete. Cron scheduled for day 5 of each month at 3am."
echo "$(date) - Starting API server on port 8000..."

exec uvicorn api:app --host 0.0.0.0 --port 8000 --log-level info
