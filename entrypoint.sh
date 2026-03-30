#!/bin/bash
set -e

echo "$(date) - CNPJ Pipeline starting..."

# Start cron daemon in background (for monthly updates)
cron

# Run initial pipeline (first load)
echo "$(date) - Running initial data load..."
python main.py 2>&1 | tee /var/log/cnpj-pipeline.log

echo "$(date) - Initial load complete. Cron scheduled for day 5 of each month at 3am."
echo "$(date) - Container staying alive for cron jobs..."

# Keep container running for cron
tail -f /var/log/cnpj-pipeline.log
