FROM python:3.12-slim

WORKDIR /app

# Install uv + cron
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY config.py database.py downloader.py processor.py main.py api.py ./
COPY entrypoint.sh .

# Install dependencies + API deps
RUN uv pip install --system -e . && \
    uv pip install --system fastapi uvicorn psycopg2-binary

# Create temp directory
RUN mkdir -p /app/temp

# Setup monthly cron (day 5 at 3am)
RUN echo "0 3 5 * * cd /app && python main.py >> /var/log/cnpj-pipeline.log 2>&1" > /etc/cron.d/cnpj-pipeline \
    && chmod 0644 /etc/cron.d/cnpj-pipeline \
    && crontab /etc/cron.d/cnpj-pipeline

RUN chmod +x entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=60s --timeout=30s --start-period=600s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["/app/entrypoint.sh"]
