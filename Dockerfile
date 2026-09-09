# Production Dockerfile for Cymbal Retail Operations Agent
FROM python:3.11-slim as base

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace

WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy application source code
COPY app/ ./app/
COPY agents-cli-manifest.yaml .
COPY AGENTS.md .

# Create non-root user for security compliance
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /workspace
USER appuser

EXPOSE 8080

# Run coordinator agent FastAPI server
CMD ["uvicorn", "app.fast_api_app:app", "--host", "0.0.0.0", "--port", "8080"]
