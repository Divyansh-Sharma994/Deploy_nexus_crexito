# Optimized Production Dockerfile
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    PORT=8000

# Install minimal system dependencies needed for Playwright installer
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (leverage cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and its system dependencies the "official" way
# This is more reliable than manual lists
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code (excluding files in .dockerignore)
COPY . .

# Default command: Production-grade Uvicorn with workers
CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--timeout", "120"]
