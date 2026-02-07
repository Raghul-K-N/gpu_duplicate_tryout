# =============================================================================
# Dockerfile for FastAPI Duplicate Invoice Detection Service
# =============================================================================
# Build context should be the parent 'duplicate_invoices' directory:
#   docker build -t duplicate-invoice-api -f api/Dockerfile .
# =============================================================================

FROM python:3.11-slim

# Labels
LABEL maintainer="Duplicate Invoice Detection Team"
LABEL description="FastAPI service for GPU-accelerated duplicate invoice detection"
LABEL version="1.0"

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV HOST=0.0.0.0
ENV PORT=8000
ENV WORKERS=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY api/requirements.txt /app/api/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/api/requirements.txt

# Copy the entire duplicate_invoices module
COPY . /app/duplicate_invoices/

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the FastAPI application
CMD ["uvicorn", "duplicate_invoices.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
