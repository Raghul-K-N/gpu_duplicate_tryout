# Docker Setup for FastAPI Duplicate Invoice Detection

## Quick Start

### Using Docker Compose (Recommended)

```bash
cd flask_code/duplicate_invoices/api

# Build and run
docker-compose up --build

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Using Docker CLI

```bash
cd flask_code/duplicate_invoices

# Build (from duplicate_invoices directory)
docker build -t duplicate-invoice-api -f api/Dockerfile .

# Run
docker run -d -p 8000:8000 --name dup-invoice-api duplicate-invoice-api

# View logs
docker logs -f dup-invoice-api

# Stop
docker stop dup-invoice-api && docker rm dup-invoice-api
```

## GPU Support

For GPU-accelerated processing (requires NVIDIA Docker):

```bash
# Build GPU image
docker build -t duplicate-invoice-api:gpu -f api/Dockerfile.gpu .

# Run with GPU
docker run --gpus all -d -p 8000:8000 --name dup-invoice-api-gpu duplicate-invoice-api:gpu

# Or using docker-compose with GPU profile
docker-compose --profile gpu up --build
```

## API Endpoints

Once running, access:

| Endpoint | Description |
|----------|-------------|
| http://localhost:8000/docs | Swagger UI Documentation |
| http://localhost:8000/health | Health Check |
| http://localhost:8000/device-info | GPU/Device Information |
| http://localhost:8000/benchmark/quick | Quick Benchmark (10K invoices) |

## Configuration

Environment variables (set in docker-compose.yml or via `-e`):

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | 0.0.0.0 | Server bind address |
| `PORT` | 8000 | Server port |
| `WORKERS` | 1 | Number of uvicorn workers |
| `DUPLICATE_INVOICE_THRESHOLD` | 60 | Similarity threshold (0-100) for duplicate detection |
| `TF_CPP_MIN_LOG_LEVEL` | 2 | TensorFlow log level |

## Example API Calls

```bash
# Health check
curl http://localhost:8000/health

# Device info
curl http://localhost:8000/device-info

# Quick benchmark
curl http://localhost:8000/benchmark/quick

# Custom benchmark
curl -X POST http://localhost:8000/benchmark \
  -H "Content-Type: application/json" \
  -d '{"historical_count": 50000, "current_count": 5000}'
```
