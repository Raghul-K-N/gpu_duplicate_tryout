#!/bin/bash
# =============================================================================
# Run FastAPI Duplicate Invoice Detection Service
# =============================================================================

# Default settings
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
WORKERS="${WORKERS:-1}"  # Use 1 worker for GPU to avoid memory issues
RELOAD="${RELOAD:-false}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==============================================================================${NC}"
echo -e "${GREEN}          DUPLICATE INVOICE DETECTION API - FastAPI Service${NC}"
echo -e "${BLUE}==============================================================================${NC}"

# Check for GPU
if command -v nvidia-smi &> /dev/null; then
    echo -e "${GREEN}GPU Detected:${NC}"
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader
else
    echo -e "No GPU detected. Running on CPU."
fi

echo ""
echo "Starting server on http://${HOST}:${PORT}"
echo "API Docs: http://${HOST}:${PORT}/docs"
echo "Health Check: http://${HOST}:${PORT}/health"
echo ""

# Change to script directory
cd "$(dirname "$0")"

# Run the server
if [ "$RELOAD" = "true" ]; then
    echo "Running with hot reload enabled..."
    uvicorn main:app --host "$HOST" --port "$PORT" --reload
else
    echo "Running in production mode..."
    uvicorn main:app --host "$HOST" --port "$PORT" --workers "$WORKERS"
fi
