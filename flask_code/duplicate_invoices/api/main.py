"""
FastAPI Duplicate Invoice Detection Service
============================================

Independent FastAPI application for duplicate invoice detection
with TensorFlow GPU acceleration.

Features:
- REST API for duplicate detection
- GPU-accelerated processing
- Batch processing support
- Async endpoints for large datasets
- Health checks and monitoring

Run:
    uvicorn duplicate_invoices.api.main:app --host 0.0.0.0 --port 8000 --reload
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
from io import BytesIO

# ============================================================
# Logging Configuration
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('duplicate_invoice_api')
logger.setLevel(logging.DEBUG)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import duplicate invoice components with fallbacks
logger.info("Loading GPU backend module...")
try:
    from duplicate_invoices.gpu.tf_backend import (
        TFDuplicateAccelerator,
        get_device_info,
        is_gpu_available
    )
    logger.info("GPU backend module loaded successfully")
except ImportError as e:
    # Fallback if GPU module not available
    logger.warning(f"GPU module not available, using fallback: {e}")
    TFDuplicateAccelerator = None
    def get_device_info():
        return {"gpu_available": False, "tensorflow_version": "N/A"}
    def is_gpu_available():
        return False

logger.info("Loading duplicate extract helper module...")
try:
    from duplicate_invoices.model.duplicate_extract_helper import (
        is_invoice_similar,
        get_similarity_score,
        SCORE_THRESOLD
    )
    logger.info("Duplicate extract helper loaded successfully")
except ImportError as e:
    # Fallback similarity function
    logger.warning(f"Duplicate extract helper not available, using fallback: {e}")
    SCORE_THRESOLD = 60.0
    def is_invoice_similar(inv1, inv2):
        return inv1 == inv2, 100.0 if inv1 == inv2 else 0.0
    def get_similarity_score(inv1, inv2):
        return inv1 == inv2, 100.0 if inv1 == inv2 else 0.0

logger.info("Loading config module...")
try:
    from duplicate_invoices.config.config import (
        THRESHOLD_VALUE,
        INVOICE_NUMBER_COLUMN,
        SUPPLIER_ID_COLUMN,
        INVOICE_AMOUNT_COLUMN,
        INVOICE_DATE_COLUMN,
        SUPPLIER_NAME_COLUMN
    )
    logger.info(f"Config loaded - Threshold: {THRESHOLD_VALUE}")
except (ImportError, AttributeError) as e:
    # Default column names (fallback for circular import or missing module)
    logger.warning(f"Config module not available, using defaults: {e}")
    THRESHOLD_VALUE = 60.0
    INVOICE_NUMBER_COLUMN = "INVOICE_NUMBER"
    SUPPLIER_ID_COLUMN = "SUPPLIER_ID"
    INVOICE_AMOUNT_COLUMN = "INVOICE_AMOUNT"
    INVOICE_DATE_COLUMN = "INVOICE_DATE"
    SUPPLIER_NAME_COLUMN = "SUPPLIER_NAME"

# Thread pool for CPU-bound operations
max_workers = os.cpu_count() or 4
logger.info(f"Initializing thread pool with {max_workers} workers")
executor = ThreadPoolExecutor(max_workers=max_workers)

# Global accelerator instance
accelerator = None  # Will be TFDuplicateAccelerator if GPU available
logger.info("Module initialization complete")


# ============================================================
# Pydantic Models
# ============================================================

class InvoiceRecord(BaseModel):
    """Single invoice record for duplicate checking."""
    invoice_number: str = Field(..., description="Invoice number")
    supplier_id: Optional[str] = Field(None, description="Supplier/Vendor ID")
    supplier_name: Optional[str] = Field(None, description="Supplier/Vendor name")
    invoice_amount: float = Field(..., description="Invoice amount")
    invoice_date: str = Field(..., description="Invoice date (YYYY-MM-DD)")
    is_current: bool = Field(True, description="Is this current data (vs historical)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class DuplicateCheckRequest(BaseModel):
    """Request for duplicate checking."""
    invoices: List[InvoiceRecord] = Field(..., description="List of invoices to check")
    threshold: float = Field(default=60.0, ge=0, le=100, description="Similarity threshold (0-100)")
    use_gpu: bool = Field(True, description="Use GPU acceleration if available")

    class Config:
        json_schema_extra = {
            "example": {
                "invoices": [
                    {
                        "invoice_number": "INV-001",
                        "supplier_id": "SUP-123",
                        "invoice_amount": 1000.50,
                        "invoice_date": "2024-01-15",
                        "is_current": True
                    },
                    {
                        "invoice_number": "INV-001A",
                        "supplier_id": "SUP-123",
                        "invoice_amount": 1000.50,
                        "invoice_date": "2024-01-15",
                        "is_current": False
                    }
                ],
                "threshold": 60.0,
                "use_gpu": True
            }
        }


class DuplicatePair(BaseModel):
    """A pair of duplicate invoices."""
    source_index: int
    target_index: int
    source_invoice: str
    target_invoice: str
    similarity_score: float
    is_exact_match: bool


class DuplicateCheckResponse(BaseModel):
    """Response for duplicate checking."""
    success: bool
    total_invoices: int
    duplicates_found: int
    duplicate_groups: int
    processing_time_ms: float
    used_gpu: bool
    duplicates: List[DuplicatePair]
    threshold_used: float


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    gpu_available: bool
    gpu_info: Optional[Dict[str, Any]]
    version: str


class BatchJobResponse(BaseModel):
    """Response for batch job submission."""
    job_id: str
    status: str
    message: str


class BatchJobStatus(BaseModel):
    """Status of a batch job."""
    job_id: str
    status: str
    progress: float
    result: Optional[DuplicateCheckResponse]
    error: Optional[str]


# ============================================================
# Background Job Storage
# ============================================================

batch_jobs: Dict[str, Dict] = {}


# ============================================================
# FastAPI App
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global accelerator
    
    # Startup
    logger.info("=" * 60)
    logger.info("DUPLICATE INVOICE DETECTION API - STARTING")
    logger.info("=" * 60)
    
    # Initialize TensorFlow accelerator if available
    if TFDuplicateAccelerator is not None:
        logger.info("Attempting to initialize TensorFlow GPU accelerator...")
        try:
            accelerator = TFDuplicateAccelerator()
            logger.info("TensorFlow GPU accelerator initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize GPU accelerator: {e}")
            accelerator = None
    else:
        logger.info("TensorFlow accelerator not available")
    
    device_info = get_device_info()
    logger.info(f"TensorFlow Version: {device_info.get('tensorflow_version')}")
    logger.info(f"GPU Available: {device_info.get('gpu_available')}")
    if device_info.get('gpu_available'):
        logger.info(f"GPU Name: {device_info.get('gpu_name', 'Unknown')}")
        logger.info(f"GPU Memory: {device_info.get('gpu_memory', 'Unknown')}")
    logger.info("=" * 60)
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Duplicate Invoice API...")
    executor.shutdown(wait=True)
    logger.info("Thread pool shutdown complete")
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Duplicate Invoice Detection API",
    description="GPU-accelerated duplicate invoice detection service",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Helper Functions
# ============================================================

def process_duplicates_sync(
    invoices: List[InvoiceRecord],
    threshold: float,
    use_gpu: bool
) -> DuplicateCheckResponse:
    """
    Synchronous duplicate processing.
    """
    start_time = time.time()
    
    # Convert to DataFrame for processing
    data = []
    for idx, inv in enumerate(invoices):
        data.append({
            'index': idx,
            INVOICE_NUMBER_COLUMN: inv.invoice_number,
            SUPPLIER_ID_COLUMN: inv.supplier_id or '',
            INVOICE_AMOUNT_COLUMN: inv.invoice_amount,
            INVOICE_DATE_COLUMN: inv.invoice_date,
            SUPPLIER_NAME_COLUMN: inv.supplier_name or '',
            'is_current_data': inv.is_current,
            'INVOICE_AMOUNT_ABS': abs(inv.invoice_amount)
        })
    
    df = pd.DataFrame(data)
    
    # Group by supplier_id and invoice_amount_abs for comparison
    duplicates = []
    duplicate_groups = set()
    
    # Generate all pairs within groups
    for _, group_df in df.groupby([SUPPLIER_ID_COLUMN, 'INVOICE_AMOUNT_ABS']):
        if len(group_df) < 2:
            continue
        
        indices = group_df['index'].tolist()
        invoice_numbers = group_df[INVOICE_NUMBER_COLUMN].tolist()
        is_current = group_df['is_current_data'].tolist()
        
        # Check all pairs in group
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                # At least one must be current data
                if not (is_current[i] or is_current[j]):
                    continue
                
                # Check similarity
                is_dup, score = is_invoice_similar(
                    invoice_numbers[i],
                    invoice_numbers[j]
                )
                
                if is_dup and score >= threshold:
                    duplicates.append(DuplicatePair(
                        source_index=indices[i],
                        target_index=indices[j],
                        source_invoice=invoice_numbers[i],
                        target_invoice=invoice_numbers[j],
                        similarity_score=round(score, 2),
                        is_exact_match=(invoice_numbers[i] == invoice_numbers[j])
                    ))
                    duplicate_groups.add(frozenset([indices[i], indices[j]]))
    
    processing_time = (time.time() - start_time) * 1000
    
    return DuplicateCheckResponse(
        success=True,
        total_invoices=len(invoices),
        duplicates_found=len(duplicates),
        duplicate_groups=len(duplicate_groups),
        processing_time_ms=round(processing_time, 2),
        used_gpu=use_gpu and is_gpu_available(),
        duplicates=duplicates,
        threshold_used=threshold
    )


async def process_batch_job(job_id: str, invoices: List[InvoiceRecord], threshold: float, use_gpu: bool):
    """Process a batch job asynchronously."""
    logger.info(f"[Job {job_id[:8]}] Starting batch job processing with {len(invoices)} invoices")
    try:
        batch_jobs[job_id]['status'] = 'processing'
        batch_jobs[job_id]['progress'] = 0.1
        logger.debug(f"[Job {job_id[:8]}] Status updated to processing")
        
        # Run in thread pool to not block event loop
        loop = asyncio.get_event_loop()
        start_time = time.time()
        result = await loop.run_in_executor(
            executor,
            process_duplicates_sync,
            invoices,
            threshold,
            use_gpu
        )
        elapsed = time.time() - start_time
        
        batch_jobs[job_id]['status'] = 'completed'
        batch_jobs[job_id]['progress'] = 1.0
        batch_jobs[job_id]['result'] = result
        logger.info(f"[Job {job_id[:8]}] Completed in {elapsed:.2f}s - Found {result.duplicates_found} duplicates")
        
    except Exception as e:
        batch_jobs[job_id]['status'] = 'failed'
        batch_jobs[job_id]['error'] = str(e)
        logger.error(f"[Job {job_id[:8]}] Failed with error: {e}")


# ============================================================
# API Endpoints
# ============================================================

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint."""
    logger.debug("Root endpoint called")
    return {
        "service": "Duplicate Invoice Detection API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    logger.debug("Health check endpoint called")
    device_info = get_device_info()
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        gpu_available=device_info.get('gpu_available', False),
        gpu_info=device_info if device_info.get('gpu_available') else None,
        version="1.0.0"
    )


@app.get("/device-info")
async def device_info():
    """Get detailed device information."""
    logger.debug("Device info endpoint called")
    info = get_device_info()
    logger.info(f"Device info: GPU={info.get('gpu_available')}, TF={info.get('tensorflow_version')}")
    return info


@app.post("/check-duplicates", response_model=DuplicateCheckResponse)
async def check_duplicates(request: DuplicateCheckRequest):
    """
    Check for duplicate invoices in the provided list.
    
    This endpoint performs synchronous duplicate detection.
    For large datasets (>10000 records), use the /batch endpoint.
    """
    logger.info(f"Check duplicates called with {len(request.invoices)} invoices, threshold={request.threshold}")
    
    if len(request.invoices) > 50000:
        logger.warning(f"Request rejected: too many invoices ({len(request.invoices)})")
        raise HTTPException(
            status_code=400,
            detail="Too many invoices. Use /batch endpoint for large datasets (>50000 records)"
        )
    
    if len(request.invoices) < 2:
        logger.warning(f"Request rejected: not enough invoices ({len(request.invoices)})")
        raise HTTPException(
            status_code=400,
            detail="At least 2 invoices required for duplicate detection"
        )
    
    try:
        start_time = time.time()
        result = process_duplicates_sync(
            request.invoices,
            request.threshold,
            request.use_gpu
        )
        elapsed = time.time() - start_time
        logger.info(f"Check duplicates completed in {elapsed:.3f}s - Found {result.duplicates_found} duplicates")
        return result
    except Exception as e:
        logger.error(f"Check duplicates failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/batch", response_model=BatchJobResponse)
async def submit_batch_job(
    request: DuplicateCheckRequest,
    background_tasks: BackgroundTasks
):
    """
    Submit a batch job for large dataset processing.
    
    Returns a job_id that can be used to check status and retrieve results.
    """
    import uuid
    
    job_id = str(uuid.uuid4())
    logger.info(f"Batch job submitted: {job_id[:8]}... with {len(request.invoices)} invoices")
    
    batch_jobs[job_id] = {
        'status': 'queued',
        'progress': 0.0,
        'result': None,
        'error': None,
        'created_at': datetime.now().isoformat()
    }
    
    background_tasks.add_task(
        process_batch_job,
        job_id,
        request.invoices,
        request.threshold,
        request.use_gpu
    )
    logger.debug(f"[Job {job_id[:8]}] Added to background tasks")
    
    return BatchJobResponse(
        job_id=job_id,
        status="queued",
        message=f"Batch job submitted with {len(request.invoices)} invoices"
    )


@app.get("/batch/{job_id}", response_model=BatchJobStatus)
async def get_batch_status(job_id: str):
    """Get the status of a batch job."""
    logger.debug(f"Batch status requested for job {job_id[:8]}...")
    if job_id not in batch_jobs:
        logger.warning(f"Job not found: {job_id[:8]}...")
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = batch_jobs[job_id]
    logger.debug(f"[Job {job_id[:8]}] Status: {job['status']}, Progress: {job['progress']}")
    
    return BatchJobStatus(
        job_id=job_id,
        status=job['status'],
        progress=job['progress'],
        result=job['result'],
        error=job['error']
    )


@app.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    threshold: float = Query(default=60.0, ge=0, le=100),
    use_gpu: bool = Query(default=True)
):
    """
    Upload a CSV file for duplicate detection.
    
    Expected columns:
    - INVOICE_NUMBER (required)
    - SUPPLIER_ID (optional)
    - INVOICE_AMOUNT (required)
    - INVOICE_DATE (required)
    """
    logger.info(f"CSV upload received: {file.filename}")
    
    if file.filename is None or not file.filename.endswith('.csv'):
        logger.warning(f"Invalid file type: {file.filename}")
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    try:
        # Read CSV
        contents = await file.read()
        file_size_mb = len(contents) / (1024 * 1024)
        logger.info(f"CSV file size: {file_size_mb:.2f} MB")
        
        df = pd.read_csv(BytesIO(contents))
        logger.info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Validate required columns
        required_cols = ['INVOICE_NUMBER', 'INVOICE_AMOUNT', 'INVOICE_DATE']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {missing}"
            )
        
        # Convert to invoice records
        logger.debug("Converting CSV rows to invoice records...")
        invoices = []
        for _, row in df.iterrows():
            invoices.append(InvoiceRecord(
                invoice_number=str(row['INVOICE_NUMBER']),
                supplier_id=str(row.get('SUPPLIER_ID', '')),
                supplier_name=str(row.get('SUPPLIER_NAME', '')),
                invoice_amount=float(row['INVOICE_AMOUNT']),
                invoice_date=str(row['INVOICE_DATE']),
                is_current=bool(row.get('IS_CURRENT', True)),
                metadata=None
            ))
        logger.info(f"Converted {len(invoices)} invoice records")
        
        # Process
        start_time = time.time()
        result = process_duplicates_sync(invoices, threshold, use_gpu)
        elapsed = time.time() - start_time
        logger.info(f"CSV processing completed in {elapsed:.2f}s - Found {result.duplicates_found} duplicates")
        
        return result
        
    except pd.errors.EmptyDataError:
        logger.error("CSV file is empty")
        raise HTTPException(status_code=400, detail="CSV file is empty")
    except Exception as e:
        logger.error(f"CSV upload processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/config")
async def get_config():
    """Get current configuration."""
    logger.debug("Config endpoint called")
    return {
        "threshold": THRESHOLD_VALUE,
        "invoice_number_column": INVOICE_NUMBER_COLUMN,
        "supplier_id_column": SUPPLIER_ID_COLUMN,
        "invoice_amount_column": INVOICE_AMOUNT_COLUMN,
        "invoice_date_column": INVOICE_DATE_COLUMN,
        "supplier_name_column": SUPPLIER_NAME_COLUMN,
        "gpu_available": is_gpu_available()
    }


# ============================================================
# Benchmark / Test Endpoints
# ============================================================

class BenchmarkConfig(BaseModel):
    """Configuration for benchmark test."""
    historical_count: int = Field(default=1000000, ge=1000, le=50000000, description="Number of historical invoices")
    current_count: int = Field(default=50000, ge=100, le=5000000, description="Number of current invoices")
    threshold: float = Field(default=60.0, ge=0, le=100, description="Similarity threshold")
    use_gpu: bool = Field(default=True, description="Use GPU if available")
    duplicate_rate: float = Field(default=0.05, ge=0.01, le=0.5, description="Approximate duplicate rate (0.01-0.5)")
    num_suppliers: int = Field(default=10000, ge=100, le=100000, description="Number of unique suppliers")
    num_amounts: int = Field(default=5000, ge=100, le=50000, description="Number of unique invoice amounts")


class BenchmarkResult(BaseModel):
    """Result of benchmark test."""
    success: bool
    historical_count: int
    current_count: int
    total_pairs_checked: int
    duplicates_found: int
    duplicate_groups: int
    
    # Timing breakdown
    data_generation_time_sec: float
    preprocessing_time_sec: float
    grouping_time_sec: float
    similarity_computation_time_sec: float
    result_formatting_time_sec: float
    total_time_sec: float
    
    # Performance metrics
    pairs_per_second: float
    records_per_second: float
    
    # System info
    used_gpu: bool
    gpu_info: Optional[Dict[str, Any]]
    
    # Memory usage
    peak_memory_mb: float
    
    threshold_used: float


def generate_realistic_invoice_number(base_id: int, variation: int = 0) -> str:
    """Generate realistic invoice numbers with variations for duplicates."""
    prefixes = ['INV', 'INVOICE', 'IN', 'I', 'PO', 'BILL', '']
    separators = ['-', '_', '', '/']
    
    prefix = prefixes[base_id % len(prefixes)]
    separator = separators[(base_id // 10) % len(separators)]
    
    # Base number
    number = f"{base_id:08d}"
    
    # Add variations for potential duplicates
    if variation == 0:
        invoice_num = f"{prefix}{separator}{number}"
    elif variation == 1:
        # Similar variation - different prefix
        invoice_num = f"{prefix}{separator}{number}A"
    elif variation == 2:
        # Missing leading zeros
        invoice_num = f"{prefix}{separator}{str(int(number))}"
    elif variation == 3:
        # Extra suffix
        invoice_num = f"{prefix}{separator}{number}-01"
    elif variation == 4:
        # Slightly different number (typo simulation)
        typo_num = number[:-1] + str((int(number[-1]) + 1) % 10)
        invoice_num = f"{prefix}{separator}{typo_num}"
    else:
        invoice_num = f"{prefix}{separator}{number}"
    
    return invoice_num.strip('-_/')


def generate_test_data(
    historical_count: int,
    current_count: int,
    duplicate_rate: float,
    num_suppliers: int,
    num_amounts: int
) -> tuple:
    """
    Generate realistic test data for benchmarking.
    
    Creates a mix of:
    - Completely unique invoices
    - Exact duplicate invoices
    - Similar invoice numbers (variations)
    - Same supplier/amount combinations
    
    Returns:
        tuple: (historical_df, current_df, expected_duplicate_count)
    """
    import random
    from datetime import datetime, timedelta
    
    logger.info(f"Generating test data: {historical_count:,} historical + {current_count:,} current")
    logger.debug(f"Parameters: duplicate_rate={duplicate_rate}, suppliers={num_suppliers}, amounts={num_amounts}")
    
    random.seed(42)  # Reproducible results
    np.random.seed(42)
    
    # Generate supplier IDs
    suppliers = [f"SUP-{i:06d}" for i in range(num_suppliers)]
    
    # Generate invoice amounts (realistic distribution)
    amounts = np.round(np.abs(np.random.lognormal(mean=7, sigma=1.5, size=num_amounts)), 2)
    amounts = np.clip(amounts, 10, 1000000)  # Between $10 and $1M
    
    # Date range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days
    
    # Calculate how many duplicates to create
    num_duplicates = int(current_count * duplicate_rate)
    
    # Generate historical data
    historical_data = []
    for i in range(historical_count):
        supplier = suppliers[i % num_suppliers]
        amount = amounts[i % num_amounts]
        invoice_date = start_date + timedelta(days=random.randint(0, date_range - 365))
        invoice_num = generate_realistic_invoice_number(i)
        
        historical_data.append({
            'INVOICE_NUMBER': invoice_num,
            'SUPPLIER_ID': supplier,
            'SUPPLIER_NAME': f"Supplier {supplier}",
            'INVOICE_AMOUNT': float(amount),
            'INVOICE_DATE': invoice_date.strftime('%Y-%m-%d'),
            'is_current_data': False,
            'INVOICE_AMOUNT_ABS': abs(float(amount)),
            '_base_id': i
        })
    
    historical_df = pd.DataFrame(historical_data)
    
    # Generate current data with intentional duplicates
    current_data = []
    duplicate_indices = random.sample(range(historical_count), min(num_duplicates, historical_count))
    
    for i in range(current_count):
        if i < num_duplicates and i < len(duplicate_indices):
            # Create a duplicate/similar invoice
            hist_idx = duplicate_indices[i]
            hist_row = historical_data[hist_idx]
            
            # Decide variation type
            variation_type = random.randint(0, 4)
            
            invoice_num = generate_realistic_invoice_number(hist_row['_base_id'], variation_type)
            supplier = hist_row['SUPPLIER_ID']
            amount = hist_row['INVOICE_AMOUNT']
            
            # Date within 30 days of original
            orig_date = datetime.strptime(hist_row['INVOICE_DATE'], '%Y-%m-%d')
            invoice_date = orig_date + timedelta(days=random.randint(-30, 30))
        else:
            # Create unique invoice
            base_id = historical_count + i
            invoice_num = generate_realistic_invoice_number(base_id)
            supplier = suppliers[random.randint(0, num_suppliers - 1)]
            amount = amounts[random.randint(0, num_amounts - 1)]
            invoice_date = start_date + timedelta(days=random.randint(date_range - 365, date_range))
        
        current_data.append({
            'INVOICE_NUMBER': invoice_num,
            'SUPPLIER_ID': supplier,
            'SUPPLIER_NAME': f"Supplier {supplier}",
            'INVOICE_AMOUNT': float(amount),
            'INVOICE_DATE': invoice_date.strftime('%Y-%m-%d'),
            'is_current_data': True,
            'INVOICE_AMOUNT_ABS': abs(float(amount))
        })
    
    current_df = pd.DataFrame(current_data)
    
    logger.info(f"Test data generated: historical={len(historical_df)}, current={len(current_df)}, expected_duplicates={num_duplicates}")
    
    return historical_df, current_df, num_duplicates


def run_full_duplicate_detection(
    historical_df: pd.DataFrame,
    current_df: pd.DataFrame,
    threshold: float,
    use_gpu: bool
) -> Dict:
    """
    Run full duplicate detection pipeline matching the actual implementation.
    
    This simulates the actual duplicate detection flow:
    1. Combine historical and current data
    2. Group by supplier and amount
    3. Check all pairs within groups
    4. Return duplicates
    """
    import gc
    import tracemalloc
    
    tracemalloc.start()
    timings = {}
    
    # Preprocessing
    preprocess_start = time.time()
    
    # Combine data
    combined_df = pd.concat([historical_df, current_df], ignore_index=True)
    combined_df['index'] = combined_df.index
    
    # Format invoice numbers (similar to preprocessors.py)
    combined_df['INVOICE_NUMBER_FORMAT'] = combined_df['INVOICE_NUMBER'].str.strip().str.upper()
    
    timings['preprocessing'] = time.time() - preprocess_start
    
    # Grouping
    group_start = time.time()
    
    # Group by supplier and amount (matching actual implementation)
    groups = combined_df.groupby(['SUPPLIER_ID', 'INVOICE_AMOUNT_ABS'])
    group_indices = {name: group.index.tolist() for name, group in groups if len(group) >= 2}
    
    timings['grouping'] = time.time() - group_start
    
    # Similarity computation
    similarity_start = time.time()
    
    duplicates = []
    duplicate_groups = set()
    total_pairs = 0
    
    for group_key, indices in group_indices.items():
        if len(indices) < 2:
            continue
        
        group_df = combined_df.loc[indices]
        invoice_numbers = group_df['INVOICE_NUMBER_FORMAT'].tolist()
        is_current = group_df['is_current_data'].tolist()
        group_indices_list = group_df['index'].tolist()
        
        # Check all pairs (matching actual implementation)
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                total_pairs += 1
                
                # At least one must be current
                if not (is_current[i] or is_current[j]):
                    continue
                
                # Check similarity using actual function
                is_dup, score = is_invoice_similar(
                    invoice_numbers[i],
                    invoice_numbers[j]
                )
                
                if is_dup and score >= threshold:
                    duplicates.append({
                        'source_index': group_indices_list[i],
                        'target_index': group_indices_list[j],
                        'source_invoice': invoice_numbers[i],
                        'target_invoice': invoice_numbers[j],
                        'similarity_score': round(score, 2),
                        'is_exact_match': invoice_numbers[i] == invoice_numbers[j]
                    })
                    duplicate_groups.add(frozenset([group_indices_list[i], group_indices_list[j]]))
    
    timings['similarity'] = time.time() - similarity_start
    
    # Result formatting
    format_start = time.time()
    result_df = pd.DataFrame(duplicates) if duplicates else pd.DataFrame()
    timings['formatting'] = time.time() - format_start
    
    # Memory stats
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    gc.collect()
    
    return {
        'duplicates': duplicates,
        'duplicate_groups': len(duplicate_groups),
        'total_pairs': total_pairs,
        'timings': timings,
        'peak_memory_mb': peak / 1024 / 1024
    }


@app.post("/benchmark", response_model=BenchmarkResult)
async def run_benchmark(config: BenchmarkConfig):
    """
    Run a full benchmark test with generated data.
    
    This endpoint:
    1. Generates realistic invoice data (historical + current)
    2. Runs the full duplicate detection pipeline
    3. Returns detailed timing and performance metrics
    
    WARNING: This is computationally intensive! 
    - 1M historical + 50K current may take 30+ minutes on CPU
    - Use smaller counts for quick tests
    """
    logger.info(f"Benchmark started: {config.historical_count:,} historical + {config.current_count:,} current")
    logger.info(f"Config: threshold={config.threshold}, duplicate_rate={config.duplicate_rate}, use_gpu={config.use_gpu}")
    total_start = time.time()
    
    # Step 1: Generate test data
    gen_start = time.time()
    historical_df, current_df, expected_duplicates = generate_test_data(
        historical_count=config.historical_count,
        current_count=config.current_count,
        duplicate_rate=config.duplicate_rate,
        num_suppliers=config.num_suppliers,
        num_amounts=config.num_amounts
    )
    data_gen_time = time.time() - gen_start
    logger.info(f"Data generation completed in {data_gen_time:.2f}s")
    
    # Step 2: Run duplicate detection
    logger.info("Starting duplicate detection...")
    results = run_full_duplicate_detection(
        historical_df=historical_df,
        current_df=current_df,
        threshold=config.threshold,
        use_gpu=config.use_gpu
    )
    
    total_time = time.time() - total_start
    
    logger.info(f"Benchmark completed in {total_time:.2f}s")
    logger.info(f"Results: pairs_checked={results['total_pairs']:,}, duplicates={len(results['duplicates'])}, peak_memory={results['peak_memory_mb']:.2f}MB")
    
    # Calculate metrics
    total_records = config.historical_count + config.current_count
    pairs_per_second = results['total_pairs'] / results['timings']['similarity'] if results['timings']['similarity'] > 0 else 0
    records_per_second = total_records / total_time if total_time > 0 else 0
    
    return BenchmarkResult(
        success=True,
        historical_count=config.historical_count,
        current_count=config.current_count,
        total_pairs_checked=results['total_pairs'],
        duplicates_found=len(results['duplicates']),
        duplicate_groups=results['duplicate_groups'],
        
        data_generation_time_sec=round(data_gen_time, 3),
        preprocessing_time_sec=round(results['timings']['preprocessing'], 3),
        grouping_time_sec=round(results['timings']['grouping'], 3),
        similarity_computation_time_sec=round(results['timings']['similarity'], 3),
        result_formatting_time_sec=round(results['timings']['formatting'], 3),
        total_time_sec=round(total_time, 3),
        
        pairs_per_second=round(pairs_per_second, 2),
        records_per_second=round(records_per_second, 2),
        
        used_gpu=config.use_gpu and is_gpu_available(),
        gpu_info=get_device_info() if config.use_gpu else None,
        
        peak_memory_mb=round(results['peak_memory_mb'], 2),
        threshold_used=config.threshold
    )


@app.get("/benchmark/quick")
async def quick_benchmark():
    """
    Run a quick benchmark with 10K historical + 1K current invoices.
    Good for testing the endpoint works correctly.
    """
    logger.info("Quick benchmark endpoint called")
    config = BenchmarkConfig(
        historical_count=10000,
        current_count=1000,
        threshold=60.0,
        use_gpu=True,
        duplicate_rate=0.05,
        num_suppliers=500,
        num_amounts=200
    )
    return await run_benchmark(config)


@app.get("/benchmark/medium")
async def medium_benchmark():
    """
    Run a medium benchmark with 100K historical + 10K current invoices.
    Takes a few minutes on CPU.
    """
    logger.info("Medium benchmark endpoint called")
    config = BenchmarkConfig(
        historical_count=100000,
        current_count=10000,
        threshold=60.0,
        use_gpu=True,
        duplicate_rate=0.05,
        num_suppliers=2000,
        num_amounts=1000
    )
    return await run_benchmark(config)


@app.get("/benchmark/full")
async def full_benchmark():
    """
    Run full benchmark with 1M historical + 50K current invoices.
    
    WARNING: This takes a LONG time on CPU (30+ minutes).
    Recommended to use GPU or run as background job.
    """
    logger.info("Full benchmark endpoint called")
    config = BenchmarkConfig(
        historical_count=10000000,
        current_count=50000,
        threshold=60.0,
        use_gpu=True,
        duplicate_rate=0.05,
        num_suppliers=10000,
        num_amounts=5000
    )
    return await run_benchmark(config)


@app.post("/benchmark/async", response_model=BatchJobResponse)
async def submit_benchmark_job(
    config: BenchmarkConfig,
    background_tasks: BackgroundTasks
):
    """
    Submit a benchmark as a background job.
    
    Use this for large benchmarks that would timeout on synchronous requests.
    Poll /batch/{job_id} for status and results.
    """
    import uuid
    
    job_id = str(uuid.uuid4())
    logger.info(f"Async benchmark submitted: {job_id[:8]}... ({config.historical_count:,} + {config.current_count:,})")
    
    batch_jobs[job_id] = {
        'status': 'queued',
        'progress': 0.0,
        'result': None,
        'error': None,
        'created_at': datetime.now().isoformat(),
        'type': 'benchmark',
        'config': config.model_dump()
    }
    
    async def run_benchmark_job():
        try:
            logger.info(f"[Job {job_id[:8]}] Starting async benchmark")
            batch_jobs[job_id]['status'] = 'generating_data'
            batch_jobs[job_id]['progress'] = 0.1
            
            # Generate data
            logger.debug(f"[Job {job_id[:8]}] Generating test data...")
            historical_df, current_df, _ = generate_test_data(
                historical_count=config.historical_count,
                current_count=config.current_count,
                duplicate_rate=config.duplicate_rate,
                num_suppliers=config.num_suppliers,
                num_amounts=config.num_amounts
            )
            
            batch_jobs[job_id]['status'] = 'processing'
            batch_jobs[job_id]['progress'] = 0.3
            logger.debug(f"[Job {job_id[:8]}] Running duplicate detection...")
            
            # Run detection in thread pool
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                executor,
                run_full_duplicate_detection,
                historical_df,
                current_df,
                config.threshold,
                config.use_gpu
            )
            
            batch_jobs[job_id]['status'] = 'completed'
            batch_jobs[job_id]['progress'] = 1.0
            batch_jobs[job_id]['result'] = {
                'duplicates_found': len(results['duplicates']),
                'duplicate_groups': results['duplicate_groups'],
                'total_pairs_checked': results['total_pairs'],
                'timings': results['timings'],
                'peak_memory_mb': results['peak_memory_mb']
            }
            logger.info(f"[Job {job_id[:8]}] Completed - Found {len(results['duplicates'])} duplicates")
            
        except Exception as e:
            batch_jobs[job_id]['status'] = 'failed'
            batch_jobs[job_id]['error'] = str(e)
            logger.error(f"[Job {job_id[:8]}] Failed: {e}")
    
    background_tasks.add_task(run_benchmark_job)
    
    return BatchJobResponse(
        job_id=job_id,
        status="queued",
        message=f"Benchmark job submitted: {config.historical_count:,} historical + {config.current_count:,} current invoices"
    )


# ============================================================
# Main Entry Point
# ============================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1  # Use 1 worker for GPU to avoid memory issues
    )
