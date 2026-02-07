"""
GPU Configuration for Duplicate Invoice Detection
==================================================

This file contains GPU-specific settings that can be imported
into the main configuration module.

Usage:
    from duplicate_invoices.config.gpu_config import GPU_CONFIG, USE_GPU
"""

import os

# ============================================================
# GPU ACCELERATION SETTINGS
# ============================================================

# Enable/Disable GPU acceleration globally
USE_GPU = os.environ.get('DUPLICATE_INVOICE_USE_GPU', 'true').lower() == 'true'

# GPU Configuration
GPU_CONFIG = {
    # TensorFlow settings
    'tf_memory_growth': True,           # Enable memory growth (prevents TF from allocating all GPU memory)
    'tf_mixed_precision': True,         # Use FP16 for faster computation
    'tf_xla_compile': True,             # Enable XLA JIT compilation
    
    # Processing settings
    'batch_size': 10000,                # Number of pairs per batch for GPU processing
    'chunk_size': 200000,               # Chunk size for large dataset processing
    
    # A100-specific optimizations (80GB VRAM)
    'a100_optimized': True,             # Enable A100-specific optimizations
    'max_memory_fraction': 0.95,        # Maximum GPU memory fraction to use
    
    # Fallback settings
    'cpu_fallback': True,               # Fallback to CPU if GPU unavailable
}

# Performance thresholds
PERFORMANCE_THRESHOLDS = {
    # Minimum dataset size to use GPU (below this, CPU may be faster due to overhead)
    'min_records_for_gpu': 1000,
    
    # Maximum dataset size for synchronous processing
    'max_sync_records': 50000,
    
    # Records per second targets
    'target_throughput_gpu': 100000,    # Records/second on GPU
    'target_throughput_cpu': 5000,      # Records/second on CPU
}

# A100 80GB Specific Settings
A100_CONFIG = {
    'batch_size': 20000,                # Larger batches for A100
    'chunk_size': 500000,               # Larger chunks fit in 80GB
    'embedding_batch_size': 2048,       # For embedding generation
    'similarity_chunk_size': 250000,    # For similarity matrix computation
    'use_tf32': True,                   # Use TF32 for Tensor Cores
    'num_streams': 4,                   # CUDA streams for overlap
}


def get_optimal_batch_size(n_records: int, has_a100: bool = False) -> int:
    """
    Get optimal batch size based on dataset size and GPU type.
    
    Args:
        n_records: Number of records to process
        has_a100: Whether A100 GPU is available
        
    Returns:
        Optimal batch size
    """
    if has_a100:
        if n_records > 1_000_000:
            return A100_CONFIG['batch_size']
        elif n_records > 100_000:
            return 15000
        else:
            return 10000
    else:
        # Generic GPU
        if n_records > 500_000:
            return 10000
        elif n_records > 100_000:
            return 5000
        else:
            return 2000


def get_optimal_chunk_size(n_records: int, available_vram_gb: float = 16.0) -> int:
    """
    Get optimal chunk size based on available VRAM.
    
    Args:
        n_records: Number of records
        available_vram_gb: Available VRAM in GB
        
    Returns:
        Optimal chunk size
    """
    # Rough estimation: each record needs ~4KB for processing
    # Leave 20% headroom for other operations
    max_records = int((available_vram_gb * 0.8 * 1024 * 1024) / 4)
    
    return min(n_records, max_records, 500000)
