"""
GPU Acceleration Module for Duplicate Invoice Detection
========================================================

This module provides TensorFlow-based GPU acceleration for 
the duplicate invoice detection pipeline.

Usage:
    from duplicate_invoices.gpu import TFDuplicateAccelerator, is_gpu_available
    
    if is_gpu_available():
        accelerator = TFDuplicateAccelerator()
        # Use accelerator for batch processing
"""

from .tf_backend import (
    TFDuplicateAccelerator,
    TFGroupProcessor,
    get_accelerator,
    is_gpu_available,
    get_device_info,
    configure_tensorflow_gpu,
    HAS_GPU
)

__all__ = [
    'TFDuplicateAccelerator',
    'TFGroupProcessor',
    'get_accelerator',
    'is_gpu_available',
    'get_device_info',
    'configure_tensorflow_gpu',
    'HAS_GPU'
]
