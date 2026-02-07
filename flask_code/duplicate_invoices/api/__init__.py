"""
FastAPI Application for Duplicate Invoice Detection
====================================================

This module provides a REST API for duplicate invoice detection
with TensorFlow GPU acceleration.

Run the API:
    uvicorn duplicate_invoices.api.main:app --host 0.0.0.0 --port 8000
    
Or with hot reload:
    uvicorn duplicate_invoices.api.main:app --reload
"""

from .main import app

__all__ = ['app']
