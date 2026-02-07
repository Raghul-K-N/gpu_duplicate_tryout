"""
Duplicate Invoice Detection Module
===================================
Provides duplicate invoice detection with optional GPU acceleration.
"""
import logging
import pathlib
import os

# Setup basic logging first
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Try to import logging config, fallback to basic console handler
try:
    from duplicate_invoices.config import logging_config
    logger.addHandler(logging_config.get_console_handler())
except ImportError:
    # Fallback: add a basic console handler
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(funcName)s:%(lineno)d - %(message)s"
    ))
    logger.addHandler(handler)

logger.propagate = False

# Try to get version from VERSION file
try:
    from duplicate_invoices.config import config
    VERSION_PATH = config.PACKAGE_ROOT / 'VERSION'
    with open(VERSION_PATH, 'r') as version_file:
        __version__ = version_file.read().strip()
except Exception:
    # Fallback: try to find VERSION file relative to this file
    try:
        VERSION_PATH = pathlib.Path(__file__).resolve().parent / 'VERSION'
        with open(VERSION_PATH, 'r') as version_file:
            __version__ = version_file.read().strip()
    except Exception:
        __version__ = "1.0.0"  # Default version for Docker deployment