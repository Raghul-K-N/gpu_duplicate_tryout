import logging
import os
from datetime import datetime, timezone

def setup_logger(batch_id=0, log_level=logging.DEBUG):
    """
    Set up logger for SAP data pipeline with batch_id based log folders
    in the configured master log directory.
    """
    # Get environment configurations
    base_path = os.getenv('UPLOADS', None)
    master_folder = os.getenv('MASTER_FOLDER', 'dow_transformation')
    master_log_folder = os.getenv('MASTER_LOG_FOLDER', 'master_logs')
    
    # Generate timestamp (UTC hour start) for the folder name
    utc_now = datetime.now(timezone.utc)
    utc_hour_start = utc_now.replace(minute=0, second=0, microsecond=0)
    run_timestamp = utc_hour_start.strftime("%Y%m%d%H%M%S")
    
    # Construct log directory path
    if base_path:
        # Path: UPLOADS -> MASTER_FOLDER -> MASTER_LOG_FOLDER -> batch_<batch_id>_logs -> sap_pipeline_<timestamp>
        log_dir = os.path.join(
            base_path, 
            master_folder, 
            master_log_folder,
            f"batch_{batch_id}_logs",
            f"sap_pipeline_{run_timestamp}"
        )
    else:
        # Fallback if UPLOADS is not set
        log_dir = os.path.join(os.path.dirname(__file__), 'logs', f"logs_{run_timestamp}")
        
    os.makedirs(log_dir, exist_ok=True)
    
    # Fixed log filename within the timestamped folder
    str_utc_now = utc_now.strftime("%Y%m%d%H%M%S")
    filname = f"sap_pipeline_{str_utc_now}.log"
    log_filepath = os.path.join(log_dir, filname)
    
    # Create logger
    logger = logging.getLogger('sap_pipeline')
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create file handler
    file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
    file_handler.setLevel(log_level)
    
    # Create console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Only show warnings and errors on console
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s -  %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logger initialized. Log file: {log_filepath}")
    
    return logger

def get_logger():
    """
    Get the existing logger or create a new one
    """
    logger = logging.getLogger('sap_pipeline')
    if not logger.handlers:
        logger = setup_logger(batch_id=0)
    return logger