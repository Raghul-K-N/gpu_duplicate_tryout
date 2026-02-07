import os
import logging
from datetime import datetime
import utils
from flask import g
# Define a global log_message variable
global_logger = None

def clear_all_variable_values():
    ''' This function is used to clear all variable values'''
   
    utils.data_for_external_api_call = {}

def setup_batch_logger(batch_id):
    global global_logger
    
    # Create logs directory if it doesn't exist.
    script_path = os.path.abspath(__file__)
    parent_directory = os.path.dirname(script_path)
    base_directory = os.path.dirname(parent_directory)
    logs_directory = os.path.join(base_directory, 'iv_logs')
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
    
    # Create a subdirectory named with the current timestamp and batch_id.
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_folder = os.path.join(logs_directory, f"{timestamp}_{batch_id}")
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    
    # Define filenames for general and error logs.
    general_log_filename = os.path.join(log_folder, f"General_log_{timestamp}_{batch_id}.log")
    error_log_filename = os.path.join(log_folder, f"Error_log_{timestamp}_{batch_id}.log")
    
    clear_all_variable_values()
    
    # Create and configure the logger.
    global_logger = logging.getLogger(f"batch_{batch_id}")
    global_logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers to avoid duplicate logs.
    if global_logger.handlers:
        global_logger.handlers.clear()
    
    # Create file handler for general logs: logs messages below ERROR level.
    general_handler = logging.FileHandler(general_log_filename)
    general_handler.setLevel(logging.DEBUG)
    general_handler.addFilter(lambda record: record.levelno < logging.ERROR)
    general_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    general_handler.setFormatter(general_formatter)
    
    # Create file handler for error logs: logs messages at ERROR level and above.
    error_handler = logging.FileHandler(error_log_filename)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    
    # Add both handlers to the logger.
    global_logger.addHandler(general_handler)
    global_logger.addHandler(error_handler)

    initialise_mail_variables(global_logger=global_logger)


def initialise_mail_variables(global_logger):
    g.general_logger = global_logger
    g.error_logger = global_logger
    g.pipeline_name = "iv"
    g.CLIENT_NAME =  ""
    g.data_for_external_mail = []


def log_message(log_message,error_logger:bool = None):
    '''This function logs a message using the global log_message.
    
    Args:
    log_message : str : The message to log.
    '''
    if not error_logger:
        formatted_log_message = f"{log_message}"
        # Log the message using the global log_message
        global_logger.info(formatted_log_message)
    else:
        formatted_log_message = f"{log_message}"
        global_logger.error(formatted_log_message)


