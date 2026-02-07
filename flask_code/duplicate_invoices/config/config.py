import pathlib
import pandas as pd
import os

pd.options.display.max_rows = 10
pd.options.display.max_columns = 10

# Try to import threshold from code1 (Flask app context), fallback for standalone/Docker
try:
    from code1.src_load import get_duplicate_invoice_threshold_value
    THRESHOLD_VALUE = get_duplicate_invoice_threshold_value()
except (ImportError, RuntimeError) as e:
    # Default threshold when code1 is not available (Docker/standalone deployment)
    # This matches the default value returned by get_duplicate_invoice_threshold_value()
    THRESHOLD_VALUE = float(os.environ.get('DUPLICATE_INVOICE_THRESHOLD', 60))

POSTED_DATE_THRESHOLD = 365

# Calculate PACKAGE_ROOT from this file's location (avoids circular import)
# config.py is at duplicate_invoices/config/config.py, so parent.parent is duplicate_invoices/
PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent
TRAINED_MODEL_DIR = PACKAGE_ROOT / "trained_models"
EXTERNAL_MODEL_DIR = PACKAGE_ROOT / "external_models"

DATASET_DIR = PACKAGE_ROOT / "datasets"
# TESTING_DATA_FILE = "ap_accountdocuments_1708.csv"
# TRAINING_DATA_FILE = "ap_accountdocuments_1708.csv"

TESTING_DATA_FILE = "ap_accountdoc_dup_inv_19_09.csv"
TRAINING_DATA_FILE = "ap_accountdoc_dup_inv_19_09.csv"

MODE = 'AMC' #'AMC' # 'CCS'
SUPPLIER_SIMILARITY_CHECK = False

# PRIMARY_KEY_VARIABLES = ['AP_PLANT', 'VOUCHER', 'CHECK_NUMBER', 'SUPPLIER', 'INVOICE_NUMBER', 'INVOICE_AMOUNT', 'INVOICE_DATE']
PRIMARY_KEY_VARIABLES = ['SUPPLIER_ID', 'INVOICE_NUMBER', 'INVOICE_AMOUNT', 'INVOICE_DATE']


SUPPLIER_NAME_COLUMN = 'SUPPLIER_NAME' #'VENDOR_NAME.1' #'SUPPLIER_NAME'
SUPPLIER_ID_COLUMN = "SUPPLIER_ID"
INVOICE_NUMBER_COLUMN = "INVOICE_NUMBER"
INVOICE_AMOUNT_COLUMN = 'INVOICE_AMOUNT'
INVOICE_DATE_COLUMN = 'INVOICE_DATE'
COMPANY_COLUMN = 'COMPANY_NAME'
INVOICE_STATUS_COLUMN = None
INVOICE_TYPE_COLUMN = None
SUPPLY_GROUP_ID_COLUMN = None

SCENARIO_TABLE_NAME = "scenarios"  # Table name in the database for scenarios


# GROUPING_COLUMNS = (SUPPLIER_NAME_COLUMN, 'INVOICE_AMOUNT_ABS', INVOICE_DATE_COLUMN)  #AMC
GROUPING_COLUMNS = ( 'INVOICE_AMOUNT_ABS', INVOICE_DATE_COLUMN)   #CCS

SUPPLIER_MODEL_GROUPING_COLUMNS = ('INVOICE_AMOUNT_ABS', INVOICE_DATE_COLUMN)
INVOICE_MODEL_GROUPING_COLUMNS = ('DUPLICATE_ID_SUPPLIER_NAME_ML',)

PIPELINE_NAME = "duplicate_detector"
PIPELINE_SAVE_FILE = f"{PIPELINE_NAME}_output_v"

# used for differential testing
# ACCEPTABLE_MODEL_DIFFERENCE = 0.05

LOGS_DIR = PACKAGE_ROOT / "logs"

EXACT_MATCHING = False

INVOICE_NUMBER_SIMILARITY_MODEL = 'RULE_BASED' # 'ML' or 'RULE_BASED' # Redundant if EXACT_MATCHING==True
