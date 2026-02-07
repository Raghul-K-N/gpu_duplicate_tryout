# general_logger = None
# error_logger = None
# stage_logger = None
# run_id = None
# audit_id = None
# client_id = None
# pipeline_id = None
# pipeline_name = None
# current_module = None
# final_status = None
# module_name_to_id_mapping = {}
# pipeline_type_to_id_mapping = {}
# data_for_external_mail = {}
# data_for_external_api_call = {}

from datetime import datetime
# from code1.logger import capture_log_message
from code1 import src_load 
from flask import g

ZBLOCK_DATA_READ_STAGE = 'ZBLOCK_DATA_READ'
ZBLOCK_DATA_INGESTION_STAGE = 'ZBLOCK_DATA_INGESTION'
Z_BLOCK_INVOICE_VERIFICATION_STAGE = 'Z_BLOCK_INVOICE_VERIFICATION'

AP_DATA_READ_STAGE = 'AP_DATA_READ'
AP_DATA_INGESTION_STAGE = 'AP_DATA_INGESTION'
AP_FLOW_STAGE = 'AP_FLOW'

PENDING_STATUS = 'PENDING'
RUNNING_STATUS = 'RUNNING'
SUCCESS_STATUS = 'SUCCESS'
FAILED_STATUS = 'FAILED'

AP_RAW_DATA_RENAME_MAPPING = {
    "DOCUMENT_NUMBER": "ACCOUNTING_DOC",
    "ITEM_TEXT":"TRANSACTION_DESCRIPTION",
    "DOCUMENT_TYPE":"DOC_TYPE",
    'TOTAL_AMOUNT':'AMOUNT',
    'ENTERED_BY':'ENTERED_BY',
    'POSTED_BY':'POSTED_BY',
    'PAYMENT_DATE':'PAYMENT_DATE',
    "COMPANY_CODE":"COMPANY_CODE",
    "INVOICE_DATE":"INVOICE_DATE",
    "ENTERED_DATE":"ENTERED_DATE",
    "DUE_DATE":"DUE_DATE",
    "POSTED_DATE":"POSTED_DATE",
    "PAYMENT_TERMS":"PAYMENT_TERMS",
    "SUPPLIER_ID":"SUPPLIER_ID",
    # "DEBIT_CREDIT_INDICATOR":"DEBIT_CREDIT_INDICATOR_LINE_ITEM",
    # "DEBIT_CREDIT_INDICATOR_HEADER_LEVEL":"DEBIT_CREDIT_INDICATOR",
    "VENDOR_NAME":"SUPPLIER_NAME",
    "PAYMENT_TERMS_Invoice":"PAYMENT_TERMS",
    "REGION_BKPF":"REGION"


}

EXPIRY_DATE = datetime(2026, 3, 31).date()

DATA_UPLOAD_STAGE = 'DATA_UPLOAD'
DATA_HEALTH_CHECK_STAGE = 'DATA_HEALTH_CHECK_STATUS'
DATA_INGESTION_STAGE = 'DATA_INGESTION'
DATA_SCORING_STAGE = 'DATA_SCORING'
INVOICE_VALIDATION = "INVOICE_VALIDATION"
VENDOR_MASTER = "VENDOR_MASTER"
CLIENT_NAME = ''
AUDIT_NAME = ""

# error_constants.py

NO_INPUT_FILES = "NO_INPUT_FILES"
MAPPING_FILE_ISSUE = "MAPPING_FILE_ISSUE"
UNABLE_TO_READ_INPUT_FILES = "UNABLE_TO_READ_INPUT_FILES"
ISSUE_WITH_COLUMN_MAPPING = "ISSUE_WITH_COLUMN_MAPPING"
ISSUE_WITH_DATE_FORMAT = "ISSUE_WITH_DATE_FORMAT"
MISSING_MANDATORY_COLUMNS = "MISSING_MANDATORY_COLUMNS"
NULL_CHECK_FAILED = "NULL_CHECK_FAILED"
UNIQUE_IDENTIFIER_CHECK_FAILED = "UNIQUE_IDENTIFIER_CHECK_FAILED"
DATE_CHECK_FAILED = "DATE_CHECK_FAILED"
CREDIT_DEBIT_INDICATOR_CHECK_FAILED = "CREDIT_DEBIT_INDICATOR_CHECK_FAILED"
DUE_DATE_CHECK_FAILED = "DUE_DATE_CHECK_FAILED"
MANUAL_ENTRY_FLAG_FAILED = "MANUAL_ENTRY_FLAG_FAILED"
DOC_TYPE_CHECK_FAILED = "DOC_TYPE_CHECK_FAILED"
DEBIT_CREDIT_BALANCE_CHECK_FAILED = "DEBIT_CREDIT_BALANCE_CHECK_FAILED"
INGESTION_FAILED = "INGESTION_FAILED"
GENERAL_SCORING_FAILED = "GENERAL_SCORING_FAILED"
AI_SCORING_FAILED = "AI_SCORING_FAILED"
STAT_SCORING_FAILED = "STAT_SCORING_FAILED"
RULES_SCORING_FAILED = "RULES_SCORING_FAILED"
DUPLICATE_SCORING_FAILED = "DUPLICATE_SCORING_FAILED"
DB_CONNECTION_ISSUE = "DB_CONNECTION_ISSUE"
ISSUES_WITH_STORING_RESULTS = "ISSUES_WITH_STORING_RESULTS"
OTHER_ERRORS = "OTHER_ERRORS"
ISSUE_WITH_STORING_DATA_AS_PARQUET_FILES = "ISSUE_WITH_STORING_DATA_AS_PARQUET_FILES"

manual_doc_types_to_monitor = ["SA", "ZC", "ZL", "ZZ","AA" ]

def check_valid_mapping_file(mapping_csv_temp, mapping_json_file, module):
    """This function check whether the mapping template file is uploaded correctly
    in the expected format with expected columns"""

    if mapping_csv_temp is None:
        return False, "No mapping template found in the uploads directory"
    df = mapping_csv_temp
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # mapping_temp_config = get_mappings("mapping_csv_mand_cols.json")
    with open(mapping_json_file) as f:
        import json
        mapping_temp_config = json.load(f)
    expected_columns = mapping_temp_config['expected_template_cols']
    mandatory_fields = mapping_temp_config[module]['mandatory_fields']
    expected_shape = mapping_temp_config[module]['expected_shape']

    if df.shape != tuple(expected_shape):
        return False , "Invalid Mappings template structure. Please check columns or rows were added or removed!!"
    
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        return False, f"Missing columns in CSV: {', '.join(missing_columns)}"
    
    for field in mandatory_fields:
        if field not in df['Column Names'].values:
             return False, f"Mandatory field '{field}' not found in Mappings template"
        
        if df.loc[(df['Column Names'] == field) & (df['Requirement'] == 'Mandatory')].empty:
            return False, f"Mandatory field '{field}' is not marked as 'Mandatory' in the Mappings template"
    
    return True, "CSV is valid and all mandatory fields are properly marked"


def create_job_query(src_status):
    with src_load.connect_to_database() as connect:
        with connect.cursor() as cur:
            insert_job_query = f"INSERT INTO SRC_STATUS(STATUS,MESSAGE,`MODULE`,audit_id) VALUES({src_status['status']},'{src_status['message']}','{src_status['module']}', '{src_status['audit_id']}');"
            cur.execute(insert_job_query)
            src_id = cur.lastrowid
            connect.commit()
            connect.close()
            return src_id

def make_mapping_unique(source_columns):
    counts = {}
    result = []
    redundant_cols = {}

    for value in source_columns:
        if value == '':
            result.append(value)
        else:
            if value in counts:
                counts[value] += 1
                new_value = f"{value}_{counts[value]}"
                redundant_cols[new_value] = value
            else:
                counts[value] = 0
                new_value = value
            result.append(new_value)
    return result, redundant_cols

def get_mappings(mapping_file_name):
    with open(mapping_file_name) as f:
        import json
        mappings = json.load(f)
    return mappings

def get_mappings_csv(mapping_csv, mapping_file_name):

    df = mapping_csv

    #strip trailing and leading spaces for all str cols
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    g.historical_date = None

    if df['Column Names'].isin(['Type of Data', 'Date threshold']).any():
        if str(df.iloc[-2]['Source Data Column']).lower().startswith("histo"):
            g.historical_date = df.iloc[-1]['Source Data Column']
    else:
        pass
            
    
    df = df[~df['Column Names'].isin(['Type of Data', 'Date threshold'])]

    # Extract the 3rd and 4th columns
    mapping_cols_df = df[['Column Names','Source Data Column']]
    mapping_cols_df = mapping_cols_df.fillna('')

    mapping_cols_df['Source Data Column'], redundant_columns = make_mapping_unique(mapping_cols_df['Source Data Column'])
    db_mappings = get_mappings(mapping_file_name)
    
    # Storing column and english values in a dict
    g.english_col_dict = dict([(val, key) for key, val in db_mappings.items()])
    
    # Map the first column to database columns using the JSON mapping
    db_columns = mapping_cols_df['Column Names'].map(db_mappings)
    db_columns = db_columns.fillna('')
    
    mapping_dict = dict(zip( db_columns, mapping_cols_df['Source Data Column']))
    
    # # Extract mapped cols
    # mapped_column_df = mapping_cols_df.replace('', pd.NA).dropna(subset=['Source Data Column'])
    # g.mapped_column_names = mapped_column_df['Column Names'].map(db_mappings)

    # Extract the date col and Source Date Format col
    date_format_cols = df[['Column Names','Source Date Format']]
    date_format_cols.dropna(subset=['Source Date Format'], inplace = True)
    date_columns = date_format_cols['Column Names'].map(db_mappings)
    date_formats_dict = dict(zip(date_columns , date_format_cols['Source Date Format'].astype(str).str.lower()))
    g.date_formats_dict = date_formats_dict
    
    # Extract mandatory cols
    mandatory_filt = df['Requirement'] == 'Mandatory'
    mandatory_col_names = df.loc[mandatory_filt,'Column Names'].map(db_mappings)
    g.mandatory_col_names = mandatory_col_names.to_list()
    
    return mapping_dict, redundant_columns

def get_config(config_file_name):
    with open("configs.json") as f:
        import json
        config = json.load(f)
    return config

def set_config(configs, mappinggroup_id, selected_module,client_id):
    
    
    module = selected_module
    created_by = 4
    insert_configs = []
    insert_config_query = """INSERT INTO triniconfigmapping (MODULE, CONFIG_KEY, CONFIG_VALUE, CREATED_BY, MAPPINGGROUPID,client_id) VALUES (%s,%s,%s,%s,%s,%s)"""
    for key,val in configs.items():
        config = (module, key, val, created_by, mappinggroup_id,client_id)
        insert_configs.append(config)
    with src_load.connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.executemany(insert_config_query,insert_configs)
            connect.commit()
            connect.close()

def get_random_int(max_int):
    """
    Generates a random integer within the specified range using timestamp and inbuilt functionalities.

    Args:
        max_int: The maximum value for the random integer (inclusive).

    Returns:
        A random integer within the range [0, max_int].
    """
    import random
    import time
    # Combine current time and nanoseconds for better randomness
    seed_value = int(time.time() * 10**9) + int(time.perf_counter_ns())

    # Use the seed to create a random object with improved repeatability
    random.seed(seed_value)

    # Generate a random integer within the desired range
    return random.randint(0, max_int)


def set_mappings(module,mappings, client_id):
    user_id = 4
    # import random
    # mappinggroup_id = random.randrange(1111,99999)
    max_int = 999999
    mappinggroup_id = get_random_int(max_int)
    insert_mappings = []
    insert_mapping_query = """INSERT INTO tronboarddatamapping (MAPPING_MODULE, TR_FIELDNAME, SOURCE_FIELDNAME, UPDATED_BY, MAPPINGROUP_ID, client_id) VALUES (%s,%s,%s,%s,%s,%s)"""
    for key,val in mappings.items():
        mapping = (module, key, val, user_id, mappinggroup_id, client_id)
        insert_mappings.append(mapping)
    with src_load.connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.executemany(insert_mapping_query,insert_mappings)
            connect.commit()
            connect.close()
    return mappinggroup_id
