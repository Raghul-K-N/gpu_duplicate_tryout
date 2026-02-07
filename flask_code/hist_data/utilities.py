import dask.dataframe as dd
import pandas as pd
import os
from code1.logger import capture_log_message

"""Constant Variables"""
# INVOICE_TRANSACTION_MAPPING_DATA_FLOW
INVOICE_TRANSACTION_MAPPING_DATA_PARQUET_FOLDER_NAME = 'invoice_transaction_mapping_data_parquet'
TABLE_NAME_FOR_INVOICE_TRANSACTION_MAPPING_DATA = 'invoice_transaction_mapping'
ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME = 'account_document_number'
INVOICE_TRANSACTION_MAPPING_DATA_PARQUET_FILE_NAME = 'invoice_transaction_mapping_data.parquet'

# VAT_DATA_FLOW
PARQUET_FOLDER_NAME_FOR_VAT_DATA = 'vat_data_parquet'
TABLE_NAME_FOR_VAT_DATA = 'vat'
VAT_ID_COLUMN_NAME = 'vat_id'
VAT_DATA_PARQUET_FILE_NAME = 'vat_data.parquet'

# GL_ACCOUNT_TRACKER_FLOW
PARQUET_FOLDER_NAME_FOR_GL_ACCOUNT_TRACKER_DATA = 'gl_account_tracker_data_parquet'
TABLE_NAME_FOR_GL_ACCOUNT_TRACKER_DATA = 'gl_account_tracker'
GL_ACCOUNT_TRACKER_COLUMN_NAME = 'GL_Account_Number'
GL_ACCOUNT_TRACKER_FILE_NAME = 'gl_account_tracker_data.parquet'

# VENDOR_DATA_FLOW
PARQUET_FOLDER_NAME_FOR_VENDOR_DATA = 'vendor_data_parquet'
TABLE_NAME_FOR_VENDOR_DATA = 'ap_vendorlist'
VENDOR_CODE_COLUMN_NAME = 'VENDORCODE'
VENDOR_DATA_PARQUET_FILE_NAME = 'vendor_data.parquet'
VENDOR_DUPLICATE_COLUMNS = ['VENDORCODE', 'VENDOR_NAME', 'bank_account_number', 'bank_name', 'payment_terms']
VENDOR_SIMILARITY_COLUMNS = ['VENDORCODE', 'VENDOR_NAME', 'bank_account_number', 'bank_name', 'payment_terms']
VENDOR_SIMILARITY_THRESHOLD = 4


# 1) Vendor data (raw CSV columns → ap_vendorlist table columns)
VENDOR_COLUMN_MAPPING = {
    'Vendor Code':        'VENDORCODE',
    'Vendor Name':        'VENDOR_NAME',
    'Vendor Address':     'vendor_address',
    'Bank Account Num':   'bank_account_number',
    'Bank Name':          'bank_name',
    'Beneficiary Name':   'beneficiary_name',
    'Currency':           'currency',
    'Payment Terms':      'payment_terms',
    'Contact Person':     'contact_person',
}

# 2) VAT data (raw CSV → vat table)
VAT_COLUMN_MAPPING = {
    'VAT Identifier':   'vat_id',
    'Vendor Code':      'vendor_code',
    'Region':           'region',
}

# 3) Invoice‑transaction mapping (raw CSV → invoice_transaction_mapping table)
INVOICE_TRANSACTION_MAPPING_COLUMN_MAPPING = {
    'Account Doc No':       'account_document_number',
    'Invoice Filename':     'invoice_filename',
    'Invoice Number':       'invoice_number',
}

# 4) GL‑account tracker (raw CSV → gl_account_tracker table)
GL_ACCOUNT_TRACKER_COLUMN_MAPPING = {
    'GL Account #':      'GL_Account_Number',
    'Vendor Code':       'vendor_code',
}

def read_data_from_parquet_files(input_file_path):
    """
    This function reads the data from parquet files and returns the data
    :param input_file_path: The input file path
    :return: The data read from the parquet files
    """
    capture_log_message("Reading data from parquet files, filepath: " + str(input_file_path))
    if os.path.exists(input_file_path):
        all_files = [os.path.join(input_file_path,file) for file in os.listdir(input_file_path)]
        
        capture_log_message(f"all files in filepath {input_file_path} are {all_files}")
        if len(all_files) == 0:
            return None
        data = dd.read_parquet(all_files,engine='pyarrow')
        df = data.compute()
        return df
    else:
        return None