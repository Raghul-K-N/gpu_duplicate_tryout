import pandas as pd
import os
from flask import g
from code1.logger import capture_log_message
from code1 import src_load
from app import get_folder_path_for_client
import hist_data.utilities as utils


def store_invoice_transaction_mapping_data():
    """
    This function reads the Invoice Transaction Mapping data from the input folder, checks whether the data already exists in parquet files
    and database, if not then stores the data in parquet files and database
    """
    input_folder = g.input_folder_path_for_historical_data
    all_files = os.listdir(input_folder)
    invoice_transaction_mapping_files = [file for file in all_files if str(file).lower().startswith('invoice_transaction_mapping')]
    if len(invoice_transaction_mapping_files) == 0:
        capture_log_message("No Invoice Transaction Mapping files found in the input folder")
        return False, (0,0)
    else:
        invoice_transaction_mapping_dataframes = []
        for file in invoice_transaction_mapping_files:
            file_path = os.path.join(input_folder, file)
            invoice_transaction_mapping_dataframes.append(pd.read_csv(file_path))
        invoice_transaction_mapping_data = pd.concat(invoice_transaction_mapping_dataframes,ignore_index=True)
        capture_log_message("Shape of the Invoice Transaction Mapping data: " + str(invoice_transaction_mapping_data.shape))
        invoice_transaction_mapping_data = invoice_transaction_mapping_data.rename(columns=utils.INVOICE_TRANSACTION_MAPPING_COLUMN_MAPPING)

        # Check whether the data already exists in parquet files , if not then store the data in parquet files
        main_folder = os.getenv("UPLOADS")
        client_base_folder = os.getenv("CLIENT_BASE_FOLDER_PATH",None)
        if client_base_folder is not None:
            client_folder = client_base_folder
        else:
            client_folder = get_folder_path_for_client(g.client_id)
        client_folder = str(client_folder).strip('/')
        if client_folder ==None or main_folder ==None:
            capture_log_message("Client folder or main folder is None")
            return False, invoice_transaction_mapping_data.shape
        client_path = os.path.join(main_folder, client_folder)
        folder_path_for_invoice_transaction_mapping_data_parquet = os.path.join(client_path, utils.INVOICE_TRANSACTION_MAPPING_DATA_PARQUET_FOLDER_NAME)
        if not os.path.exists(folder_path_for_invoice_transaction_mapping_data_parquet):
            os.mkdir(folder_path_for_invoice_transaction_mapping_data_parquet)
        existing_df = utils.read_data_from_parquet_files(folder_path_for_invoice_transaction_mapping_data_parquet)
        if existing_df is None:
            dest_file_path = os.path.join(folder_path_for_invoice_transaction_mapping_data_parquet, utils.INVOICE_TRANSACTION_MAPPING_DATA_PARQUET_FILE_NAME)
            invoice_transaction_mapping_data.to_parquet(dest_file_path, engine='pyarrow')
            capture_log_message("Invoice Transaction Mapping Data stored in parquet files")
        else:
            capture_log_message("Data already exists in parquet files")
            # Check for duplicates in the data, if found , drop duplicates from current data and append the remaining data
            capture_log_message("Shape of the existing data: " + str(existing_df.shape))
            capture_log_message("Shape of the new data: " + str(invoice_transaction_mapping_data.shape))
            existing_df[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME] = existing_df[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].astype(str)
            invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME] = invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].astype(str)
            filtered_df = invoice_transaction_mapping_data[~(invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].isin(existing_df[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME]))]
            capture_log_message("Shape of the filtered data: " + str(filtered_df.shape))
            if filtered_df.shape[0]!=0:
                final_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                dest_file_path = os.path.join(folder_path_for_invoice_transaction_mapping_data_parquet, utils.INVOICE_TRANSACTION_MAPPING_DATA_PARQUET_FILE_NAME)
                final_df.to_parquet(dest_file_path, engine='pyarrow')
                capture_log_message("Invoice Transaction Mapping Data stored in parquet files")
            else:
                capture_log_message("No new data to store in parquet files")


        # check whether data already exists in the database, if not then store the data in the database
        existing_data = src_load.read_table(utils.TABLE_NAME_FOR_INVOICE_TRANSACTION_MAPPING_DATA)
        if existing_data.empty:
            # No data is present in database, so load the Invoice Transaction Mapping data into the database
            src_load.upload_data_to_database(data=invoice_transaction_mapping_data, tablename=utils.TABLE_NAME_FOR_INVOICE_TRANSACTION_MAPPING_DATA)
            capture_log_message("Invoice Transaction Mapping Data stored in the database")
        else:
            # Data is already present in the database, so check for duplicates
            capture_log_message("Data already exists in the database")
            capture_log_message("Shape of the existing data in the database: " + str(existing_data.shape))
            invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME] = invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].astype(str)
            existing_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME] = existing_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].astype(str)
            filtered_data = invoice_transaction_mapping_data[~(invoice_transaction_mapping_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME].isin(existing_data[utils.ACCOUNT_DOCUMENT_NUMBER_COLUMN_NAME]))]
            capture_log_message("Shape of the filtered data: " + str(filtered_data.shape))
            if filtered_data.shape[0]!=0:
                src_load.upload_data_to_database(data=filtered_data, tablename=utils.TABLE_NAME_FOR_INVOICE_TRANSACTION_MAPPING_DATA)
                capture_log_message("Invoice Transaction Mapping Data stored in the database")
            else:
                capture_log_message("No new data to store in the database")



        return True, invoice_transaction_mapping_data.shape