import pandas as pd
import os
from flask import g
from code1.logger import capture_log_message
from code1 import src_load
from app import get_folder_path_for_client
import hist_data.utilities as utils


def store_gl_account_tracker_data():
    """
    THis function reads the GL Account Tracker data from the input folder, checks whether the data already exists in the
    parquet files and database, if not then stores the data in the parquet files and database
    """
    input_folder = g.input_folder_path_for_historical_data
    all_files = os.listdir(input_folder)
    gl_account_tracker_files = [file for file in all_files if str(file).lower().startswith('gl_account_tracker')]
    if len(gl_account_tracker_files) == 0:
        capture_log_message("No GL Account Tracker files found in the input folder")
        return False, (0,0)
    else:
        gl_account_tracker_dataframes = []
        for file in gl_account_tracker_files:
            file_path = os.path.join(input_folder, file)
            gl_account_tracker_dataframes.append(pd.read_csv(file_path))
        gl_account_tracker_data = pd.concat(gl_account_tracker_dataframes,ignore_index=True)
        capture_log_message("Shape of the GL Account Tracker data: " + str(gl_account_tracker_data.shape))
        gl_account_tracker_data = gl_account_tracker_data.rename(columns=utils.GL_ACCOUNT_TRACKER_COLUMN_MAPPING)

        # Check whether the data already exists in parquet files , if not then store the data in parquet files
        main_path = os.getenv("UPLOADS")
        client_base_folder = os.getenv("CLIENT_BASE_FOLDER_PATH",None)
        if client_base_folder is not None:
            capture_log_message(f"Client Base Folder from env file is {client_base_folder}")
            client_folder = client_base_folder
        else:
            client_folder = get_folder_path_for_client(g.client_id)
            capture_log_message(f"Client base folder from DB is :{client_folder}")
    
        client_folder = str(client_folder).strip('/')
        if client_folder == None or main_path == None:
            capture_log_message("Client folder or main path is None or not configured")
            return False, gl_account_tracker_data.shape
        client_path = os.path.join(main_path,client_folder)

        folder_path_for_gl_account_tracker_data_parquet = os.path.join(client_path, utils.PARQUET_FOLDER_NAME_FOR_GL_ACCOUNT_TRACKER_DATA)

        if not os.path.exists(folder_path_for_gl_account_tracker_data_parquet):
            os.mkdir(folder_path_for_gl_account_tracker_data_parquet)
        existing_df = utils.read_data_from_parquet_files(folder_path_for_gl_account_tracker_data_parquet)
        if existing_df is None:
            dest_file_path = os.path.join(folder_path_for_gl_account_tracker_data_parquet, utils.GL_ACCOUNT_TRACKER_FILE_NAME)
            gl_account_tracker_data.to_parquet(dest_file_path, engine='pyarrow')
            capture_log_message("GL Account Tracker Data stored in parquet files")
        else:
            capture_log_message("Data already exists in parquet files")
            # Check for duplicates in the data, if found , drop duplicates from current data and append the remaining data
            capture_log_message("Shape of the existing data: " + str(existing_df.shape))
            capture_log_message("Shape of the new data: " + str(gl_account_tracker_data.shape))
            existing_df[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME] = existing_df[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].astype(str)
            gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME] = gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].astype(str)
            filtered_df = gl_account_tracker_data[~(gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].isin(existing_df[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME]))]
            capture_log_message("Shape of the filtered data: " + str(filtered_df.shape))
            if filtered_df.shape[0]!=0:
                final_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                dest_file_path = os.path.join(folder_path_for_gl_account_tracker_data_parquet, utils.GL_ACCOUNT_TRACKER_FILE_NAME)
                final_df.to_parquet(dest_file_path, engine='pyarrow')
                capture_log_message("GL Account Tracker Data stored in parquet files")
            else:
                capture_log_message("No new data to store in parquet files")


        # check whether data already exists in the database, if not then store the data in the database
        existing_data = src_load.read_table(utils.TABLE_NAME_FOR_GL_ACCOUNT_TRACKER_DATA)
        if existing_data.empty:
            # No data is present in database, so load the GL Account Tracker data into the database
            src_load.upload_data_to_database(data=gl_account_tracker_data, tablename=utils.TABLE_NAME_FOR_GL_ACCOUNT_TRACKER_DATA)
            capture_log_message("GL Account Tracker Data stored in the database")
        else:
            # Data already exists in database, so check for duplicates in the data and append the remaining data
            capture_log_message("Data already exists in the database")
            capture_log_message("Shape of the existing data: " + str(existing_data.shape))
            capture_log_message("Shape of the new data: " + str(gl_account_tracker_data.shape))
            existing_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME] = existing_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].astype(str)
            gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME] = gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].astype(str)
            filtered_df = gl_account_tracker_data[~(gl_account_tracker_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME].isin(existing_data[utils.GL_ACCOUNT_TRACKER_COLUMN_NAME]))]
            capture_log_message("Shape of the filtered data: " + str(filtered_df.shape))
            if filtered_df.shape[0]!=0:
                src_load.upload_data_to_database(data=filtered_df, tablename=utils.TABLE_NAME_FOR_GL_ACCOUNT_TRACKER_DATA)
                capture_log_message("GL Account Tracker Data stored in the database")
            else:
                capture_log_message("No new data to store in the database")

        return True, gl_account_tracker_data.shape