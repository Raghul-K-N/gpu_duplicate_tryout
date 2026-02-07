import pandas as pd
import dask.dataframe as dd
import numpy as np
from flask import g
from code1.logger import capture_log_message, update_data_time_period_for_audit_id
from code1 import src_load 
import os
from src_load import connect_to_database


def find_month_label_based_on_date(date_value):
    """This function gets date value as input and creates a
    month-year value label based on the value
    Args:
        date_value (datetime): Date value
    Returns:
        label (str) : Month-Year string value (format: m{month}_{year})
    """
    year = date_value.year
    month = date_value.month
    return f"m{month}_{year}"


def get_quarters(batch_id=None):
    if batch_id:
        query=f"SELECT pqt_filename FROM batch_lookup_src WHERE batch_id = {batch_id};"
    else:
        query = f"SELECT pqt_filename FROM batch_lookup_src;"
    with connect_to_database() as connection:
        quarters_df = pd.read_sql(query,con=connection)
        months_lst = quarters_df['pqt_filename'].unique().tolist()
    return months_lst

# def get_data_details_for_audit(audit_id):
#     try:
#         capture_log_message(f'Getting details from Audit table for audit id {audit_id}')
#         with src_load.connect_to_database() as connection:
#             with connection.cursor(dictionary=True) as cursor:
#                 query = """
#                         SELECT erp_id, audit_start_date, audit_end_date, hist_data_start_date, hist_data_end_date
#                         FROM audit WHERE audit_id = %s
#                     """
#                 cursor.execute(query, (audit_id,))
#                 result = cursor.fetchone()
#         if result:
#             erp_id = result.get("erp_id")
#             curr_date_strt = result.get("audit_start_date")
#             curr_date_end = result.get("audit_end_date")
#             hist_date_strt = result.get("hist_data_start_date")
#             hist_date_end = result.get("hist_data_end_date")

#             capture_log_message(f" Values are Erp_id :{erp_id},Audit start date {curr_date_strt}, Audit end date {curr_date_end}, Hist Start date {hist_date_strt}, Hist End date {hist_date_end}")
#             return erp_id, curr_date_strt, curr_date_end, hist_date_strt, hist_date_end
        
#     except Exception as e:
#         capture_log_message(current_logger=g.error_logger, log_message='{}'.format(e))
#         return None

def read_batch_ddf_from_path(client_folder_path, batch_id):
    capture_log_message(f"Fetching {g.module_nm} historical folder path for Batch id {batch_id}...")
    if g.module_nm =='AP':
        historical_folder_path = os.path.join(client_folder_path,'historical_AP_data_parquet')
    elif g.module_nm == 'ZBLOCK':
        historical_folder_path = os.path.join(client_folder_path,'historical_ZBLOCK_data_parquet')
    else:
        historical_folder_path = os.path.join(client_folder_path,'historical_GL_data_parquet')
    historical_erp_folder_path = os.path.join(historical_folder_path,'erp_'+str(g.erp_id))
    capture_log_message(f"Parquet data folder path is {historical_erp_folder_path}")
    
    files_name_lst = get_batch_quarters_files(batch_id, historical_erp_folder_path)
    if not files_name_lst:
        return pd.DataFrame() 
    
    data_files_path = [os.path.join(historical_erp_folder_path, file) for file in files_name_lst]
    capture_log_message(f"Parquet data files list: {data_files_path}")

    ddf = dd.read_parquet(data_files_path)
 
    # Filter the Dask DataFrame based on the date range
    ddf_filtered = ddf[ddf['batch_id']==batch_id]
    # Convert the filtered Dask DataFrame to a Pandas DataFrame
    data_df = ddf_filtered.compute()

    data_df = data_df.reset_index(drop=True)
    return data_df

def read_ddf_from_path(client_folder_path, start_date, end_date):
    if g.module_nm =='AP':
        historical_folder_path = os.path.join(client_folder_path,'historical_AP_data_parquet')
    elif g.module_nm == 'ZBLOCK':
        historical_folder_path = os.path.join(client_folder_path,'historical_ZBLOCK_data_parquet')
    else:
        historical_folder_path = os.path.join(client_folder_path,'historical_GL_data_parquet')
    historical_erp_folder_path = os.path.join(historical_folder_path,'erp_'+str(g.erp_id))
    capture_log_message(f"Historical ERP data folder path is {historical_erp_folder_path}")
    
    files_name_lst = get_quarters_file_names(start_date, end_date, historical_erp_folder_path)
    if not files_name_lst:
        return pd.DataFrame() 
    
    data_files_path = [os.path.join(historical_erp_folder_path, file) for file in files_name_lst]
    capture_log_message(f"Historical ERP data files list: {data_files_path}")

    ddf = dd.read_parquet(data_files_path)
    
    # Convert start_date and end_date to datetime format for filtering
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Filter the Dask DataFrame based on the date range
    ddf_filtered = ddf[(ddf['POSTED_DATE'] >= start_date) & (ddf['POSTED_DATE'] <= end_date)]

    # Convert the filtered Dask DataFrame to a Pandas DataFrame
    data_df = ddf_filtered.compute()
    data_df = data_df.reset_index(drop=True)
    return data_df


def process_batch_data(batch_id, client_id):

    erp_id = os.getenv("ERP_ID")
    
    g.hist_date_strt = None
    g.hist_date_end = None
    
    g.erp_id = erp_id
    client_base_folder = os.getenv("CLIENT_BASE_FOLDER_PATH",None)
    if client_base_folder is not None:
        capture_log_message(f"Client base folder path from env is {client_base_folder}")
        client_folder = client_base_folder
    else:
        client_folder = get_folder_path_for_client(client_id)
        
    
    if client_folder == None:
        capture_log_message(f"Client folder not found for client_id:{client_id}")
        return None
    client_folder = str(client_folder).strip('/')
    
    base_path = os.getenv("UPLOADS","")
    client_folder_path = os.path.join(base_path,client_folder)
    
    g.client_folder_path = client_folder_path

    data_df = read_batch_ddf_from_path(g.client_folder_path, batch_id)

    if data_df.empty:
        capture_log_message(f"No data files found in folder path!!")
        return None
    
    min_date = pd.to_datetime(data_df['POSTED_DATE']).min()
    max_date = pd.to_datetime(data_df['POSTED_DATE']).max()
    min_date_str = min_date.strftime('%b %Y')
    max_date_str = max_date.strftime('%b %Y')
    update_data_time_period_for_audit_id(min_date_str+'-'+max_date_str)
    hist_no_of_days = int(os.getenv("HIST_NO_OF_DAYS","1")) - 1
    if hist_no_of_days:
        hist_date_end = min_date - pd.Timedelta(days=1)
        hist_date_strt = hist_date_end - pd.Timedelta(days=hist_no_of_days)
        if hist_date_strt and hist_date_end:
            g.hist_date_strt = hist_date_strt
            g.hist_date_end = hist_date_end
    return data_df
    

def get_folder_path_for_client(client_id):
    try:
        with src_load.connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = f""" select client_path from client_data_storage_mapping
                             where client_id = {client_id}"""
                cursor.execute(query)
                results = cursor.fetchall()
                if len(results)>0:
                    return results[0][0]
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, log_message='{}'.format(e))
        return None
    
    
    
def get_quarters_file_names(start_date, end_date, folder_path):
    # Convert start and end dates to datetime format
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    if start_date> end_date:
        return []
    # Build list of months between start and end (inclusive)
    # Use PeriodRange so partial-month dates still map to their month
    month_periods = pd.period_range(start=start_date, end=end_date, freq='M')
    months = [f"m{p.month}_{p.year}" for p in month_periods]

    # Ensure uniqueness and preserve order (period_range already gives unique months)
    unique_months = list(dict.fromkeys(months))

    capture_log_message(f'List of Months for {start_date} and {end_date} is {unique_months}')
    if not os.path.exists(folder_path):
        return []
    files = os.listdir(folder_path)

    month_file_names = [file for file in files
        if any(month in file for month in unique_months) and file.endswith('.parquet')]
    capture_log_message(f"Files to read :{month_file_names}")
    return month_file_names


def get_batch_quarters_files(batch_id, folder_path):
    try:
        capture_log_message(f'Getting details from batch_lookup_src table for batch_id - {batch_id}')

        batch_pqt_files_lst = get_quarters(batch_id)
        
        if not os.path.exists(folder_path):
            return []
        files = os.listdir(folder_path)
        batch_pqt_file_names = [file for file in files 
            if any(name in file for name in batch_pqt_files_lst) and file.endswith(".parquet")]
        # batch_pqt_file_names = [file for file in files if file.endswith(".parquet")]
        capture_log_message(f"Len of Files to read :{len(batch_pqt_file_names)}[{batch_pqt_file_names}]")
        return batch_pqt_file_names
 
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, log_message='{}'.format(e))
        return None
