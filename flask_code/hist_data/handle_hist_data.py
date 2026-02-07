import traceback
import pandas as pd
from flask import g
from app import get_folder_path_for_client
import os 
import dask.dataframe as dd
from code1.logger import capture_log_message, process_data_for_sending_internal_mail
from code1.src_load import add_entry_in_batch_lookup_src_table, add_entry_in_lookup_src_table, add_snapshot_data_in_src_snapshot_table, fetch_src_id_from_lookup_src_table, update_final_values_in_client_historical_data_table
from datetime import datetime, timezone
import hist_data.utilities as utilities
from databases.sharding_tables import ShardingTables
from pandas.api.types import is_integer_dtype, is_float_dtype, is_datetime64_any_dtype, is_object_dtype, is_string_dtype, is_numeric_dtype
import utils
from code1 import src_load

def align_column_dtypes(existing_df: pd.DataFrame, filtered_df: pd.DataFrame) -> tuple:

    for col in filtered_df.columns:
        if col in existing_df.columns:
            existing_dtype = existing_df[col].dtype
            filtered_dtype = filtered_df[col].dtype
            
            try:
                if existing_dtype == filtered_dtype:
                    continue

                elif is_datetime64_any_dtype(existing_dtype) or is_datetime64_any_dtype(filtered_dtype):
                    existing_df[col] = pd.to_datetime(existing_df[col])
                    filtered_df[col] = pd.to_datetime(filtered_df[col])

                elif is_string_dtype(existing_dtype) or is_string_dtype(filtered_dtype) or \
                    is_object_dtype(existing_dtype) or is_object_dtype(filtered_dtype):
                    existing_df[col] = existing_df[col].fillna("").astype(str)
                    filtered_df[col] = filtered_df[col].fillna("").astype(str)
                    
                elif (is_integer_dtype(existing_dtype) and is_float_dtype(filtered_dtype)) or \
                   (is_float_dtype(existing_dtype) and is_integer_dtype(filtered_dtype)):
                    capture_log_message(f"float col:{col},dtype:{filtered_df[col].dtype}")
                    existing_df[col] = existing_df[col].astype(float)
                    filtered_df[col] = filtered_df[col].astype(float)

                elif is_numeric_dtype(existing_dtype) and is_numeric_dtype(filtered_dtype):
                    existing_df[col] = existing_df[col].fillna(0).astype(int)
                    filtered_df[col] = filtered_df[col].fillna(0).astype(int)

            except (ValueError, TypeError) as e:
                capture_log_message(current_logger=g.error_logger, log_message=f"Error converting column '{col}': {e}")
            except Exception as e:
                capture_log_message(current_logger=g.error_logger,log_message=f"Error Occurred while merging dataframes:{str(e)}")
    return existing_df, filtered_df


def filter_out_duplicate_data(existing_df:pd.DataFrame,filtered_df:pd.DataFrame):
    """ This function compares the existing data with current data, make sure that
    current data is not already present in existing data and the return the updated data
    
    Args:
        existing_df (pd.DataFrame) -> Existing Historical data
        filtered_df (pd.DataFrame) -> Current Data/ New Data
        
    Returns:
        final_df (pd.DataFrame) -> Final Data that has no duplicated data
    """
    # merged_df = pd.concat([existing_df,filtered_df],ignore_index=True)
    # merged_df['acc_doc'] = merged_df['ACCOUNTING_DOC'].astype(str).str.split('-').str[0]   
    
    existing_df['acc_doc'] = existing_df['ACCOUNTING_DOC'].astype(str).str.split('-').str[0]
    filtered_df['acc_doc'] = filtered_df['ACCOUNTING_DOC'].astype(str).str.split('-').str[0]
    
    existing_df, filtered_df = align_column_dtypes(existing_df, filtered_df)
    if g.module_nm == "AP":
        subset_columns =  ['acc_doc','COMPANY_NAME','POSTED_DATE','INVOICE_NUMBER','INVOICE_DATE']
    else:
        subset_columns =  ['acc_doc','FISCAL_YEAR','COMPANY_NAME','CLIENT']
    unique_df =  filtered_df[~ (filtered_df[subset_columns].apply(tuple,axis=1).isin(existing_df[subset_columns].apply(tuple,axis=1)) )]
    # final_df = merged_df.drop_duplicates(subset=['acc_doc','COMPANY_CODE','POSTED_DATE','INVOICE_NUMBER','INVOICE_DATE'],keep='first',ignore_index='True')
    
    if unique_df.empty:
        return pd.DataFrame()
    else:
        return unique_df.drop(columns=['acc_doc'])


# def find_month_label_based_on_date(date_value):
#     """This function gets date value as input and creates a
#     month-year value label based on the value
#     Args:
#         date_value (datetime): Date value
#     Returns:
#         label (str) : Month-Year string value (format: m{month}_{year})
#     """
#     year = date_value.year
#     month = date_value.month
#     return f"m{month}_{year}"

# def find_label_based_on_date(date_value):
#     """This function gets date value as input and figure out the
#     quarter-year value label based on the value

#     Args:
#         date_value (datetime): Date value
        
#     Returns:
#         label (str) : Quarter-Year string value
#     """
#     year = date_value.year
#     quarter = date_value.quarter
#     return f"q{quarter}_{year}"


def add_ids_ap(df: pd.DataFrame, parquet_path: str):
    """ This function adds Transaction_id and Account_doc_id to the dataframe """

    
    columns_for_uuid = src_load.get_ap_columns_to_create_uuid()
    columns_for_uuid[columns_for_uuid.index('COMPANY_CODE')] = 'COMPANY_NAME'
    
    capture_log_message(f"Columns for creating UUID: {columns_for_uuid}")

    if os.listdir(parquet_path):
        quarters_list = [file for file in os.listdir(parquet_path) if file.endswith('.parquet')]
        data_files_path = [os.path.join(parquet_path, file) for file in quarters_list]
        capture_log_message(f"Data files path for adding ids: {data_files_path}")
        ddf = dd.read_parquet(data_files_path).compute()
        prev_tran_id = max(ddf['TRANSACTION_ID'])
        prev_acc_doc_id = max(ddf['ACCOUNT_DOC_ID'])
        df['TRANSACTION_ID'] = range(1, len(df)+1)
        capture_log_message(f"df shape in add_ids: {ddf.shape}")
        capture_log_message(f"Transaction id start and end values: {ddf['TRANSACTION_ID'].to_list()[0]}...{ddf['TRANSACTION_ID'].to_list()[-1]}")

        new_data = filter_out_duplicate_data(existing_df=ddf,filtered_df=df)
        if new_data.empty:
            capture_log_message(f"No new data available to add ids")
            return new_data
        capture_log_message(f"Previous Transaction ID: {prev_tran_id}, Previous Account Doc ID: {prev_acc_doc_id}")
        new_data['TRANSACTION_ID'] = range(prev_tran_id+1, prev_tran_id+1+len(new_data))

        new_data['group_count'] = new_data.groupby(columns_for_uuid).ngroup()+1
        new_data['ACCOUNT_DOC_ID'] = prev_acc_doc_id + new_data['group_count']
        # capture_log_message(f"New data shape in add_ids: {new_data.shape}, {new_data['TRANSACTION_ID'].to_list()}")
        capture_log_message(f"New data transaction id start and end values: {new_data['TRANSACTION_ID'].to_list()[0]}...{new_data['TRANSACTION_ID'].to_list()[-1]}")
        new_data = new_data.drop(columns=['group_count'])   
        capture_log_message(f"Transaction ID and Account Doc ID added to the dataframe")
        return new_data

    else: 
        if 'TRANSACTION_ID' not in df.columns:
            df['TRANSACTION_ID'] = range(1, len(df)+1)
            capture_log_message(f"df columns in add_ids: {df.columns}")
        else:
            capture_log_message(f"TRANSACTION_ID column already exists in dataframe, min and max values: {df['TRANSACTION_ID'].min()}...{df['TRANSACTION_ID'].max()}")
        if 'ACCOUNT_DOC_ID' not in df.columns:
            df['ACCOUNT_DOC_ID'] = df.groupby(columns_for_uuid).ngroup()+1
            capture_log_message(f"Transaction ID and Account Doc ID added to the dataframe")
        else:
            capture_log_message(f"ACCOUNT_DOC_ID column already exists in dataframe, min and max values: {df['ACCOUNT_DOC_ID'].min()}...{df['ACCOUNT_DOC_ID'].max()}")
        return df    


# waste remove later
def add_ids_zblock(df: pd.DataFrame, parquet_path: str):
    """ This function adds Transaction_id and Account_doc_id to the dataframe """

    columns_for_uuid = src_load.get_zblock_columns_to_create_uuid()
    columns_for_uuid[columns_for_uuid.index('COMPANY_CODE')] = 'COMPANY_NAME'
    
    capture_log_message(f"Columns for creating UUID: {columns_for_uuid}")

    if os.listdir(parquet_path):
        quarters_list = [file for file in os.listdir(parquet_path) if file.endswith('.parquet')]
        data_files_path = [os.path.join(parquet_path, file) for file in quarters_list]
        capture_log_message(f"Data files path for adding ids: {data_files_path}")
        ddf = dd.read_parquet(data_files_path).compute()
        prev_tran_id = max(ddf['TRANSACTION_ID'])
        prev_acc_doc_id = max(ddf['ACCOUNT_DOC_ID'])
        df['TRANSACTION_ID'] = range(1, len(df)+1)
        capture_log_message(f"df shape in add_ids: {ddf.shape}")
        capture_log_message(f"Transaction id start and end values: {ddf['TRANSACTION_ID'].to_list()[0]}...{ddf['TRANSACTION_ID'].to_list()[-1]}")

        new_data = filter_out_duplicate_data(existing_df=ddf,filtered_df=df)
        if new_data.empty:
            capture_log_message(f"No new data available to add ids")
            return new_data
        capture_log_message(f"Previous Transaction ID: {prev_tran_id}, Previous Account Doc ID: {prev_acc_doc_id}")
        new_data['TRANSACTION_ID'] = range(prev_tran_id+1, prev_tran_id+1+len(new_data))

        new_data['group_count'] = new_data.groupby(columns_for_uuid).ngroup()+1
        new_data['ACCOUNT_DOC_ID'] = prev_acc_doc_id + new_data['group_count']
        # capture_log_message(f"New data shape in add_ids: {new_data.shape}, {new_data['TRANSACTION_ID'].to_list()}")
        capture_log_message(f"New data transaction id start and end values: {new_data['TRANSACTION_ID'].to_list()[0]}...{new_data['TRANSACTION_ID'].to_list()[-1]}")
        new_data = new_data.drop(columns=['group_count'])   
        capture_log_message(f"Transaction ID and Account Doc ID added to the dataframe")
        return new_data

    else: 
        df['TRANSACTION_ID'] = range(1, len(df)+1)
        capture_log_message(f"df columns in add_ids: {df.columns}")
        df['ACCOUNT_DOC_ID'] = df.groupby(columns_for_uuid).ngroup()+1
        capture_log_message(f"Transaction ID and Account Doc ID added to the dataframe")
        return df   
    

def store_hist_data_in_parquet_file(batch_id: int,df: pd.DataFrame):
    '''
    This function gets dataframe as input, segregates data based on Quarter-year combination and 
    store the data in one/many parquet files
    
    Args:
        df : DataFrame
        
    Returns:
        flag : Bool , True/False that indicates the final status of the process
        str: Error Message if process failed
    '''
    try:
        # Add batch id to all rows
        capture_log_message(f'Batch id for storing data in parquet files is {batch_id}')
        df['batch_id'] = batch_id
        df['POSTED_DATE'] = pd.to_datetime(df['POSTED_DATE'])
        uploaded_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data_storage_strt_time = datetime.now(timezone.utc)
        min_date = min(df['POSTED_DATE'])
        max_date = max(df['POSTED_DATE'])
        total_additional_records = 0
        main_path = os.getenv("UPLOADS","")
        client_base_path = os.getenv("CLIENT_BASE_FOLDER_PATH",None)
        if client_base_path is not None:
            capture_log_message(f"Client base folder path from env file is {client_base_path}")
            client_folder = client_base_path
        else:
            client_folder = get_folder_path_for_client(g.client_id)
        client_folder = str(client_folder).strip('/')
        capture_log_message(f'Client foler path for client id {g.client_id} and module {g.module_nm} is {client_folder}')
        if client_folder == None:
            return False, 'Could not find Client Main Folder', False
        else:
            client_path = os.path.join(main_path,client_folder)
            if g.module_nm =='AP':
                historical_data_folder_path = os.path.join(client_path,'historical_AP_data_parquet')
            else:
                historical_data_folder_path = os.path.join(client_path,'historical_ZBLOCK_data_parquet')
            historical_data_folder_path = os.path.normpath(historical_data_folder_path)
            capture_log_message(f"Historical data folder path is {historical_data_folder_path}")
            if not os.path.exists(historical_data_folder_path):
                capture_log_message(f"Create historical data folder {historical_data_folder_path}")
                os.makedirs(historical_data_folder_path,exist_ok=True)
            erp_folder_path = os.path.join(historical_data_folder_path,'erp_'+str(g.erp_id))
            if not os.path.exists(erp_folder_path):
                capture_log_message(f'Creating ERP path folder {erp_folder_path}')
                os.makedirs(erp_folder_path,exist_ok=True)
            
            capture_log_message(f'ERP data folder path is {erp_folder_path}')
            # df['date_label'] = df['POSTED_DATE'].apply(lambda x: find_label_based_on_date((x)))
            # Add month_label column
            # df['month_label'] = df['POSTED_DATE'].apply(lambda x: find_month_label_based_on_date((x)))


            # Adding Transaction_id and Account_doc_id
            if g.module_nm == "AP":
                new_data = add_ids_ap(df = df, parquet_path=erp_folder_path)
            else:
                new_data = add_ids_zblock(df = df, parquet_path=erp_folder_path)

            if not new_data.empty:
                new_data_flag = True
                total_additional_records = new_data.shape[0]
                if g.module_nm == "ZBLOCK":
                    list_of_labels = new_data['date_label'].value_counts().index.values
                else:
                    list_of_labels = new_data['MONTH_LABEL'].value_counts().index.values
            else:
                new_data_flag = False
                list_of_labels = []
            
            capture_log_message(f'No. of months data available for client {g.client_id} and ERP {g.erp_id} is {list_of_labels}')

            for label in list_of_labels:
                if g.module_nm == "AP":
                    label_column = 'MONTH_LABEL'
                else:
                    label_column = 'date_label'
                filtered_df = new_data[new_data[label_column]==label].copy()
                file_name = 'hist_'+str(g.client_id)+'_'+str(g.erp_id)+'_'+str(label)+'.parquet'
                parquet_file_path = os.path.join(erp_folder_path,file_name)
                capture_log_message(f'File path for {label} client id {g.client_id} and erp id {g.erp_id} is {parquet_file_path}')
                
                # Parse month and year from label (format: M{month}-{year})
                if g.module_nm == "AP":
                    month = label.split('_')[0].replace('m', '')
                    quarter = str((int(month) - 1) // 3 + 1)
                    year = label.split('_')[1]
                else:
                    quarter,year = label.split('_')
                    quarter = quarter.split('q')[1]
                    month = None

                month_int = int(month) if month is not None else None

                no_of_records = 0
                if not os.path.exists(parquet_file_path):
                    capture_log_message(f"File DOES NOT exist {parquet_file_path}")
                    filtered_df.to_parquet(parquet_file_path)
                    if file_name.endswith(".parquet"):
                        db_table_name = os.path.splitext(file_name)[0]
                        capture_log_message(f"Creating table {db_table_name}")
                        sharding_instance = ShardingTables()
                        sharding_instance.create_parquet_data_table(db_table_name)
                    else:
                        capture_log_message(f"Could not create table for {file_name}")
                    
                    # src_id = add_entry_in_lookup_src_table(hist_id=g.hist_id,quarter=quarter,year=year,src_name=str(file_name).split('.')[0])
                    add_entry_in_batch_lookup_src_table(batch_id=batch_id,pqt_filename=label,
                                                        year=int(year),
                                                        quarter=int(quarter),
                                                        month=month_int,
                                                        no_of_records=filtered_df.shape[0],)
                    no_of_records = filtered_df.shape[0]
                    
                else:
                    capture_log_message(f"File already exists {parquet_file_path}")
                    src_id = fetch_src_id_from_lookup_src_table(hist_id=g.hist_id,quarter=quarter,year=year)
                    existing_df = dd.read_parquet(parquet_file_path,engine='pyarrow')
                    existing_df = existing_df.compute()
                    final_df = pd.concat([existing_df,filtered_df],ignore_index=True)

                    add_entry_in_batch_lookup_src_table(batch_id=batch_id,pqt_filename=label,
                                                    year=int(year),
                                                    quarter=int(quarter),
                                                    month=month_int,
                                                    no_of_records=final_df.shape[0],)
                    no_of_records = final_df.shape[0] - existing_df.shape[0]
                    final_df['VENDOR_PO_BOX'] = final_df['VENDOR_PO_BOX'].fillna('').astype(str)
                    final_df.to_parquet(parquet_file_path)

                capture_log_message(f"{no_of_records} new records stored in parquet file for {label} and client id {g.client_id}")

                # flag = add_snapshot_data_in_src_snapshot_table(src_id=src_id,no_of_records=no_of_records,uploaded_by=g.user_id,uploaded_at=uploaded_time)
                # if flag:
                #     capture_log_message(f'Row added successfully in src_snapshot table for {label} and client id {g.client_id}')
                # else:
                #     capture_log_message(current_logger=g.error_logger,
                #                         log_message=f'Error occurred while adding row in src_snapshot table for {label} ')
            # flag = update_final_values_in_client_historical_data_table(hist_id=g.hist_id,
            #                                                            start_date=min_date,end_date=max_date,
            #                                                            no_of_records=total_additional_records,
            #                                                            modified_by=g.user_id,
            #                                                            modified_at=uploaded_time)

            if (total_additional_records)!=0:
                capture_log_message(f"{total_additional_records} total new records stored in parquet file/s!!")
            else:
                capture_log_message(f"No new records to be stored in parquet!!")

            data_storage_end_time = datetime.now(timezone.utc)
            time_taken_for_data_storage = data_storage_end_time - data_storage_strt_time
            capture_log_message(f'Time taken for storing data in parquet files is {time_taken_for_data_storage}')

            return True,'Success', new_data_flag
            # if flag:
            #     # process_data_for_sending_internal_mail(subject='Data Storage Status',stage=utils.DATA_INGESTION_STAGE,is_success=True,
            #     #                                 description_list=['Historical Data Stored Successfully'], 
            #     #                                 volume_list=[(total_additional_records,)],
            #     #                                 time_taken_list=[time_taken_for_data_storage],
            #     #                                 date_list=[data_storage_strt_time],
            #     #                                 historical_flow=True)
            #     return flag,'Success', new_data_flag
            # else:
            #     return flag,'Failure', new_data_flag
            
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,
                            log_message= str(traceback.format_exc()),
                            error_name=utils.ISSUE_WITH_STORING_DATA_AS_PARQUET_FILES)
        return False, str(e), False