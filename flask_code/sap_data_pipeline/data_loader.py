import pandas as pd
import os
from .logger_config import get_logger


from .utils import EXPECTED_TABLES, Z_BLOCK_EXPECTED_TABLES




def read_all_sap_tables(folder_path, z_block=True):
    all_data = {}
    all_tables = []
    if z_block:
        all_tables = Z_BLOCK_EXPECTED_TABLES
    else:
        all_tables = EXPECTED_TABLES

    all_files = os.listdir(folder_path)

    # temporary filter only a subset of rows, only for testing
    # all_files = [each for each in all_files if 'TRD403' in each]

    for table in all_tables:
        data = read_sap_table(table_name=table, folder_path=folder_path,z_block=z_block,all_files=all_files)
        if data is not None:
            all_data[table] = data
        else:
            logger = get_logger()
            logger.warning(f"No data found for table {table}")
    return all_data, all_tables



def read_sap_table(table_name, folder_path,all_files,z_block=False):
    logger = get_logger()
    if len(all_files) == 0:
        logger.error("No files found in the specified folder.")
        return None
    if z_block and table_name=='BKPF':
        specific_files = [each for each in all_files if table_name.lower() in each.lower()  and each.lower().startswith('z') ]
    else: 
        specific_files = [each for each in all_files if table_name.lower() in each.lower() and not each.lower().startswith('z') ]

    if len(specific_files) == 0:
        logger.warning(f"No file found for table {table_name}.")
        return None
    else:
        logger.info(f"Files found for table {table_name}: Count is {len(specific_files)}")    
    dfs = []
    cols_count = 0
    for file in specific_files:
        file_path = os.path.join(folder_path, file)
        try:
            if file_path.lower().endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_excel(file_path, engine='openpyxl')
            
            if cols_count == 0:
                cols_count = len(df.columns)
            else:
                if len(df.columns) != cols_count:
                    # logger.warning(f"Column count mismatch in file {file} for table {table_name}. Expected {cols_count} columns, but found {len(df.columns)} columns. Adjusting to match existing columns.")
                    raise ValueError(f"Column count mismatch in file {file} for table {table_name}. Expected {cols_count} columns, but found {len(df.columns)} columns.")
                    # old_cols = dfs[-1].columns.tolist() if dfs else None
                    # if old_cols:
                    #     # Check old cols is less than new cols
                    #     if len(old_cols) < len(df.columns):
                    #         available_cols = set(old_cols).intersection(set(df.columns))
                    #         df = df[list(available_cols)]
                    #     else:
                    #         available_cols = set(df.columns).intersection(set(old_cols))
                    #         df = df[list(available_cols)]

            df.columns = [str(c).strip() for c in df.columns]
            logger.info(f"Successfully read data for table {table_name} from file {file}, shape: {df.shape}")
            if df.duplicated().any():
                logger.warning(f"Number of duplicate rows in file {file} for table {table_name}: {df.duplicated().sum()}")
                logger.warning(f"Duplicate rows found in file {file} for table {table_name}.")
                df.drop_duplicates(inplace=True,keep='first')
                logger.info(f"Shape after dropping duplicates in file {file} for table {table_name}: {df.shape}")
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error reading file {file} for table {table_name}: {e}")
            
    if not dfs:
        return None
        
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"After combining, shape for table {table_name}: {combined_df.shape}")
    combined_df.drop_duplicates(inplace=True,keep='first')
    logger.info(f"Combined shape for table after dropping duplicate rows {table_name}: {combined_df.shape}")
    return combined_df
