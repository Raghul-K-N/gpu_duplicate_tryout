
import pandas as pd
import numpy as np

import dask.dataframe as dd
import os
from .logger_config import get_logger


dow_uat_filepath = r"C:\Users\ShriramSrinivasan\Downloads\dow uat iv data\Nov25"
dow_uat_parquet_filepath = r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\nov-26\dow-uat-parquet"

ap_dupl_flow_filepath = r"C:\Users\ShriramSrinivasan\Downloads\AP duplicate data Nov 26"
ap_dupl_parquet_path = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\nov-26\ap-data-parquet"



# all_files = os.listdir(dow_uat_filepath)
# print(all_files)


# Z_BLOCK_EXPECTED_TABLES = ['BKPF','BSEG','WTH','T003','VIM_','VIMT100','VIMT101','1LOG_','8LOG_',
#                    'APRLOG','EKKO','EKPO','LFA1','LFB1','LFM1','LFBK','T052U','T042Z','T053S','T001','UDC',
#                     'DOAREDEL','VRDOA','1LOGCOMM','8LOGCOMM']


# for each in Z_BLOCK_EXPECTED_TABLES:
#     z_block=True
#     if each == 'BKPF' and z_block:
#         specific_files = [f for f in all_files if each.lower() in f.lower()  and f.lower().startswith('z') ]
#     else: 
#         specific_files = [f for f in all_files if each.lower() in f.lower()]

#     if specific_files:
#         print(f"Files found for table {each}:Count is  {len(specific_files)}, files: {specific_files}")
#         dfs = []
#         for file in specific_files:
#             file_path = os.path.join(dow_uat_filepath, file)
#             try:
#                 df = pd.read_excel(file_path, engine='openpyxl')
#                 dfs.append(df)
#             except Exception as e:
#                 raise Exception(f"Error reading {file_path}: {e}")
            
#             if dfs:
#                 combined_df = pd.concat(dfs, ignore_index=True)
#                 new_filename = file.replace('.xlsx', '.parquet')
#                 parquet_path = os.path.join(dow_uat_parquet_filepath, new_filename)
#                 combined_df.to_parquet(parquet_path, index=False)
#                 print(f"Saved combined parquet for {each} at {parquet_path}")
#     else:
#         print(f"No file found for table {each}.")


all_files = os.listdir(ap_dupl_flow_filepath)
logger = get_logger()
logger.info(f"Files found in directory: {all_files}")

z_block=False

AP_EXPECTED_TABLES = ['BKPF','BSEG','LFA1','LFB1','LFM1','LFBK','T052U','T042Z','T053S','T001','UDC']

for each in AP_EXPECTED_TABLES:
    if each == 'BKPF' and z_block:
        specific_files = [f for f in all_files if each.lower() in f.lower()  and f.lower().startswith('z') ]
    else: 
        specific_files = [f for f in all_files if each.lower() in f.lower() and not f.lower().startswith('z') ]

    if specific_files:
        logger.info(f"Files found for table {each}: Count is {len(specific_files)}, files: {specific_files}")
        dfs = []
        for file in specific_files:
            file_path = os.path.join(ap_dupl_flow_filepath, file)
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                dfs.append(df)
            except Exception as e:
                raise Exception(f"Error reading {file_path}: {e}")
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # Strip whitespace from string columns
                for col in combined_df.columns:
                    if combined_df[col].dtype == 'object':
                        combined_df[col] = combined_df[col].astype(str).str.strip()
                
                new_filename = file.replace('.xlsx', '.parquet')
                parquet_path = os.path.join(ap_dupl_parquet_path, new_filename)
                combined_df.to_parquet(parquet_path, index=False)
                logger.info(f"Saved combined parquet for {each} at {parquet_path}")
