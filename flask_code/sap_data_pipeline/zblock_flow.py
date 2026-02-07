# test code


z_block_filepath = r"C:\Users\ShriramSrinivasan\Downloads\dow uat iv data\Nov25\Z_TRD403_BKPF_20251125172859.xlsx"

vim_filepath = r"c:\Users\ShriramSrinivasan\Downloads\dow uat iv data\Nov25\TRD403_VIM_20251125193330.xlsx"


import pandas as pd
from .logger_config import get_logger

logger = get_logger()


z_block_bkpf_df = pd.read_excel(z_block_filepath, engine='openpyxl')
vim_df = pd.read_excel(vim_filepath, engine='openpyxl')

logger.info(f"Z_BLOCK BKPF shape: {z_block_bkpf_df.shape}")
logger.info(f"VIM shape: {vim_df.shape}")

logger.info(f"Z_BLOCK BKPF columns: {z_block_bkpf_df.columns.tolist()}")
logger.info(f"VIM columns: {vim_df.columns.tolist()}")

# columns to find unique values - Client , Company Code, Fiscal Year, Document Number

z_block_bkpf_df['unique_id'] = (z_block_bkpf_df['Client'].astype(str) + '_' +
                               z_block_bkpf_df['Company Code'].astype(str).str.zfill(4) + '_' +
                                 z_block_bkpf_df['Fiscal Year'].astype(str) + '_' +
                                      z_block_bkpf_df['Document Number'].astype(str))


vim_df['unique_id'] = (vim_df['Client'].astype(str) + '_' +
                       vim_df['Company Code'].astype(str).str.zfill(4) + '_' + 
                            vim_df['Fiscal Year'].astype(str) + '_' +
                                vim_df['Document Number'].astype(str))


logger.info(f"Z_BLOCK BKPF unique IDs count: {z_block_bkpf_df['unique_id'].nunique()}")
logger.info(f"VIM unique IDs count: {vim_df['unique_id'].nunique()}")

# Find common unique IDs
common_unique_ids = set(z_block_bkpf_df['unique_id']).intersection(set(vim_df['unique_id']))
logger.info(f"Common unique IDs count: {len(common_unique_ids)}")