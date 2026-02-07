import pandas as pd
import numpy as np
from typing import Optional

from .vim_rename import VIM_RENAME_MAPPING
from .vim_t100t_rename import VIM_T100T_RENAME_MAPPING
from .vim_t101t_rename import VIM_T101_RENAME_MAPPING
from ..logger_config import get_logger

# 
# logger = logging.getLogger(__name__)

def generate_vim_apr_log_data(vim_apr_log:pd.DataFrame)-> pd.DataFrame:
    """Generate VIM APR LOG DataFrame with renamed columns.
      
    Args:
        vim_apr_log (pd.DataFrame): Raw VIM APR LOG DataFrame.
    Returns:
        pd.DataFrame: Processed VIM APR LOG DataFrame with renamed columns.
        """
    logger = get_logger()
    # These tables are always in expected list, so None won't happen after validation
    # But keep defensive check
    if vim_apr_log is None:
        raise ValueError("VIM APR LOG DataFrame must be provided - data loading failed")
    
    if vim_apr_log.empty:
        raise ValueError("VIM APR LOG DataFrame is empty - data loading failed")
    
    logger.info(f'Initial VIM APR LOG shape: {vim_apr_log.shape}')
    from .vim_apr_logg_rename import VIM_APR_LOGG_RENAME_DICT
    vim_apr_log.rename(columns=VIM_APR_LOGG_RENAME_DICT,inplace=True)

    logger.info(f"Shape after renaming VIM APR LOG columns: {vim_apr_log.shape}")

    logger.info(f"Null value counts in VIM APR LOG after renaming:")
    null_vals = vim_apr_log.isna().sum()
    logger.info(f"{null_vals[null_vals > 0]}")

    # Filter only rows where App.Action is 'A'
    logger.info(f"Shape before filtering VIM APR LOG for App.Action 'A': {vim_apr_log.shape}")
    vim_apr_log = vim_apr_log[vim_apr_log['APP_ACTION_APR'] == 'A']
    logger.info(f"Shape after filtering VIM APR LOG for App.Action 'A': {vim_apr_log.shape}")

    return vim_apr_log

def generate_vim_1log_data(vim_1log:pd.DataFrame)-> pd.DataFrame:
    """Generate VIM 1LOG DataFrame with renamed columns.
      
    Args:
        vim_1log (pd.DataFrame): Raw VIM 1LOG DataFrame.
    Returns:
        pd.DataFrame: Processed VIM 1LOG DataFrame with renamed columns.
        """
    logger = get_logger()
    # These tables are always in expected list, so None won't happen after validation
    # But keep defensive check
    if vim_1log is None:
        raise ValueError("VIM 1LOG DataFrame must be provided - data loading failed")
    
    if vim_1log.empty:
        raise ValueError("VIM 1LOG DataFrame is empty - data loading failed")
    
    logger.info(f'Initial VIM 1LOG shape: {vim_1log.shape}')
    from .vim_1log_rename import VIM_1LOG_RENAME_MAPPINGS
    vim_1log.rename(columns=VIM_1LOG_RENAME_MAPPINGS,inplace=True)


    return vim_1log


def generate_vim_8log_data(vim_8log:pd.DataFrame)-> pd.DataFrame:
    """Generate VIM 8LOG DataFrame with renamed columns.
      
    Args:
        vim_8log (pd.DataFrame): Raw VIM 8LOG DataFrame.
    Returns:
        pd.DataFrame: Processed VIM 8LOG DataFrame with renamed columns.
        """
    logger = get_logger()
    # These tables are always in expected list, so None won't happen after validation
    # But keep defensive check
    if vim_8log is None:
        raise ValueError("VIM 8LOG DataFrame must be provided - data loading failed")
    
    if vim_8log.empty:
        raise ValueError("VIM 8LOG DataFrame is empty - data loading failed")
    
    logger.info(f'Initial VIM 8LOG shape: {vim_8log.shape}')
    from .vim_8log_rename import VIM_8LOG_RENAME_MAPPINGS
    vim_8log.rename(columns=VIM_8LOG_RENAME_MAPPINGS,inplace=True)


    return vim_8log



def generate_vim_1log_comm_data(vim_1log_comm:pd.DataFrame)-> pd.DataFrame:
    """Generate VIM 1LOGCOMM DataFrame with renamed columns.
      and grouped comments by Document ID.
      
    Args:
        vim_1log_comm (pd.DataFrame): Raw VIM 1LOGCOMM DataFrame.
    Returns:
        pd.DataFrame: Processed VIM 1LOGCOMM DataFrame with renamed columns and grouped comments.
        """
    logger = get_logger()
    # Handle None - not needed for this pipeline mode (AP flow)
    if vim_1log_comm is None:
        logger.info("VIM 1LOGCOMM not required for this pipeline mode, returning empty DataFrame")
        return pd.DataFrame()  # Return empty DataFrame to skip processing
    
    if vim_1log_comm.empty:
        raise ValueError("VIM 1LOGCOMM DataFrame was expected but is empty - data loading failed")
    
    logger.info(f'Initial VIM 1LOGCOMM shape: {vim_1log_comm.shape}')
    from .vim_1log_comm_rename import VIM_1LOGCOMM_RENAME_MAP
    vim_1log_comm.rename(columns=VIM_1LOGCOMM_RENAME_MAP,inplace=True)

    # Groupby and Create columns

    # Group by Client and Document ID, takes all comments and join with a new line character
    vim_1log_comm_grouped = vim_1log_comm.groupby(['CLIENT','DOCUMENT_ID'],as_index=False).agg({
        '1LOG_COMMENTS': lambda x: '\n'.join(x.dropna().astype(str))
    })

    # Create a new dataframe with 1 row for each unique (Client, Document ID) pair with grouped comments
    return vim_1log_comm_grouped


def generate_vim_8log_comm_data(vim_8log_comm:pd.DataFrame)-> pd.DataFrame:
    """Generate VIM 8LOGCOMM DataFrame with renamed columns.
      and grouped comments by Object Type and Object Key.
      
    Args:
        vim_8log_comm (pd.DataFrame): Raw VIM 8LOGCOMM DataFrame.
    Returns:
        pd.DataFrame: Processed VIM 8LOGCOMM DataFrame with renamed columns and grouped comments.
        """
    logger = get_logger()
    # Handle None - not needed for this pipeline mode (AP flow)
    if vim_8log_comm is None:
        logger.info("VIM 8LOGCOMM not required for this pipeline mode, returning empty DataFrame")
        return pd.DataFrame()  # Return empty DataFrame to skip processing
    
    if vim_8log_comm.empty:
        raise ValueError("VIM 8LOGCOMM DataFrame was expected but is empty - data loading failed")
    
    logger.info(f'Initial VIM 8LOGCOMM shape: {vim_8log_comm.shape}')
    from .vim_8log_comm_rename import VIM_8LOGCOMM_RENAME_MAP
    vim_8log_comm.rename(columns=VIM_8LOGCOMM_RENAME_MAP,inplace=True)

    # Groupby and Create columns

    # Group by Client, Object Type and Object Key, takes all comments and join with a new line character
    vim_8log_comm_grouped = vim_8log_comm.groupby(['CLIENT','8LOG_OBJECT_TYPE','8LOG_OBJECT_KEY'],as_index=False).agg({
        '8LOG_COMMENTS': lambda x: '\n'.join(x.dropna().astype(str).unique())
    })

    # Create a new dataframe with 1 row for each unique (Client, Object Type, Object Key) pair with grouped comments
    return vim_8log_comm_grouped




def merge_vim_with_vim_t100t_and_t101(vim_data:pd.DataFrame,
                                        vim_t100t:pd.DataFrame,
                                        vim_t101:pd.DataFrame)-> pd.DataFrame:
        """
       Merge VIM header (/OPT/VIM_1HEAD) with VIM_T100T (document type) and
        VIM_T101T (status description) tables.

        Args:
            vim_data (pd.DataFrame): /OPT/VIM_1HEAD VIM document header data.
            vim_t100t (pd.DataFrame): VIM_T100T text table with document type descriptions.
            vim_t101 (pd.DataFrame): VIM_T101T text table with document status descriptions.

        Returns:
            pd.DataFrame: Enriched VIM header DataFrame with human-readable type and status fields.

        """
        logger = get_logger()
        if vim_data is None or vim_t100t is None or vim_t101 is None:
            raise ValueError("VIM Data, VIM T100T, and VIM T101 DataFrames must be provided for merging.")
        
        if vim_data.empty or vim_t100t.empty or vim_t101.empty:
            raise ValueError("VIM Data, VIM T100T, and VIM T101 DataFrames cannot be empty.")
        
        logger.info(f'Initial VIM Data shape: {vim_data.shape}')
        logger.info(f'Initial VIM T100T shape: {vim_t100t.shape}')
        logger.info(f'Initial VIM T101 shape: {vim_t101.shape}')

        
        # Rename all columns based on the provided mappings
        logger.info('Renaming VIM Data columns....')
        vim_data.rename(columns=VIM_RENAME_MAPPING, inplace=True)
        logger.info('Renaming VIM T100T columns....')
        vim_t100t.rename(columns=VIM_T100T_RENAME_MAPPING, inplace=True)
        logger.info('Renaming VIM T101 columns....')
        vim_t101.rename(columns=VIM_T101_RENAME_MAPPING, inplace=True)

        logger.info('Mapping VIM_DP_TRANSACTION_EVENT columns int to string')
        vim_dp_transaction_event_map = {1: 'INVOICE',
                                       2: 'CREDIT MEMO',
                                        3: 'SUBSEQUENT CREDIT',
                                        4: 'SUBSEQUENT DEBIT'}
        
        if 'VIM_DP_TRANSACTION_EVENT' in vim_data.columns:
            vim_data['VIM_DP_TRANSACTION_EVENT'] = vim_data['VIM_DP_TRANSACTION_EVENT'].apply(lambda x: vim_dp_transaction_event_map.get(x,None))
            print("Mapped VIM_DP_TRANSACTION_EVENT values in VIM Data.",vim_data['VIM_DP_TRANSACTION_EVENT'].value_counts(dropna=False))
        else:
            print("Warning: 'VIM_DP_TRANSACTION_EVENT' column not found in VIM Data for mapping.")

        # Value counts for vim_dp_transaction_event after mapping
        logger.info("Value counts for VIM_DP_TRANSACTION_EVENT after mapping:")
        logger.info(f"{vim_data['VIM_DP_TRANSACTION_EVENT'].value_counts(dropna=False)}")

        vim_t100t_keys = ['CLIENT','VIM_DP_DOCUMENT_TYPE']
        vim_t101t_keys = ['CLIENT','VIM_DOCUMENT_STATUS']

        for key in vim_t100t_keys:
            if key not  in vim_data.columns or key not in vim_t100t.columns:
                raise KeyError(f"Missing join key {key} in either VIM Data or VIM T100T.")
            
        for key in vim_t101t_keys:
            if key not  in vim_data.columns or key not in vim_t101.columns:
                raise KeyError(f"Missing join key {key} in either VIM Data or VIM T101.")

        # Drop duplicates in lookup tables to ensure clean merges
        dup_mask_t100t = vim_t100t.duplicated(subset=['CLIENT','VIM_DP_DOCUMENT_TYPE'], keep=False)
        dup_rows_t100t = vim_t100t[dup_mask_t100t]
        if not dup_rows_t100t.empty:
            logger.warning('Duplicate rows found in VIM T100T lookup table based on CLIENT and VIM_DP_DOCUMENT_TYPE:')
            logger.warning(f'Duplicate rows: {dup_rows_t100t}')
            logger.info(f'Dropping duplicate rows in VIM T100T lookup table, keeping first occurrence. Shape: {vim_t100t.shape}')
            vim_t100t = vim_t100t.drop_duplicates(subset=['CLIENT','VIM_DP_DOCUMENT_TYPE'], keep='first')
            logger.info(f'After dropping duplicates, VIM T100T shape: {vim_t100t.shape}')
        else:
            logger.info("No duplicates found in VIM T100T based on (CLIENT, VIM_DP_DOCUMENT_TYPE).")

        dup_mask_t101 = vim_t101.duplicated(subset=['CLIENT','VIM_DOCUMENT_STATUS'], keep=False)
        dup_rows_t101 = vim_t101[dup_mask_t101]
        if not dup_rows_t101.empty:
            logger.warning('Duplicate rows found in VIM T101 lookup table based on CLIENT and VIM_DOCUMENT_STATUS:')
            logger.warning(f'Duplicate rows: {dup_rows_t101}')
            logger.info(f'Dropping duplicate rows in VIM T101 lookup table, keeping first occurrence. Shape: {vim_t101.shape}')
            vim_t101 = vim_t101.drop_duplicates(subset=['CLIENT','VIM_DOCUMENT_STATUS'], keep='first')
            logger.info(f'After dropping duplicates, VIM T101 shape: {vim_t101.shape}')
        else:
            logger.info("No duplicates found in VIM T101 based on (CLIENT, VIM_DOCUMENT_STATUS).")


        if 'VIM_DOCUMENT_STATUS' in vim_data.columns:
            vim_data['VIM_DOCUMENT_STATUS'] = vim_data['VIM_DOCUMENT_STATUS'].astype(str).str.strip()
            vim_t101['VIM_DOCUMENT_STATUS'] = vim_t101['VIM_DOCUMENT_STATUS'].astype(str).str.strip()

        if 'VIM_DP_DOCUMENT_TYPE' in vim_data.columns:
            vim_data['VIM_DP_DOCUMENT_TYPE'] = vim_data['VIM_DP_DOCUMENT_TYPE'].astype(str).str.strip()
            vim_t100t['VIM_DP_DOCUMENT_TYPE'] = vim_t100t['VIM_DP_DOCUMENT_TYPE'].astype(str).str.strip()



        logger.debug("Data types of join keys in VIM Data after type conversion:")
        logger.debug(f'{vim_data[vim_t100t_keys].dtypes}')
        logger.debug("Data types of join keys in VIM T100T after type conversion:")
        logger.debug(f'{vim_t100t[vim_t100t_keys].dtypes}')
        logger.debug("Data types of join keys in VIM T101 after type conversion:")
        logger.debug(f'{vim_t101[vim_t101t_keys].dtypes}')

        # Merge VIM Data with VIM T100T
        vim_with_t100_merged = vim_data.merge(vim_t100t, on=vim_t100t_keys, how='left', suffixes=('_VIMData','_VIMT100T'))
        logger.info(f'After merging VIM Data with VIM T100T, merged shape: {vim_with_t100_merged.shape}')
        
        # Merge the above result with VIM T101
        logger.debug("Data types of join keys in VIM Data with T100 merged after type conversion:")
        logger.debug(f'{vim_with_t100_merged[vim_t101t_keys].dtypes}')
        logger.debug("Data types of join keys in VIM T101 after type conversion:")
        logger.debug(f'{vim_t101[vim_t101t_keys].dtypes}')

        # Merge coverage check
        pre_rows = vim_data.shape[0]
        post_rows = vim_with_t100_merged.shape[0]
        matched = vim_with_t100_merged['VIM_DOC_TYPE_DESC'].notna()
        match_pct = 100 * matched.sum() / pre_rows
        logger.info(f"MERGE COVERAGE: VIM_DATA + VIM_T100T: {match_pct:.2f}% of line items found matching document type without null values.")

        merged = vim_with_t100_merged.merge(vim_t101, on=vim_t101t_keys, how='left', suffixes=('_VIMWithT100','_VIMT101'))
        logger.info(f'After merging with VIM T101, final merged shape: {merged.shape}')

        post_merge_shape = merged.shape
        if vim_data.shape[0] != post_merge_shape[0]:
            raise ValueError("Merge altered number of rows in VIM DataFrame, indicating a potential issue with join keys.")

        # Check coverage of merges
        missing_doctype_desc=merged['VIM_DOC_TYPE_DESC'].isna().sum()
        logger.info(f"Number of missing VIM_DOC_TYPE_DESC after merge: {missing_doctype_desc}")
        missing_status_desc=merged['VIM_DOC_STATUS_DESC'].isna().sum()
        logger.info(f"Number of missing VIM_DOC_STATUS_DESC after merge: {missing_status_desc}")

        # Merge coverage check
        pre_rows = vim_with_t100_merged.shape[0]
        post_rows = merged.shape[0]
        matched = merged['VIM_DOC_STATUS_DESC'].notna()
        match_pct = 100 * matched.sum() / pre_rows
        logger.info(f"MERGE COVERAGE: VIM_WITH_T100 + VIM_T101: {match_pct:.2f}% of line items found matching document status without null values.")

        # print('vim full merged columns:', merged.columns.tolist())
        return merged



def merge_invoice_line_item_with_vim_data(invoice_line_item:pd.DataFrame,
                                          vim_data:Optional[pd.DataFrame],
                                          vim_t100t:Optional[pd.DataFrame],
                                          vim_t0101:Optional[pd.DataFrame],
                                          vim_1log_comm:Optional[pd.DataFrame],
                                          vim_8log_comm:Optional[pd.DataFrame],
                                          vim_1log:Optional[pd.DataFrame],
                                          vim_8log:Optional[pd.DataFrame],
                                          vim_apr_log:Optional[pd.DataFrame])-> pd.DataFrame:
                                     
    """
    Merge Invoice Line Item DataFrame with VIM Data DataFrame on key identifiers.
    Args:
        invoice_line_item (pd.DataFrame): Invoice Line Item DataFrame.
        vim_data (pd.DataFrame): VIM Data DataFrame.
        vim_t100t (pd.DataFrame): VIMT100T table DataFrame with VIM text descriptions.
        vim_t0101 (pd.DataFrame): VIMT101 table DataFrame with additional VIM details.

    Returns:
        pd.DataFrame: Merged DataFrame containing invoice line item data enriched with VIM data.
    """
    logger = get_logger()
    # Validate required DataFrames
    if invoice_line_item is None or invoice_line_item.empty:
        raise ValueError("Invoice Line Item DataFrame must be provided and cannot be empty")
    
    if vim_data is None or vim_data.empty:
        raise ValueError("VIM Data DataFrame must be provided and cannot be empty")
    
    if vim_t100t is None or vim_t100t.empty:
        raise ValueError("VIM T100T DataFrame must be provided and cannot be empty")
    
    if vim_t0101 is None or vim_t0101.empty:
        raise ValueError("VIM T101 DataFrame must be provided and cannot be empty")
    
    # Handle optional LOGCOMM tables - None means not needed for this pipeline mode
    # These will be checked in the generate functions
    
    
    # First, merge VIM Data with its lookup tables
    vim_full_merged = merge_vim_with_vim_t100t_and_t101(vim_data=vim_data,
                                                         vim_t100t=vim_t100t,
                                                         vim_t101=vim_t0101)
    
    logger.info(f'Merged data columns after VIM full merge: {vim_full_merged.columns.tolist()}')
    logger.info(f'Shape of data after VIM full merge: {vim_full_merged.shape}')

    # Get vim 1LOG and 8LOG data
    logger.info("Generating VIM 1LOG data...")
    vim_1log_data = generate_vim_1log_data(vim_1log)
    logger.info("Generating VIM 8LOG data...")
    vim_8log_data = generate_vim_8log_data(vim_8log)
    logger.info(f"Generating VIM APR LOG data...")
    vim_apr_log_data = generate_vim_apr_log_data(vim_apr_log)

    # no of acc docs where comments exist for 1LOG
    vim_1log_comments_doc_ids = vim_1log_data[vim_1log_data['VIM_1LOG_COMMENTS_EXIST']=='X']['VIM_1LOG_DOCUMENT_ID'].unique()
    vim_8log_comments_doc_ids = vim_8log_data[vim_8log_data['VIM_8LOG_COMMENTS_EXIST']=='X']['VIM_8LOG_DOCUMENT_LOG_ID'].unique()
    logger.info(f'Number of unique VIM Document IDs with comments in 1LOG: {len(vim_1log_comments_doc_ids)}')
    logger.info(f'Number of unique VIM Document IDs with comments in 8LOG: {len(vim_8log_comments_doc_ids)}')

    # Get 1 log and 8 log comments data
    logger.info("Generating VIM 1LOGCOMM grouped data...")  
    vim_1log_comm_data = generate_vim_1log_comm_data(vim_1log_comm)
    logger.info("Generating VIM 8LOGCOMM grouped data...")
    vim_8log_comm_data = generate_vim_8log_comm_data(vim_8log_comm)

    # Only process LOGCOMM merges if data is available (z-block mode)
    if not vim_1log_comm_data.empty:
        # Check datatype of Document ID columns for merging
        logger.info(f'VIM data doc types: {vim_full_merged[['CLIENT','VIM_DOCUMENT_ID']].dtypes}')
        logger.info(f'VIM 1LOGCOMM doc types: {vim_1log_comm_data[["CLIENT","DOCUMENT_ID"]].dtypes}')

        vim_full_merged['VIM_DOCUMENT_ID'] = vim_full_merged['VIM_DOCUMENT_ID'].astype(int)
        vim_1log_comm_data['DOCUMENT_ID'] = vim_1log_comm_data['DOCUMENT_ID'].astype(int)

        # Merge 1LOG comments into VIM full merged data
        logger.info("Merging VIM 1LOGCOMM data into VIM full merged data...")
        vim_1log_lookup_dict = vim_1log_comm_data.set_index(['CLIENT','DOCUMENT_ID'])['1LOG_COMMENTS'].to_dict()

        # Use lookup dict to map comments in vim_full_merged
        vim_full_merged['VIM_1LOG_COMMENTS'] = vim_full_merged.apply(
            lambda row: vim_1log_lookup_dict.get((row['CLIENT'], row['VIM_DOCUMENT_ID']), np.nan), axis=1
        )

        logger.info("Null value counts in VIM_1LOG_COMMENTS after merge:")
        logger.info(f"{vim_full_merged['VIM_1LOG_COMMENTS'].isna().sum()} nulls out of {len(vim_full_merged)} rows")

        # After merging,no of acc docs where comments exist for 1LOG
        acc_docs_with_1log_comments = vim_full_merged[vim_full_merged['VIM_1LOG_COMMENTS'].notna()]['VIM_DOCUMENT_ID'].nunique()
        logger.info(f"Number of unique VIM Document IDs with VIM 1LOG comments after merge: {acc_docs_with_1log_comments}")
    else:
        logger.info("VIM 1LOGCOMM data not available, skipping 1LOG comments merge")
        vim_full_merged['VIM_1LOG_COMMENTS'] = np.nan

    # Process 8LOG comments if available
    if not vim_8log_comm_data.empty:
        # Create a dict look up for 8 log comments
        vim_8log_lookup_dict = vim_8log_comm_data.set_index(['CLIENT','8LOG_OBJECT_KEY'])['8LOG_COMMENTS'].to_dict()

        vim_full_merged['VIM_8LOG_COMMENTS'] = vim_full_merged.apply(
            lambda row: vim_8log_lookup_dict.get((row['CLIENT'], row['VIM_OBJECT_KEY']), np.nan), axis=1
        )
    else:
        logger.info("VIM 8LOGCOMM data not available, skipping 8LOG comments merge")
        vim_full_merged['VIM_8LOG_COMMENTS'] = np.nan

    # Merge VIM APR LOG df into vim_full_merged data
    logger.info("Merging VIM APR LOG data into VIM full merged data...")
    apr_log_keys = ['CLIENT','VIM_DOCUMENT_ID_APR']
    vim_keys = ['CLIENT','VIM_DOCUMENT_ID']
    for key in apr_log_keys:
        if key not  in vim_apr_log_data.columns:
            raise KeyError(f"Missing join key {key} in VIM APR LOG data.")
        
    for key in vim_keys:
        if key not  in vim_full_merged.columns:
            raise KeyError(f"Missing join key {key} in VIM full merged data.")
    
    # Check doc type of key columns
    logger.info(f'VIM APR LOG doc types: {vim_apr_log_data[apr_log_keys].dtypes}')
    logger.info(f'VIM full merged doc types: {vim_full_merged[vim_keys].dtypes}')

    vim_full_merged = pd.merge(vim_full_merged, vim_apr_log_data,
                               left_on=vim_keys, right_on=apr_log_keys, how='left', suffixes=('_VIMData', '_VIMAprLog'))
    
    # Merge coverage check
    pre_rows = vim_full_merged.shape[0]
    post_rows = vim_full_merged.shape[0]
    matched = vim_full_merged['VIM_DOCUMENT_ID_APR'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: VIM_FULL_MERGED + VIM_APR_LOG: {match_pct:.2f}% of line items found matching VIM APR LOG document ID without null values.")

    vim_full_merged['VIM_COMMENTS'] = vim_full_merged['VIM_1LOG_COMMENTS'].fillna('').astype(str)\
          + '\n' + vim_full_merged['VIM_8LOG_COMMENTS'].fillna('').astype(str) +\
              '\n' + vim_full_merged['COMMENTS_APR'].fillna('').astype(str)

    logger.info("Null value counts in VIM_COMMENTS after comments")
    logger.info(f"{vim_full_merged['VIM_COMMENTS'].isna().sum()} nulls out of {len(vim_full_merged)} rows")

    invoice_data_keys = ['CLIENT','COMPANY_CODE','FISCAL_YEAR','VIM_OBJECT_KEY']
    vim_data_keys = ['CLIENT','COMPANY_CODE','FISCAL_YEAR','VIM_OBJECT_KEY']
    for key in invoice_data_keys:
        if key not  in invoice_line_item.columns:
            raise KeyError(f"Missing join key {key} in either Invoice Line Item or VIM Data.")
        
    for key in vim_data_keys:
        if key not  in vim_full_merged.columns:
            raise KeyError(f"Missing join key {key} in either Invoice Line Item or VIM Data.")
        

    invoice_line_item['VIM_OBJECT_KEY'] = invoice_line_item['VIM_OBJECT_KEY'].astype(int)
    vim_full_merged['VIM_OBJECT_KEY'] = vim_full_merged['VIM_OBJECT_KEY'].astype(int)

    logger.debug("Data types of join keys in Invoice Line Item before merge:")
    logger.debug(f'{invoice_line_item[invoice_data_keys].dtypes}')
    logger.debug("Data types of join keys in VIM full merged before merge:")
    logger.debug(f'{vim_full_merged[vim_data_keys].dtypes}')


    pre_rows = len(invoice_line_item)   
    final_merged = invoice_line_item.merge(vim_full_merged, left_on=invoice_data_keys, right_on=vim_data_keys, how='left', suffixes=('_Invoice','_VIMData'))
    logger.info(f'After merging Invoice Line Item with VIM Data, final merged shape: {final_merged.shape}')
    post_rows = len(final_merged)

    if pre_rows != post_rows:
        logger.warning(f"Row count changed after merge: before={pre_rows}, after={post_rows}")
        raise ValueError("Row count mismatch after merging Invoice Line Item with VIM Data.")
    
    # Fill empty DP_TRANSACTION_EVENT with values based on DEBIT_CREDIT_INDICATOR_HEADER_LEVEL

    # Null vlaue count for DP_TRANSACTION_EVENT before fill
    null_count_before = final_merged['VIM_DP_TRANSACTION_EVENT'].isna().sum()
    logger.info(f"Null value count for VIM_DP_TRANSACTION_EVENT before fill: {null_count_before}")

    final_merged['VIM_DP_TRANSACTION_EVENT'] = final_merged['VIM_DP_TRANSACTION_EVENT'].fillna(
        final_merged['DEBIT_CREDIT_INDICATOR_HEADER_LEVEL'].map({
            'H': 'INVOICE',
            'S': 'CREDIT MEMO'
        })
    )

    # Null vlaue count for DP_TRANSACTION_EVENT after fill
    null_count_after = final_merged['VIM_DP_TRANSACTION_EVENT'].isna().sum()
    logger.info(f"Null value count for VIM_DP_TRANSACTION_EVENT after fill: {null_count_after}")
    # Value counts after fill
    logger.info("Value counts for VIM_DP_TRANSACTION_EVENT after fill:")
    logger.info(f"{final_merged['VIM_DP_TRANSACTION_EVENT'].value_counts(dropna=False)}")


    # Diagnosis of merged data
    matched = final_merged['VIM_DP_DOCUMENT_TYPE'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"merge_invoice_line_item_with_vim_data: match coverage={match_pct:.2f}% ({matched.sum()} of {pre_rows})")
    

    # Check coverage at account document level
    final_merged['unique_id'] = (
        final_merged['CLIENT'].astype(str) + '_' +
        final_merged['COMPANY_CODE'].astype(str) + '_' +
        final_merged['FISCAL_YEAR'].astype(str) + '_' +
        final_merged['REFERENCE_KEY'].astype(str)
    )
    account_doc_ids = final_merged['unique_id'].unique()
    logger.info(f"Total unique account document IDs in Invoice Line Item: {len(account_doc_ids)}")

    merged_account_doc_ids = final_merged[final_merged['VIM_DP_DOCUMENT_TYPE'].notna()]['unique_id'].unique()
    logger.info(f"Unique account document IDs with VIM match: {len(merged_account_doc_ids)}")

    
    missing_status = final_merged['VIM_DOC_STATUS_DESC'].isna().mean() * 100 if 'VIM_DOC_STATUS_DESC' in final_merged.columns else 0
    missing_type = final_merged['VIM_DOC_TYPE_DESC'].isna().mean() * 100 if 'VIM_DOC_TYPE_DESC' in final_merged.columns else 0
    logger.debug(f"Missing type desc={missing_type:.2f}%, status desc={missing_status:.2f}%")

    logger.info(f"Final merged shape: {final_merged.shape}")

    # No of acc docs where VIM_COMMENTS is not null
    acc_docs_with_comments = final_merged[final_merged['VIM_COMMENTS'].notna()]['unique_id'].nunique()
    logger.info(f"Number of unique account document IDs with VIM comments: {acc_docs_with_comments}")

    # Merge coverage check
    pre_rows = invoice_line_item.shape[0]
    post_rows = final_merged.shape[0]
    matched = final_merged['VIM_DP_DOCUMENT_TYPE'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_LINE_ITEM + VIM_DATA: {match_pct:.2f}% of line items found matching VIM document type without null values.")



    return final_merged
