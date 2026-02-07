import pandas as pd
from typing import Optional
from .t053_rename import T053_RENAME_MAP

def merge_t053s(invoice_df:pd.DataFrame,
                 t053s:Optional[pd.DataFrame])-> pd.DataFrame:
    """Merge invoice DataFrame with T053S DataFrame on key payment block/ Reason Code identifiers.
    Args:
        invoice_df (pd.DataFrame): Invoice DataFrame.
        t053s (pd.DataFrame | None): T053S table DataFrame with payment block / Reason code details, or None if not needed.
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice data enriched with payment block info.
    """
    from ..logger_config import get_logger
    logger = get_logger()
    logger.info("Invoice + Payment Block T053S Merge Start")
    
    # Handle None - not needed for this pipeline mode
    if t053s is None:
        logger.info("T053S not required for this pipeline mode, skipping merge")
        return invoice_df
    
    # Validate invoice_df
    if invoice_df is None or invoice_df.empty:
        raise ValueError("Invoice DataFrame must be provided and cannot be empty")
    
    # Validate t053s - if provided, must not be empty
    if t053s.empty:
        raise ValueError("T053S DataFrame was expected but is empty - data loading failed")  
    
    logger.info(f'Shape of Invoice Data: {invoice_df.shape}')
    logger.info(f'Shape of T053S Data: {t053s.shape}')

    logger.info("Renaming T053S columns for consistency...")
    t053s = t053s.rename(columns=T053_RENAME_MAP)

    logger.info('Null values in T053S df')
    null_t053s_data = t053s.isna().sum()
    logger.debug(f'Null value counts:\n{null_t053s_data[null_t053s_data > 0]}')


    keys = ['CLIENT','REASON_CODE']
    for k in keys:
        if k not in invoice_df.columns or k not in t053s.columns:
            raise KeyError(f"Missing join key {k} in either Invoice or T053S.")
        
    dup_mask = t053s.duplicated(subset=keys,keep=False)
    dup_rows = t053s[dup_mask]
    if not dup_rows.empty:
        logger.warning(f"T053S has duplicate (Client, Reason Code) records — investigate possible extraction issue., number of duplicates: {dup_rows.shape[0]}")
        logger.debug(f"Duplicate rows:\n{dup_rows}")
        logger.info(f'Before dropping duplicates, keeping last T053S shape: {t053s.shape}')
        t053s = t053s.drop_duplicates(subset=keys,keep='last').copy()
        logger.info(f'After dropping duplicates, T053S shape: {t053s.shape}')
    else:
        logger.info("No duplicates found in T053S based on (Client, Reason Code).")
    
    # convert REASON_CODE to str before Merging
    invoice_df['REASON_CODE'] = invoice_df['REASON_CODE'].astype(str)
    t053s['REASON_CODE'] = t053s['REASON_CODE'].astype(str)

    logger.debug(f"Data types of join keys in T053S:\n{t053s[keys].dtypes}")
    logger.debug(f"Data types of join keys in Invoice:\n{invoice_df[keys].dtypes}")
    
    pre_merge_shape = invoice_df.shape
    merged = pd.merge(invoice_df, t053s, how='left', on=keys, suffixes=('_Invoice', '_T053S'))
    logger.info(f"Invoice and T053S merged. Shape: {merged.shape}")
    post_merge_shape = merged.shape
    if pre_merge_shape[0] != post_merge_shape[0]:
        raise ValueError("Merge altered number of rows in invoice DataFrame, indicating a potential issue with join keys.")
    
    # Check null value in new columns from T053S
    new_columns = set(merged.columns) - set(invoice_df.columns)
    logger.info("Null values in newly merged T053S columns:")
    for col in new_columns:
        null_count = merged[col].isna().sum()
        if null_count > 0:
            logger.info(f"Column {col} has {null_count} null values after merge.")

    # Merge coverage check
    pre_rows = invoice_df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['REASON_CODE_DESCRIPTION'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + T053S: {match_pct:.2f}% of line items found matching header  without null values.")
            
    return merged
