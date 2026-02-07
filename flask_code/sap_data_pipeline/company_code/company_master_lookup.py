import pandas as pd
from typing import Optional
from .t001_rename import T001_RENAME_MAP

def merge_t001(invoice_df:pd.DataFrame,
                t001:Optional[pd.DataFrame])-> pd.DataFrame:
    """Merge invoice DataFrame with T001 DataFrame on key company code identifiers.
    Args:
        invoice_df (pd.DataFrame): Invoice DataFrame.
        t001 (pd.DataFrame | None): T001 table DataFrame with company code details, or None if not needed.
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice data enriched with company code info. 
    """ 
    from ..logger_config import get_logger
    logger = get_logger()
    logger.info("Invoice + Company Master T001 Merge Start")

    # Handle None - not needed for this pipeline mode
    if t001 is None:
        logger.info("T001 not required for this pipeline mode, skipping merge")
        return invoice_df
    
    # Validate invoice_df
    if invoice_df is None or invoice_df.empty:
        raise ValueError("Invoice DataFrame must be provided and cannot be empty")
    
    # Validate t001 - if provided, must not be empty
    if t001.empty:
        raise ValueError("T001 DataFrame was expected but is empty - data loading failed")
    
    logger.info(f'Shape of Invoice Data: {invoice_df.shape}')
    logger.info(f'Shape of T001 Data: {t001.shape}')

    logger.info("Renaming T001 columns for consistency...")
    t001.rename(columns=T001_RENAME_MAP, inplace=True)

    # Create a new column called LE_ADDRESS by combining address related  fields, filling missing values with empty strings
    t001['LE_ADDRESS'] = t001[['LE_STREET', 'LE_CITY', 'LE_POSTAL_CODE', 'LE_COUNTRY']].fillna('').astype(str).agg(', '.join, axis=1).str.strip(', ')

    logger.info('Null values in T001 df')
    null_t001_data = t001.isna().sum()
    logger.debug(f'Null value counts:\n{null_t001_data[null_t001_data > 0]}')

    # This line replaces any LE_ADDRESS values that consist only of commas and optional whitespace with an empty string
    t001['LE_ADDRESS'] = t001['LE_ADDRESS'].replace(r'^\s*,\s*$', '', regex=True)

    # value counts for LE_ADDRESS
    logger.debug(f"Value counts for LE_ADDRESS in T001:\n{t001['LE_ADDRESS'].value_counts(dropna=False)}")

    keys = ['CLIENT','COMPANY_CODE']
    for k in keys:
        if k not in invoice_df.columns or k not in t001.columns:
            raise KeyError(f"Missing join key {k} in either Invoice or T001.")
        
    dup_mask = t001.duplicated(subset=keys,keep=False)
    dup_rows = t001[dup_mask]

    # print data types of the keys
    logger.debug(f"Data types of join keys in T001:\n{t001[keys].dtypes}")
    logger.debug(f"Data types of join keys in Invoice:\n{invoice_df[keys].dtypes}")

    if not dup_rows.empty:
        logger.warning("T001 has duplicate (Client, Company Code) records — investigate possible extraction issue.")
        logger.debug(f"Duplicate rows:\n{dup_rows}")
        logger.info(f'Before dropping duplicates, keeping last value, T001 shape: {t001.shape}')
        t001 = t001.drop_duplicates(subset=keys, keep='last')
        logger.info(f'After dropping duplicates, T001 shape: {t001.shape}')
    else:
        logger.info("No duplicates found in T001 based on (Client, Company Code).")
    pre_merge_shape = invoice_df.shape
    merged = pd.merge(invoice_df, t001, how='left', on=keys, suffixes=('_Invoice', '_T001'))
    post_merge_shape = merged.shape
    if pre_merge_shape[0] != post_merge_shape[0]:
        raise ValueError("Merge altered number of rows in invoice DataFrame, indicating a potential issue with join keys.")
    
    # Null check after merge for new columns from T001
    new_columns = set(merged.columns) - set(invoice_df.columns)
    for col in new_columns:
        null_count = merged[col].isna().sum()
        if null_count > 0:
            logger.info(f"Post-merge, column '{col}' has {null_count} null values.")
    
    logger.info(f"Invoice and T001 merged. Shape: {merged.shape}")

    # Merge coverage check
    pre_rows = invoice_df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['LE_NAME'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + T001: {match_pct:.2f}% of line items found matching header  without null values.")
    return merged
