import pandas as pd
from typing import Optional
from .t052u_rename import T052U_RENAME_MAP
from ..logger_config import get_logger
def merge_t052u(invoice_df:pd.DataFrame,
                t052u: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merge invoice DataFrame with T052U DataFrame on key payment terms identifiers.
    Args:
        invoice_df (pd.DataFrame): Invoice DataFrame.
        t052u (pd.DataFrame | None): T052U table DataFrame with payment terms details, or None if not needed.
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice data enriched with payment terms info. 
    """ 
    logger = get_logger()
    logger.info("\n===  Invoice + Payment Terms T052U Merge Start ===")

    # Handle None - not needed for this pipeline mode
    if t052u is None:
        logger.info("T052U not required for this pipeline mode, skipping merge")
        return invoice_df
    
    # Validate invoice_df
    if invoice_df is None or invoice_df.empty:
        raise ValueError("Invoice DataFrame must be provided and cannot be empty")
    
    # Validate t052u - if provided, must not be empty
    if t052u.empty:
        raise ValueError("T052U DataFrame was expected but is empty - data loading failed")
    
    logger.debug(f'Shape of Invoice Data: {invoice_df.shape}')
    logger.debug(f'Shape of T052U Data: {t052u.shape}')

    logger.info("Renaming T052U columns for consistency...")
    t052u = t052u.rename(columns=T052U_RENAME_MAP)

    logger.debug('Null value in T052U df')
    null_t052u_data = t052u.isna().sum()
    logger.debug(null_t052u_data[null_t052u_data > 0])

    keys = ['CLIENT','PAYMENT_TERMS']
    for k in keys:
        if k not in invoice_df.columns or k not in t052u.columns:
            raise KeyError(f"Missing join key {k} in either Invoice or T052U.")
        
    dup_mask = t052u.duplicated(subset=keys,keep=False)
    dup_rows = t052u[dup_mask]

    if not dup_rows.empty:
        logger.warning("⚠️ T052U has duplicate (Client, Pyt Terms) records — investigate possible extraction issue.")
        logger.warning(dup_rows)
        logger.debug(f'Before dropping duplicates,keeping last value, T052U shape: {t052u.shape}')
        t052u = t052u.drop_duplicates(subset=keys,keep='last').copy()
        logger.debug(f'After dropping duplicates, T052U shape: {t052u.shape}')
    else:
        logger.info("No duplicates found in T052U based on (Client, Pyt Terms).")
    # Payment terms standarised values to uppercase
    t052u['PAYMENT_TERMS'] = t052u['PAYMENT_TERMS'].astype(str).str.upper().str.strip()
    invoice_df['PAYMENT_TERMS'] = invoice_df['PAYMENT_TERMS'].astype(str).str.upper().str.strip()
    
    logger.debug("Data types of join keys in T052U:")
    logger.debug(t052u[keys].dtypes)
    logger.debug("Data types of join keys in Invoice:")
    logger.debug(invoice_df[keys].dtypes)
    
    pre_merge_shape = invoice_df.shape
    merged = pd.merge(invoice_df, t052u, how='left', on=keys, suffixes=('_Invoice', '_T052U'))
    
    post_merge_shape = merged.shape
    if pre_merge_shape[0] != post_merge_shape[0]:
        raise ValueError("Merge altered number of rows in invoice DataFrame, indicating a potential issue with join keys.")
    
    # Check null value in new columns from T052U
    new_columns = set(merged.columns) - set(invoice_df.columns)
    logger.info("Null values in newly merged T052U columns:")
    for col in new_columns:
        null_count = merged[col].isna().sum()
        if null_count > 0:
            logger.info(f"Column {col} has {null_count} null values after merge.")
            logger.debug("Affected (CLIENT, PAYMENT_TERMS) combinations:")
            logger.debug(f'{merged[merged[col].isna()][["CLIENT","PAYMENT_TERMS"]].value_counts()}')

    
    logger.info(f"Invoice and T052U merged. Shape: {merged.shape}")

    # Merge coverage check
    pre_rows = invoice_df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['PAYMENT_TERMS_DESCRIPTION'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + T052U: {match_pct:.2f}% of line items found matching header  without null values.")

    return merged