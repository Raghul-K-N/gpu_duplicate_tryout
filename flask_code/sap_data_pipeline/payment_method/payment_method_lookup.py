import pandas as pd
from typing import Optional
from .t042z_rename import T042Z_RENAME_MAP


def merge_t042z(invoice_df:pd.DataFrame,
                t042z:Optional[pd.DataFrame])-> pd.DataFrame:
    """Merge invoice DataFrame with T042Z DataFrame on key payment method identifiers.
    Args:
        invoice_df (pd.DataFrame): Invoice DataFrame.
        t042z (pd.DataFrame | None): T042Z table DataFrame with payment method details, or None if not needed.
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice data enriched with payment method info. 
    """ 
    from ..logger_config import get_logger
    logger = get_logger()
    logger.info("Invoice + Payment Method T042Z Merge Start")
    
    # Handle None - not needed for this pipeline mode
    if t042z is None:
        logger.info("T042Z not required for this pipeline mode, skipping merge")
        return invoice_df
    
    # Validate invoice_df
    if invoice_df is None or invoice_df.empty:
        raise ValueError("Invoice DataFrame must be provided and cannot be empty")
    
    # Validate t042z - if provided, must not be empty
    if t042z.empty:
        raise ValueError("T042Z DataFrame was expected but is empty - data loading failed")
    
    logger.info(f'Shape of Invoice Data: {invoice_df.shape}')
    logger.info(f'Shape of T042Z Data: {t042z.shape}')

    logger.info("Renaming T042Z columns for consistency...")
    t042z = t042z.rename(columns=T042Z_RENAME_MAP)

   
    logger.info('Null values in T042Z df')
    null_t042z_data = t042z.isna().sum()
    logger.debug(f'T042Z null value counts:\n{null_t042z_data[null_t042z_data > 0]}')

    logger.info('Null values in Invoice df')
    null_invoice_data = invoice_df.isna().sum()
    logger.debug(f'Invoice null value counts:\n{null_invoice_data[null_invoice_data > 0]}')


    t042z_keys = ['CLIENT','COUNTRY','PAYMENT_METHOD']
    invoice_keys = ['CLIENT','LE_COUNTRY','PAYMENT_METHOD']
    
    for k1, k2 in zip(t042z_keys, invoice_keys):
        if k2 not in invoice_df.columns or k1 not in t042z.columns:
            raise KeyError(f"Missing join key {k2} in Invoice or {k1} in T042Z.")
    
    # Check how many countries from invoice are missing in T042Z
    invoice_countries = set(invoice_df['LE_COUNTRY'].unique())
    t042z_countries = set(t042z['COUNTRY'].unique())
    missing_countries = invoice_countries - t042z_countries
    if missing_countries:
        logger.warning(f"Countries in Invoice missing from T042Z: {len(missing_countries)}")
        logger.debug(f"Missing countries: {missing_countries}")
    else:
        logger.info("All countries in Invoice are present in T042Z.")
    dup_mask = t042z.duplicated(subset=t042z_keys,keep=False)
    dup_rows = t042z[dup_mask]
    if not dup_rows.empty:
        logger.warning(f"T042Z has duplicate (Client, Payment Method) records — investigate possible extraction issue., number of duplicates: {dup_rows.shape[0]}")
        logger.debug(f"Duplicate rows:\n{dup_rows}")
        logger.info(f'Before dropping duplicates, T042Z shape: {t042z.shape}')
        t042z = t042z.drop_duplicates(subset=t042z_keys).copy()
        logger.info(f'After dropping duplicates, T042Z shape: {t042z.shape}')
    else:
        logger.info("No duplicates found in T042Z based on (Client, Payment Method).")
    pre_merge_shape = invoice_df.shape

    logger.debug(f"Data types of join keys in T042Z:\n{t042z[t042z_keys].dtypes}")
    logger.debug(f"Data types of join keys in Invoice:\n{invoice_df[invoice_keys].dtypes}")

    # Standarise country and payment method values
    t042z['COUNTRY'] = t042z['COUNTRY'].astype(str).str.upper().str.strip()
    invoice_df['LE_COUNTRY'] = invoice_df['LE_COUNTRY'].astype(str).str.upper().str.strip()
    t042z['PAYMENT_METHOD'] = t042z['PAYMENT_METHOD'].astype(str).str.upper().str.strip()
    invoice_df['PAYMENT_METHOD'] = invoice_df['PAYMENT_METHOD'].astype(str).str.upper().str.strip()

    # Instead of Merging the data, read the t042z data, create a dictionary and map the values, key being Payment Method
    # and the value being Payment Method Description that occurs most frequently for that payment method.
    payment_method_desc_map = t042z.groupby('PAYMENT_METHOD')['PAYMENT_METHOD_DESCRIPTION'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA).to_dict()
    invoice_df['PAYMENT_METHOD_DESCRIPTION'] = invoice_df['PAYMENT_METHOD'].map(payment_method_desc_map)
    
    
    # merged = pd.merge(invoice_df, t042z, how='left', left_on=invoice_keys, right_on=t042z_keys, suffixes=('_Invoice', '_T042Z'))
    merged = invoice_df.copy()
    logger.info(f"Invoice and T042Z merged using mapping. Shape: {merged.shape}")
    # Check for row count consistency
    post_merge_shape = merged.shape
    if pre_merge_shape[0] != post_merge_shape[0]:
        raise ValueError("Merge altered number of rows in invoice DataFrame, indicating a potential issue with join keys.")
    else:
        logger.info("Row count consistent after merge invoice and T042Z.")

    new_columns = set(merged.columns) - set(invoice_df.columns)
    logger.info("Null values in newly merged T042Z columns:")
    for col in new_columns:
        null_count = merged[col].isna().sum()
        if null_count > 0:
            logger.info(f"Column {col} has {null_count} null values after merge.")

    # check merge coverage
    pre_rows = invoice_df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['PAYMENT_METHOD_DESCRIPTION'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + T042Z: {match_pct:.2f}% of line items found matching header  without null values.")

    return merged