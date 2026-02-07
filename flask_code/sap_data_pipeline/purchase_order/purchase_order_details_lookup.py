import pandas as pd
from typing import Optional

from sap_data_pipeline.data_cleaning import clean_amount_column, clean_date_column
from .ekko_rename import EKKO_RENAME_MAP
from .ekpo_rename import EKPO_RENAME_MAP
import numpy as np


def merge_po_info(invoice_df:pd.DataFrame,ekko_df:Optional[pd.DataFrame],ekpo_df:Optional[pd.DataFrame])-> pd.DataFrame:
    """Merge invoice DataFrame with EKKO and EKPO DataFrames on key purchase order identifiers.
    Args:
        invoice_df (pd.DataFrame): Invoice DataFrame.
        ekko_df (pd.DataFrame | None): EKKO table DataFrame with purchase order header details, or None if not needed.
        ekpo_df (pd.DataFrame | None): EKPO table DataFrame with purchase order item details, or None if not needed.
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice data enriched with purchase order info. 
    """ 
    from ..logger_config import get_logger
    logger = get_logger()
    logger.info("Invoice + Purchase Order Details Merge Start")

    # Handle None - not needed for this pipeline mode
    if ekko_df is None or ekpo_df is None:
        logger.info("PO tables (EKKO/EKPO) not required for this pipeline mode, skipping merge")
        return invoice_df
    
    # Validate invoice_df
    if invoice_df is None or invoice_df.empty:
        raise ValueError("Invoice DataFrame must be provided and cannot be empty")
    
    # Validate PO tables - if provided, must not be empty
    if ekko_df.empty or ekpo_df.empty:
        raise ValueError("EKKO and EKPO DataFrames were expected but are empty - data loading failed")
    
    logger.info(f'Shape of Invoice Data: {invoice_df.shape}')
    logger.info(f'Shape of EKKO Header Data: {ekko_df.shape}')
    logger.info(f'Shape of EKPO line-Item Data: {ekpo_df.shape}')

    logger.info("Renaming EKKO and EKPO columns for consistency...")
    ekko_df = ekko_df.rename(columns=EKKO_RENAME_MAP)
    ekpo_df = ekpo_df.rename(columns=EKPO_RENAME_MAP)

    ekko_df['EXCHANGE_RATE'] = clean_amount_column(ekko_df['EXCHANGE_RATE'])

    ekpo_df['NET_PRICE'] = clean_amount_column(ekpo_df['NET_PRICE'])
    ekpo_df['GROSS_VALUE'] = clean_amount_column(ekpo_df['GROSS_VALUE'])
    ekpo_df['PO_QUANTITY'] = clean_amount_column(ekpo_df['PO_QUANTITY'])

    ekko_df['PURCHASING_DOCUMENT_DATE'] = clean_date_column(ekko_df['PURCHASING_DOCUMENT_DATE'], can_be_null=True)
    ekko_df['CREATED_ON'] = clean_date_column(ekko_df['CREATED_ON'], can_be_null=True)
    
    # Check for null values in EKKO and EKPO data
    null_ekko = ekko_df.isnull().sum()
    null_ekpo = ekpo_df.isnull().sum()
    logger.debug(f"Null values in EKKO columns:\n{null_ekko[null_ekko > 0]}")
    logger.debug(f"Null values in EKPO columns:\n{null_ekpo[null_ekpo > 0]}")

    keys = ['CLIENT','PURCHASE_ORDER_NUMBER' ]
    for k in keys:
        if k not  in ekko_df.columns or k not in ekpo_df.columns:
            raise KeyError(f"Missing join key {k} in either  EKKO or EKPO.")

    dup_mask_ekko = ekko_df.duplicated(subset=keys,keep=False)
    dup_rows_ekko = ekko_df[dup_mask_ekko]

    if not dup_rows_ekko.empty:
        logger.warning(f"EKKO has duplicate (Client, Purchasing Doc.) records — investigate possible extraction issue., number of duplicates: {dup_rows_ekko.shape[0]}")
        logger.debug(f"Duplicate rows in EKKO:\n{dup_rows_ekko}")
        logger.info(f'Before dropping duplicates, EKKO shape: {ekko_df.shape}')
        ekko_df = ekko_df.drop_duplicates(subset=keys, keep='last')
        logger.info(f'After dropping duplicates, EKKO shape: {ekko_df.shape}')
    else:
        logger.info("No duplicates found in EKKO based on (Client, Purchasing Doc.).")

    # Data type check for join keys
    logger.debug(f"Data types of join keys in EKKO:\n{ekko_df[keys].dtypes}")
    logger.debug(f"Data types of join keys in EKPO:\n{ekpo_df[keys].dtypes}")

    # Filter EKPO to include only valid headers
    filtered_ekpo_df = ekpo_df.merge(ekko_df[keys].drop_duplicates(), on=keys, how='inner')
    logger.info(f'After filtering EKPO with valid headers, EKPO shape: {filtered_ekpo_df.shape}')

    missing_headers = ekpo_df.loc[
    ~ekpo_df['PURCHASE_ORDER_NUMBER'].isin(ekko_df['PURCHASE_ORDER_NUMBER'])]

    if not missing_headers.empty:
        logger.warning(f"{missing_headers['PURCHASE_ORDER_NUMBER'].nunique()} POs in EKPO have no matching header in EKKO. Excluded from merge.")

    # Merge coverage check
    pre_rows = ekpo_df.shape[0]
    post_rows = filtered_ekpo_df.shape[0]
    matched = filtered_ekpo_df['PURCHASE_ORDER_NUMBER'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: EKPO + EKKO: {match_pct:.2f}% of line items found matching header without null values.")

    # Data type check for join keys after filtering
    logger.debug(f"Data types of join keys in filtered EKPO:\n{filtered_ekpo_df[keys].dtypes}")
    logger.debug(f"Data types of join keys in EKKO:\n{ekko_df[keys].dtypes}")


    # Check how many rows has PO_NUMBER as null in both EKPO and EKKO
    null_ekpo_po = filtered_ekpo_df['PURCHASE_ORDER_NUMBER'].isnull().sum()
    null_ekko_po = ekko_df['PURCHASE_ORDER_NUMBER'].isnull().sum()
    logger.info(f"Number of null PURCHASE_ORDER_NUMBER in filtered EKPO: {null_ekpo_po}")
    logger.info(f"Number of null PURCHASE_ORDER_NUMBER in EKKO: {null_ekko_po}")

    pre_merge_shape = filtered_ekpo_df.shape[0]
    # Merge data
    merged_df = filtered_ekpo_df.merge(ekko_df, on=keys, how='left', suffixes=('_EKPO', '_EKKO'))
    logger.info(f'After merging EKPO with EKKO, merged shape: {merged_df.shape}')

    logger.debug(f'Columns in merged DataFrame EKPO + EKKO: {merged_df.columns.tolist()}')
    post_merge_shape = merged_df.shape[0]
    if pre_merge_shape != post_merge_shape:
        raise ValueError("Row count changed after merging EKPO with EKKO, indicating possible duplication.")
    
    
    # Check for null values after merge
    null_merged = merged_df.isnull().sum()
    logger.debug(f"Null values in merged EKPO + EKKO columns:\n{null_merged[null_merged > 0]}")

    ekpo_item_keys = ['CLIENT', 'PURCHASE_ORDER_NUMBER', 'PO_ITEM_NUMBER']
    dup_mask = merged_df.duplicated(subset=ekpo_item_keys, keep=False)
    dup_count = dup_mask.sum()

    if dup_count > 0:
        logger.warning(f"EKPO has {dup_count} duplicate items (Client, PO, Item). Removing duplicates...")
        logger.info(f'Before dedup: {merged_df.shape}')
        merged_df = merged_df.drop_duplicates(subset=ekpo_item_keys, keep='first')
        logger.info(f'After dedup: {merged_df.shape}')
    else:
        logger.info("No duplicate PO items found in merged EKPO+EKKO.")



    invoice_keys = ['CLIENT','PURCHASE_ORDER_NUMBER','PO_ITEM_NUMBER']
    po_keys = ['CLIENT','PURCHASE_ORDER_NUMBER','PO_ITEM_NUMBER']


    logger.debug(f"Data types of join keys in merged PO DataFrame:\\n{merged_df[po_keys].dtypes}")
    logger.debug(f"Data types of join keys in Invoice DataFrame:\\n{invoice_df[invoice_keys].dtypes}")

    # Null values before merging for whole dataframes
    null_merged_before = merged_df.isnull().sum()
    null_invoice_before = invoice_df.isnull().sum()
    logger.debug(f"Null values in merged PO DataFrame before merging with Invoice:\n{null_merged_before[null_merged_before > 0]}")
    logger.debug(f"Null values in Invoice DataFrame before merging with PO:\n{null_invoice_before[null_invoice_before > 0]}")


    # standarise po Number column to int
    # Handle empty strings by replacing with NaN first, then fill with -9999
    
    invoice_df['PURCHASE_ORDER_NUMBER'] = invoice_df['PURCHASE_ORDER_NUMBER'].replace('', np.nan).fillna(-9999).astype(float).astype(int)
    merged_df['PURCHASE_ORDER_NUMBER'] = merged_df['PURCHASE_ORDER_NUMBER'].astype(int)

    # Standardise PO Item Number column to int
    # Handle empty strings by replacing with NaN first, then fill with -9999
    invoice_df['PO_ITEM_NUMBER'] = invoice_df['PO_ITEM_NUMBER'].replace('', np.nan).fillna(-9999).astype(float).astype(int)
    merged_df['PO_ITEM_NUMBER'] = merged_df['PO_ITEM_NUMBER'].fillna(-9999).astype(int)

    logger.debug(f"Data types of join keys in merged PO DataFrame (after conversion):\\n{merged_df[po_keys].dtypes}")
    logger.debug(f"Data types of join keys in Invoice DataFrame (after conversion):\\n{invoice_df[invoice_keys].dtypes}")

    invoice_with_po_df = pd.merge(
    invoice_df,
    merged_df,
    how='left',
    left_on=invoice_keys,
    right_on=po_keys,
    suffixes=('', '_PO')
    )
    
    logger.info(f"Invoice rows before merge: {invoice_df.shape[0]}")
    logger.info(f"PO fact rows: {merged_df.shape[0]}")
    logger.info(f"Merged rows: {invoice_with_po_df.shape[0]}")

    invoice_df['has_po'] = invoice_df['PURCHASE_ORDER_NUMBER'].notna() & invoice_df['PURCHASE_ORDER_NUMBER'].astype(str).str.strip().ne('')
    
    # Revert -9999 back to NaN for PO number
    invoice_with_po_df['PURCHASE_ORDER_NUMBER'] = invoice_with_po_df['PURCHASE_ORDER_NUMBER'].replace(-9999, pd.NA)

    # Check row count consistency  
    if invoice_with_po_df.shape[0] != invoice_df.shape[0]:
        logger.warning("Row count mismatch — possible PO-item duplication in EKPO.")
        raise ValueError("Row count mismatch after merging Invoice with PO details.")
    else:
        logger.info("Line item counts consistent.")


    # Check null values in new columns from PO merge
    new_po_columns = [col for col in invoice_with_po_df.columns if col not in invoice_df.columns]
    null_po = invoice_with_po_df[new_po_columns].isnull().sum()
    logger.debug(f"Null values in new PO columns after merge:\\n{null_po[null_po > 0]}")
    if "SHORT_TEXT_PO" in invoice_with_po_df.columns:

        matched = invoice_with_po_df['SHORT_TEXT_PO'].notna().sum()
        logger.info(f"Matched invoices with PO details: {matched/invoice_df['has_po'].sum()}")

    logger.debug(f"Final columns after PO merge: {invoice_with_po_df.columns.tolist()}")

    # Merge coverage check
    pre_rows = invoice_df.shape[0]
    post_rows = invoice_with_po_df.shape[0]
    if "SHORT_TEXT_PO" not in invoice_with_po_df.columns:
        logger.warning("SHORT_TEXT_PO column missing after merge - cannot compute merge coverage.")
        return invoice_with_po_df
    
    
    matched = invoice_with_po_df['SHORT_TEXT_PO'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + PO: {match_pct:.2f}% of line items found matching PO details without null values.")

    return invoice_with_po_df

    