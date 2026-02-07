import pandas as pd
import numpy as np
from typing import Optional

from .retinv_rename import RETINV_RENAME_MAPPINGS
from sap_data_pipeline.data_cleaning import clean_amount_column, add_quarter_label , clean_date_column
from ..logger_config import get_logger
from .bseg_rename import BSEG_RENAME_MAP
from .bkpf_rename import BKPF_RENAME_MAP
from .withhold_tax_rename import WITHHOLD_TAX_RENAME_MAP
from .t003_rename import T003_RENAME_MAP
from .udc_rename import UDC_RENAME_MAPPINGS
region_lookup = {1:'NAA',2:'LAA',3:'APAC',4:'EMEA'}

def merge_bseg_bkpf(bseg_file_df:Optional[pd.DataFrame], bkpf_file_df:Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merge BSEG and BKPF DataFrames on key document identifiers.
    Args:
        bseg_file_df (pd.DataFrame): BSEG table DataFrame with line-item data.
        bkpf_file_df (pd.DataFrame): BKPF table DataFrame with document header data.
    Returns:
        pd.DataFrame: Merged DataFrame containing line-item data enriched with header info.
    """
    logger = get_logger()
    logger.info("\n=== [STEP 1] BSEG + BKPF Merge Start ===")

    if bseg_file_df is None or bkpf_file_df is None:
        raise ValueError("BSEG and BKPF DataFrames must be provided for merging.")
    
    if bseg_file_df.empty or bkpf_file_df.empty:
        raise ValueError("BSEG and BKPF DataFrames cannot be empty.")
    

    logger.info(f"BSEG - Line item level data shape: {bseg_file_df.shape}, BKPF Header level data shape: {bkpf_file_df.shape}")
    logger.info('Rename columns in BSEG and BKPF to standard names')
    # Rename columns using the predefined maps
    bseg_file_df.rename(columns=BSEG_RENAME_MAP, inplace=True)
    bkpf_file_df.rename(columns=BKPF_RENAME_MAP, inplace=True)

    # Validate and capture invalid REGION values in BSEG
    numeric_region = pd.to_numeric(bseg_file_df['REGION'], errors='coerce')
    valid_mask = numeric_region.isin([1, 2, 3, 4]) & numeric_region.notna()
    bseg_invalid_mask = ~valid_mask
    bseg_invalid_records = bseg_file_df.loc[bseg_invalid_mask, ['CLIENT','COMPANY_CODE','DOCUMENT_NUMBER','REGION']]

    logger.warning(f"BSEG: {bseg_invalid_mask.sum()} invalid REGION values")
    if bseg_invalid_mask.sum() > 0:
        logger.warning(f"Invalid REGION value counts:\n{bseg_invalid_records['REGION'].value_counts()}")
        logger.warning(f"Sample invalid rows:\n{bseg_invalid_records[['CLIENT','COMPANY_CODE','DOCUMENT_NUMBER','REGION']].head(10)}")

    
    # Validate and capture invalid REGION values in BKPF
    numeric_region_bkpf = pd.to_numeric(bkpf_file_df['REGION'], errors='coerce')
    valid_mask_bkpf = numeric_region_bkpf.isin([1, 2, 3, 4]) & numeric_region_bkpf.notna()
    bkpf_invalid_mask = ~valid_mask_bkpf
    bkpf_invalid_records = bkpf_file_df.loc[bkpf_invalid_mask, ['CLIENT','COMPANY_CODE','DOCUMENT_NUMBER','REGION']]

    logger.warning(f"BKPF: {bkpf_invalid_mask.sum()} invalid REGION values")
    if bkpf_invalid_mask.sum() > 0:
        logger.warning(f"Invalid REGION value counts:\n{bkpf_invalid_records['REGION'].value_counts()}")
        logger.warning(f"Sample invalid rows:\n{bkpf_invalid_records[['CLIENT','COMPANY_CODE','DOCUMENT_NUMBER','REGION']].head(10)}")

    logger.debug(f'Region value counts in BSEG before mapping: {bseg_file_df["REGION"].value_counts(dropna=False)}')
    logger.debug(f'Region value counts in BKPF before mapping: {bkpf_file_df["REGION"].value_counts(dropna=False)}')

    
    bseg_file_df['REGION'] = pd.to_numeric(bseg_file_df['REGION'], errors='coerce').astype('Int64')
    bkpf_file_df['REGION'] = pd.to_numeric(bkpf_file_df['REGION'], errors='coerce').astype('Int64')

    bseg_file_df['REGION'] = bseg_file_df['REGION'].map(region_lookup)
    bkpf_file_df['REGION'] = bkpf_file_df['REGION'].map(region_lookup)
    logger.debug(f'Region value counts in BSEG after mapping: {bseg_file_df["REGION"].value_counts(dropna=False)}')
    logger.debug(f'Region value counts in BKPF after mapping: {bkpf_file_df["REGION"].value_counts(dropna=False)}')

    # check for null values and log counts greater than 0
    null_bseg_data = bseg_file_df.isna().sum()
    null_bkpf_data = bkpf_file_df.isna().sum()
    logger.debug(f"Null value counts in BSEG columns:\n{null_bseg_data[null_bseg_data > 0]}")
    logger.debug(f"Null value counts in BKPF columns:\n{null_bkpf_data[null_bkpf_data > 0]}")

    rows_with_invoice_number_null = bkpf_file_df[bkpf_file_df['INVOICE_NUMBER'].isna()]
    logger.warning(f"Rows in BKPF with NULL INVOICE_NUMBER: {rows_with_invoice_number_null.shape[0]}")

    bkpf_file_df = bkpf_file_df[~bkpf_file_df['INVOICE_NUMBER'].isna()].copy()

    bkpf_file_df['POSTED_DATE'] = clean_date_column(bkpf_file_df['POSTED_DATE'],can_be_null=False)

    # check Null value count in Posted, drop rows will null posted date

    logger.info(f'no of rows with posted date as Null: {bkpf_file_df["POSTED_DATE"].isna().sum()}')
    logger.info(f'Shape before dropping null posted date: {bkpf_file_df.shape}')
    bkpf_file_df = bkpf_file_df[bkpf_file_df['POSTED_DATE'].notna()]
    logger.info(f'After dropping null posted dates, shape of BKPF: {bkpf_file_df.shape}')

    bkpf_file_df['INVOICE_DATE'] = clean_date_column(bkpf_file_df['INVOICE_DATE'],can_be_null=False)
    bkpf_file_df['ENTERED_DATE'] = clean_date_column(bkpf_file_df['ENTERED_DATE'],can_be_null=False)
    bkpf_file_df['INVOICE_RECEIPT_DATE'] = clean_date_column(bkpf_file_df['INVOICE_RECEIPT_DATE'],can_be_null=True)
    bkpf_file_df['PAYMENT_DATE'] = clean_date_column(bkpf_file_df['PAYMENT_DATE'],can_be_null=True)
    bkpf_file_df['BASELINE_DATE'] = clean_date_column(bkpf_file_df['BASELINE_DATE'],can_be_null=True)
    bkpf_file_df['DUE_DATE'] = clean_date_column(bkpf_file_df['DUE_DATE'],can_be_null=True) # This cant be null, for testing purposes we allow it to be null


    bkpf_file_df = add_quarter_label(bkpf_file_df, date_col='POSTED_DATE')
    logger.info('Quarter labels added to BKPF based on POSTED_DATE')
    logger.debug(bkpf_file_df['QUARTER_LABEL'].value_counts())

    logger.debug('Check for Region values in BSEG and BKPF after mapping:')
    logger.debug(f'Region value counts in BSEG after mapping: {bseg_file_df["REGION"].value_counts(dropna=False)}')
    logger.debug(f'Region value counts in BKPF after mapping: {bkpf_file_df["REGION"].value_counts(dropna=False)}')
    
    bseg_file_df['LINEITEM_AMOUNT_IN_LOCAL_CURRENCY'] = clean_amount_column(bseg_file_df['LINEITEM_AMOUNT_IN_LOCAL_CURRENCY'])
    bseg_file_df['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY'] = clean_amount_column(bseg_file_df['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY'])

    bkpf_file_df['TOTAL_AMOUNT_LC'] = clean_amount_column(bkpf_file_df['TOTAL_AMOUNT_LC'])
    bkpf_file_df['TOTAL_AMOUNT'] = clean_amount_column(bkpf_file_df['TOTAL_AMOUNT'])
    bkpf_file_df['EXCHANGE_RATE'] = clean_amount_column(bkpf_file_df['EXCHANGE_RATE'])
    bkpf_file_df['EXCHANGE_RATE_USD'] = clean_amount_column(bkpf_file_df['EXCHANGE_RATE_USD'])
    # fill null exchange rate usd with 1.0, since NULL means its in USD
    bkpf_file_df['EXCHANGE_RATE_USD'].fillna(1.0, inplace=True)

    bkpf_file_df['TOTAL_AMOUNT_USD'] = bkpf_file_df['TOTAL_AMOUNT']*bkpf_file_df['EXCHANGE_RATE_USD']
    bkpf_file_df['TOTAL_AMOUNT_LC_USD'] = bkpf_file_df['TOTAL_AMOUNT_LC']*bkpf_file_df['EXCHANGE_RATE_USD']
    # --- Check presence of key columns ---
    required_cols = ['CLIENT', 'COMPANY_CODE', 'DOCUMENT_NUMBER', 'FISCAL_YEAR']
    for df_name, df, cols in [('BSEG', bseg_file_df, required_cols),
                              ('BKPF', bkpf_file_df, required_cols)]:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise KeyError(f"{df_name} missing required columns: {missing}")

    # --- Diagnostic counts ---
    header_doc_count = bkpf_file_df[required_cols].drop_duplicates().shape[0]
    line_doc_count = bseg_file_df[required_cols].drop_duplicates().shape[0]
    logger.info(f"Header level count (BKPF): {header_doc_count}")
    logger.info(f"Line-item level count(BSEG): {line_doc_count}")


    filtered_bseg = bseg_file_df.merge(
                        bkpf_file_df[required_cols].drop_duplicates(),
                        on=required_cols,
                        how="inner"
                    )
    logger.info(f'Shape of BSEG Data before filtering: {bseg_file_df.shape}')
    logger.info(f'Shape of BSEG Data after filtering: {filtered_bseg.shape}')

    dup = bkpf_file_df[required_cols].duplicated()
    if dup.sum() > 0:
        logger.warning(f"⚠️ BKPF has {dup.sum()} duplicate document headers — investigate possible extraction issue.")
        logger.info(f"Duplicate rows df:{bkpf_file_df[dup].to_dict()}")
        # drop duplicates
        bkpf_file_df = bkpf_file_df.drop_duplicates(subset=required_cols,keep='last').copy()
        logger.info(f'After dropping duplicates, BKPF shape: {bkpf_file_df.shape}')

    # log data types of the keys
    logger.debug("Data types of join keys in BSEG:")
    logger.debug(filtered_bseg[required_cols].dtypes)
    logger.debug("Data types of join keys in BKPF:")
    logger.debug(bkpf_file_df[required_cols].dtypes)


    # --- Merge ---
    merged = pd.merge(filtered_bseg, bkpf_file_df, how='left', on=required_cols, suffixes=('_BSEG', '_BKPF'))

    merged['LINEITEM_AMOUNT_IN_LOCAL_CURRENCY_USD'] = merged['LINEITEM_AMOUNT_IN_LOCAL_CURRENCY'] * merged['EXCHANGE_RATE_USD']
    merged['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY_USD'] = merged['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY'] * merged['EXCHANGE_RATE_USD']
    logger.info(f"BSEG and BKPF merged. Shape: {merged.shape}")

    logger.info("=== [STEP 1 Complete] ===")

    # Check for Merge Coverage, calculate percentage of line items that found a matching header without null values
    pre_rows = filtered_bseg.shape[0]
    post_rows = merged.shape[0]
    matched = merged['DOCUMENT_NUMBER'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE FOR BSEG+BKPF: {match_pct:.2f}% of line items found matching header  without null values.")
    return merged


# How to merge WITH_ITEM data? its contains multiple line items per document
def merge_with_item(df, with_item):
    logger = get_logger()
    logger.info("\n=== [STEP 2] WITH_ITEM Merge Start ===")
    
    # Handle None - not needed for this pipeline mode
    if with_item is None:
        logger.info("WITH_ITEM not required for this pipeline mode, skipping merge")
        return df
    
    # If DataFrame provided, it must not be empty
    if with_item.empty:
        raise ValueError("WITH_ITEM DataFrame was expected but is empty - data loading failed")
    

    logger.info(f"WITH_ITEM original shape: {with_item.shape}")
    # Rename columns using the predefined maps
    with_item.rename(columns=WITHHOLD_TAX_RENAME_MAP, inplace=True)

    # Clean amount columns
    with_item['WITHHOLD_TAX_BASE_LC'] = clean_amount_column(with_item['WITHHOLD_TAX_BASE_LC'])
    with_item['WITHHOLD_TAX_BASE_FC'] = clean_amount_column(with_item['WITHHOLD_TAX_BASE_FC'])

    logger.debug("Null value in WITH_ITEM df")
    null_with_item_data = with_item.isna().sum()
    logger.debug(null_with_item_data[null_with_item_data > 0])
    

    keys = ['CLIENT', 'COMPANY_CODE', 'DOCUMENT_NUMBER', 'FISCAL_YEAR']

    for k in keys:
        if k not in df.columns or k not in with_item.columns:
            raise KeyError(f"Missing join key {k} in either main DF or WITH_ITEM.")

    # Assumption: WITH_ITEM has multiple entries per document, we take first entry per document
    logger.info(f"WITH_ITEM original shape: {with_item.shape}")
    # Deduplicate WITH_ITEM to ensure one record per document
    with_item = with_item.groupby(keys).first().reset_index()
    logger.info(f"WITH_ITEM shape after deduplication: {with_item.shape}")
    pre_shape = df.shape[0]

    logger.debug("Data types of join keys in WITH_ITEM:")
    logger.debug(with_item[keys].dtypes)
    logger.debug("Data types of join keys in main DF:")
    logger.debug(df[keys].dtypes)

    merged = pd.merge(df, with_item, how='left', on=keys, suffixes=('', '_WITH'))
    post_shape = merged.shape[0]

    
    logger.info(f"WITH_ITEM merge complete. Shape: {merged.shape}")
    # Diagnostic: Check how many records matched
    matched = post_shape - pre_shape
    logger.info(f"Number of records matched in WITH_ITEM merge: {matched}")
    # Check for nulls in key WITH_ITEM columns
    # 'WITHHOLD_TAX_ITEM','WITHHOLD_TAX_TYPE', 'WITHHOLD_TAX_CODE', 'WITHHOLD_TAX_BASE_LC', 'WITHHOLD_TAX_BASE_FC',
    key_with_item_cols = ['WITHHOLD_TAX_ITEM','WITHHOLD_TAX_TYPE', 'WITHHOLD_TAX_CODE', 'WITHHOLD_TAX_BASE_LC', 'WITHHOLD_TAX_BASE_FC']
    for col in key_with_item_cols:
        if col in merged.columns:
            null_count = merged[col].isna().sum()
            if null_count > 0:
                logger.warning(f"Column {col} has {null_count} null values after WITH_ITEM merge.")
    

    logger.info("=== [STEP 2 Complete] ===")
    # Check for merge coverage
    pre_rows = df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['WITHHOLD_TAX_ITEM'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + WITH_ITEM: {match_pct:.2f}% of line items found matching header  without null values.")
    if pre_rows != post_rows:
        logger.warning(f"⚠️ Row count changed during WITH_ITEM merge: before={pre_rows}, after={post_rows} — investigate possible data issues.")
        raise ValueError("Row count changed during WITH_ITEM merge.")
    return merged


def merge_t003(df, t003):
    logger = get_logger()
    logger.info("\n=== [STEP 3] T003 Merge Start ===")
    
    # Handle None - not needed for this pipeline mode
    if t003 is None:
        logger.info("T003 not required for this pipeline mode, skipping merge")
        return df
    
    # If DataFrame provided, it must not be empty
    if t003.empty:
        raise ValueError("T003 DataFrame was expected but is empty - data loading failed")
    logger.info(f"T003 original shape: {t003.shape}")
    # Rename columns using the predefined maps
    t003.rename(columns=T003_RENAME_MAP, inplace=True)

    logger.debug("Null value in T003 df")
    null_t003_data = t003.isna().sum()
    logger.debug(null_t003_data[null_t003_data > 0])

    keys = ['CLIENT', 'DOCUMENT_TYPE']
    for k in keys:
        if k not in df.columns or k not in t003.columns:
            raise KeyError(f"Missing join key {k} in either DF or T003.")
        
    dup_mask = t003.duplicated(subset=keys,keep=False)
    dup_rows = t003[dup_mask]   

    if not dup_rows.empty:
        logger.warning(f"⚠️ T003 has {dup_rows.shape[0]} duplicate (Client, Document Type) records — investigate possible extraction issue.")
        logger.debug(dup_rows)
        logger.info(f'Before dropping duplicates, T003 shape: {t003.shape}')
        t003 = t003.drop_duplicates(subset=keys).copy()
        logger.info(f'After dropping duplicates, T003 shape: {t003.shape}')
    else:
        logger.info("No duplicates found in T003 based on (Client, Document Type).")

    logger.debug("Data types of join keys in T003:")
    logger.debug(t003[keys].dtypes)
    logger.debug("Data types of join keys in main DF:")
    logger.debug(df[keys].dtypes)


    # Document type standardise value
    t003['DOCUMENT_TYPE'] = t003['DOCUMENT_TYPE'].astype(str).str.upper().str.strip()
    df['DOCUMENT_TYPE'] = df['DOCUMENT_TYPE'].astype(str).str.upper().str.strip()

    pre_shape = df.shape[0]
    merged = pd.merge(df, t003, how='left', on=keys, suffixes=('', '_T003'))
    post_shape = merged.shape[0]
    if pre_shape != post_shape:
        logger.warning(f"⚠️ Row count changed during T003 merge: before={pre_shape}, after={post_shape} — investigate possible data issues.") 

    # Diagnostic: Check key T003 columns for nulls
    key_t003_cols = ['DOCUMENT_TYPE_DESCRIPTION']
    for col in key_t003_cols:
        if col in merged.columns:
            null_count = merged[col].isna().sum()
            if null_count > 0:
                logger.warning(f"Column {col} has {null_count} null values after T003 merge.")   


    logger.info(f"T003 merge complete. Shape: {merged.shape}")
    logger.info("=== [STEP 3 Complete] ===")
    # Check for merge coverage
    pre_rows = df.shape[0]
    post_rows = merged.shape[0]
    matched = merged['DOCUMENT_TYPE_DESCRIPTION'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + T003: {match_pct:.2f}% of line items found matching header  without null values.")
    if pre_rows != post_rows:
        logger.warning(f"⚠️ Row count changed during T003 merge: before={pre_rows}, after={post_rows} — investigate possible data issues.")
        raise ValueError("Row count changed during T003 merge.")
    return merged

def create_vim_key_for_reference(row):
    """
    Create a VIM object key based on the row data.

    if REF_TRANSACTION == RMRP, return REFERENCE_KEY
    else, create a key based on  COMPANY_CODE, DOCUMENT_NUMBER, and Year
    """
    if row['REF_TRANSACTION'] == 'RMRP':
        return row['REFERENCE_KEY']
    else:
        # REMOVE ZFILL 
        val1 = str(row['COMPANY_CODE']).strip()
        val2 = str(row['DOCUMENT_NUMBER']).strip()
        val3 = str(row['FISCAL_YEAR']).strip()
        return val1 + val2 + val3


def merge_retinv(df:pd.DataFrame, retinv:pd.DataFrame)->pd.DataFrame:
    """Merge with RETINV table."""
    logger = get_logger()
    logger.info(f"Merging with RETINV. Initial shape: {df.shape}")
    
    # Handle None - not needed for this pipeline mode
    if retinv is None:
        logger.info("RETINV not required for this pipeline mode, skipping merge")
        return df
    
    # If DataFrame provided, it must not be empty (data validation)
    if retinv.empty:
        raise ValueError("RETINV DataFrame was expected but is empty - data loading failed")
    
    if df.empty or df is None:
        raise ValueError("Main DataFrame cannot be empty for merging with RETINV.")

    logger.info(f"RETINV original shape: {retinv.shape}")
    # Rename columns using the predefined maps
    retinv.rename(columns=RETINV_RENAME_MAPPINGS, inplace=True)
    logger.debug("Null value in RETINV df")
    null_retinv_data = retinv.isna().sum()
    logger.debug(null_retinv_data[null_retinv_data > 0])
    keys = ['CLIENT', 'COMPANY_CODE', 'DOCUMENT_NUMBER', 'FISCAL_YEAR']
    for k in keys:
        if k not in df.columns or k not in retinv.columns:
            raise KeyError(f"Missing join key {k} in either main DF or RETINV.")
    pre_shape = df.shape[0]
    logger.debug("Data types of join keys in RETINV:")
    logger.debug(retinv[keys].dtypes)
    logger.debug("Data types of join keys in main DF:")

    # Group RETINV to ensure one record per document, taking list of values for other columns
    retinv = retinv.groupby(keys).agg(list).reset_index()
    logger.info(f"RETINV shape after grouping: {retinv.shape}")
    
    logger.debug(df[keys].dtypes)
    merged = pd.merge(df, retinv, how='left', on=keys, suffixes=('', '_RETINV'))
    post_shape = merged.shape[0]
    logger.info(f"RETINV merge complete. Shape: {merged.shape}")
    # Diagnostic: Check how many records matched
    matched = post_shape - pre_shape
    # merge coverage
    pre_shape = df.shape[0]
    post_shape = merged.shape[0]
    matched = merged['AUDIT_REASON'].notna()
    match_pct = 100 * matched.sum() / pre_shape     
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + RETINV: {match_pct:.2f}% of line items found matching header  without null values.")
    logger.info("=== [STEP 4 Complete] ===")
    if pre_shape != post_shape:
        logger.warning(f"⚠️ Row count changed during RETINV merge: before={pre_shape}, after={post_shape} — investigate possible data issues.")
        raise ValueError("Row count changed during RETINV merge.")
    return merged

def merge_udc(df:pd.DataFrame, udc:pd.DataFrame)->pd.DataFrame:
    """Merge with UDC table."""
    logger = get_logger()
    logger.info(f"Merging with UDC. Initial shape: {df.shape}")
    # Handle None - not needed for this pipeline mode
    if udc is None:
        logger.info("UDC not required for this pipeline mode, skipping merge")
        return df
    
    # If DataFrame provided, it must not be empty
    if udc.empty:
        raise ValueError("UDC DataFrame was expected but is empty - data loading failed")
    
    if df.empty or df is None:
        raise ValueError("Main DataFrame cannot be empty for merging with UDC.")
    
    logger.info("\n=== [STEP 5] UDC Merge Start ===")
    logger.info(f"UDC original shape: {udc.shape}")



    # Rename columns using the predefined maps
    udc.rename(columns=UDC_RENAME_MAPPINGS, inplace=True)
    udc['UDC_KEY'] = udc['DOCUMENT_NUMBER_UDC'].astype(str).str.strip() + udc['FISCAL_YEAR_UDC'].astype(str).str.strip()


    inv_keys = ['CLIENT','VIM_OBJECT_KEY','PURCHASE_ORDER_NUMBER','PO_ITEM_NUMBER']
    udc_keys = ['CLIENT','UDC_KEY','PURCHASING_DOCUMENT_UDC','ITEM_UDC']
    
    # check for duplicate rows in UDC and drop if exists
    dup_mask = udc.duplicated(subset=udc_keys, keep=False)
    dup_rows = udc[dup_mask]

    if not dup_rows.empty:
        logger.warning(f"⚠️ UDC has {dup_rows.shape[0]} duplicate records based on {udc_keys} — investigating and removing duplicates.")
        logger.debug(f"Duplicate rows in UDC:\n{dup_rows}")
        logger.info(f'Before dropping duplicates, UDC shape: {udc.shape}')
        udc = udc.drop_duplicates(subset=udc_keys, keep='last').copy()
        logger.info(f'After dropping duplicates, UDC shape: {udc.shape}')
    else:
        logger.info(f"No duplicates found in UDC based on {udc_keys}.")

    
    if 'AMOUNT_UDC' in udc.columns:
        udc['AMOUNT_UDC'] = clean_amount_column(udc['AMOUNT_UDC'])



    for k in inv_keys:
        if k not in df.columns:
            raise KeyError(f"Missing join key {k} in main DF")
    for k in udc_keys:
        if k not in udc.columns:
            raise KeyError(f"Missing join key {k} in UDC.")
        
    df['VIM_OBJECT_KEY'] = df['VIM_OBJECT_KEY'].astype(int)
    udc['UDC_KEY'] = udc['UDC_KEY'].astype(int)

    df['PURCHASE_ORDER_NUMBER'] = df['PURCHASE_ORDER_NUMBER'].replace('', np.nan).fillna(-9999).astype(float).astype(int)
    df['PO_ITEM_NUMBER'] = df['PO_ITEM_NUMBER'].replace('', np.nan).fillna(-9999).astype(float).astype(int)


    udc['PURCHASING_DOCUMENT_UDC'] = udc['PURCHASING_DOCUMENT_UDC'].replace('', np.nan).fillna(-9999).astype(float).astype(int)
    udc['ITEM_UDC'] = udc['ITEM_UDC'].replace('', np.nan).fillna(-9999).astype(float).astype(int)



    # Data type before merging
    logger.debug("Data types of join keys in UDC:")
    logger.debug(udc[udc_keys].dtypes)
    logger.debug("Data types of join keys in main DF:")
    logger.debug(df[inv_keys].dtypes)

    pre_shape = df.shape[0]
    merged = pd.merge(df, udc, how='left', left_on=inv_keys, right_on=udc_keys, suffixes=('', '_UDC'))
    post_shape = merged.shape[0]
    logger.info(f"UDC merge complete. Shape: {merged.shape}")
    # Diagnostic: Check how many records matched
    matched = post_shape - pre_shape
    # merge coverage
    matched = merged['AMOUNT_UDC'].notna()
    match_pct = 100 * matched.sum() / pre_shape     
    logger.info(f"MERGE COVERAGE: INVOICE_DATA + UDC: {match_pct:.2f}% of line items found matching header  without null values.")
    logger.info("=== [STEP 5 Complete] ===")

    if pre_shape != post_shape:
        logger.warning(f"⚠️ Row count changed during UDC merge: before={pre_shape}, after={post_shape} — investigate possible data issues.")
        raise ValueError("Row count changed during UDC merge.")

    merged['AMOUNT_UDC'].fillna(0.0, inplace=True)
    merged['AMOUNT_UDC_USD'] = merged['AMOUNT_UDC'] * merged['EXCHANGE_RATE_USD']
    return merged

def build_invoice_core(bseg:Optional[pd.DataFrame],
                        bkpf:Optional[pd.DataFrame],
                        with_item:Optional[pd.DataFrame]=None,
                        t003:Optional[pd.DataFrame]=None,
                        retinv:Optional[pd.DataFrame]=None,
                        udc:Optional[pd.DataFrame]=None)-> pd.DataFrame:
    """Orchestrate the core invoice dataset building."""
    logger = get_logger()
    logger.info("Starting build_invoice_core...")
    

    logger.info("\n🚀 Starting Invoice Core Build Pipeline")
    df = merge_bseg_bkpf(bseg, bkpf)
    logger.info(f"After BSEG+BKPF merge: {df.shape}")
    logger.debug(df['REGION_BSEG'].value_counts(dropna=False))
    logger.debug(df['REGION_BKPF'].value_counts(dropna=False))
    # Check number of accounting docs, Client-CompanyCode-DocumentNumber-FiscalYear
    num_accounting_docs = df[['CLIENT', 'COMPANY_CODE', 'DOCUMENT_NUMBER', 'FISCAL_YEAR']].drop_duplicates().shape[0]
    logger.info(f"Number of unique accounting documents after BSEG+BKPF merge: {num_accounting_docs}")

    # add a new column called VIM_OBJECT_KEY, same key is used for UDC too....

    df['VIM_OBJECT_KEY'] = df.apply(lambda row:create_vim_key_for_reference(row),axis=1)

    if with_item is not None:
        df = merge_with_item(df, with_item)
        logger.info(f"After WITH_ITEM merge: {df.shape}")
    else:
        logger.info("Skipping WITH_ITEM (not provided)")

    if t003 is not None:
        df = merge_t003(df, t003)
        logger.info(f"After T003 merge: {df.shape}")
    else:
        logger.info("Skipping T003 (not provided)")

    logger.info(f"✅ Invoice Core Build Complete. BSEG+BKPF+WITH_ITEM+T003 Final Shape: {df.shape}")
    logger.debug(f"Columns: {df.columns.tolist()}")
    
    if retinv is not None:
        df = merge_retinv(df, retinv)
        logger.info(f"After RETINV merge: {df.shape}")
    else:
        logger.info("Skipping RETINV (not provided)")

    if udc is not None:
        df = merge_udc(df, udc)
        logger.info(f"After UDC merge: {df.shape}")
    else:
        logger.info("Skipping UDC (not provided)")

    return df