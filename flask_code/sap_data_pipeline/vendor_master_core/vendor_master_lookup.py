import pandas as pd
from typing import Optional

from .lfbk_rename import LFBK_RENAME_COLUMNS
from .lfa1_rename import LFA1_RENAME_COLUMNS
from .lfb1_rename import LFB1_RENAME_COLUMNS


def merge_lfa1_lfb1(lfa1:pd.DataFrame,
                    lfb1:pd.DataFrame)-> pd.DataFrame:
     
    """Build Vendor Master Core DataFrame by merging LFA1 and LFB1 DataFrames on key vendor identifiers.
    Args:
        lfa1 (pd.DataFrame): LFA1 table DataFrame with general vendor details.
        lfb1 (pd.DataFrame): LFB1 table DataFrame with vendor company code details.

    Returns:
        pd.DataFrame: Merged DataFrame containing vendor master core data.
    """
    from ..logger_config import get_logger
    logger = get_logger()
    
    if lfa1 is None or lfb1 is None:
        raise ValueError("LFA1 and LFB1 DataFrames must be provided for merging.")
    
    if lfa1.empty or lfb1.empty:
        raise ValueError("LFA1 and LFB1 DataFrames cannot be empty.")
    
    logger.info('Renaming LFA1 columns...')
    lfa1.rename(columns=LFA1_RENAME_COLUMNS,inplace=True)
    logger.info('Renaming LFB1 columns...')
    lfb1.rename(columns=LFB1_RENAME_COLUMNS,inplace=True)

    address_fields = [ 'VENDOR_PO_BOX', 'VENDOR_POSTAL_CODE','VENDOR_STREET','VENDOR_CITY','VENDOR_REGION','VENDOR_COUNTRY']
    lfa1['VENDOR_ADDRESS'] = lfa1[address_fields].fillna('').agg(', '.join, axis=1).str.strip(', ')

    # if Vendor Address has just commas, replace with NaN
    lfa1['VENDOR_ADDRESS'] = lfa1['VENDOR_ADDRESS'].replace(r'^(,\s*)+$', pd.NA, regex=True)

    logger.debug(lfa1['VENDOR_ADDRESS'].value_counts(dropna=False))

    # VENDOR_TAX_NUMBER_1, VENDOR_TAX_NUMBER_2 ..... VENDOR_TAX_NUMBER_6 -- take all tax numbers and combine into one field, ignore NaNs, combined by comma ,
    tax_number_fields = [ 'VENDOR_TAX_NUMBER_1', 'VENDOR_TAX_NUMBER_2','VENDOR_TAX_NUMBER_3',
                         'VENDOR_TAX_NUMBER_4','VENDOR_TAX_NUMBER_5','VENDOR_TAX_NUMBER_6']
    
    lfa1['VENDOR_TAX_NUMBER_LIST'] = lfa1[tax_number_fields].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
    logger.info('Combined vendor tax numbers into VENDOR_TAX_NUMBER_LIST field.')
    logger.debug(lfa1['VENDOR_TAX_NUMBER_LIST'].value_counts(dropna=False))
    # Null values in LFA1 and LFB1 dataframes
    null_lfa1 = lfa1.isnull().sum()
    null_lfb1 = lfb1.isnull().sum()
    logger.debug(f"Null values in LFA1 columns:\n{null_lfa1[null_lfa1 > 0]}")
    logger.debug(f"Null values in LFB1 columns:\n{null_lfb1[null_lfb1 > 0]}")
    
    keys = ['CLIENT','SUPPLIER_ID']
    for k in keys:
        if k not  in lfa1.columns or k not in lfb1.columns:
            raise KeyError(f"Missing join key {k} in either LFA1 or LFB1.")
    
    logger.info(f'Shape of LFA1 Vendor Level Details data: {lfa1.shape}')
    logger.info(f'Shape of LFB1 company code data: {lfb1.shape}')
    # Drop duplicates in LFA1, 1 row per vendor expected
    dup_mask_lfa1 = lfa1.duplicated(subset=keys,keep=False)
    dup_rows_lfa1 = lfa1[dup_mask_lfa1]

    # LFA1 is Vendor master, how many rows has Supplier id as null / missing
    missing_supplier_id = lfa1['SUPPLIER_ID'].isna()
    logger.warning(f"Number of records in LFA1 with missing SUPPLIER_ID: {missing_supplier_id.sum()}")
    
    
    if not dup_rows_lfa1.empty:
        logger.warning(f"⚠️ LFA1 has duplicate (Client, Supplier) records — investigate possible extraction issue., no of duplicates: {dup_rows_lfa1.shape[0]} ")
        logger.debug(dup_rows_lfa1)
        logger.info(f'Before dropping duplicates, LFA1 shape: {lfa1.shape}')
        lfa1 = lfa1.drop_duplicates(subset=keys,keep='first')
        logger.info(f'After dropping duplicates, LFA1 shape: {lfa1.shape}')
    else:
        logger.info("No duplicates found in LFA1 based on (Client, Supplier).")

    # Check and print datatype of key columns before merge
    logger.debug("LFA1 key column datatypes:")
    logger.debug(lfa1[keys].dtypes)
    logger.debug("LFB1 key column datatypes:")
    logger.debug(lfb1[keys].dtypes)

    #drop supplier id if not numeric, assumption SUPPLIER_ID should be numeric
    lfa1 = lfa1[pd.to_numeric(lfa1['SUPPLIER_ID'], errors='coerce').notna()]
    lfb1 = lfb1[pd.to_numeric(lfb1['SUPPLIER_ID'], errors='coerce').notna()]


    lfa1['SUPPLIER_ID'] = lfa1['SUPPLIER_ID'].astype(int)
    lfb1['SUPPLIER_ID'] = lfb1['SUPPLIER_ID'].astype(int)   
    merged_df = lfb1.merge(lfa1, on=keys, how='left', suffixes=('_LFB1', '_LFA1'))
    logger.info(f'After merging LFB1 with LFA1, merged shape: {merged_df.shape}')
    
    # Validation
    total_lfb1 = lfb1.shape[0]
    total_matched = merged_df['VENDOR_NAME'].notna().sum() if 'VENDOR_NAME' in merged_df.columns else None
    match_ratio = (total_matched / total_lfb1) if total_matched else 0

    logger.info(f'Vendor match coverage: {total_matched} out of {total_lfb1} ({match_ratio:.2%})')


    # NUll check  post-merge
    logger.debug("Null values in merged DataFrame LFA1+LFB1 columns:")
    null_merged = merged_df.isnull().sum()
    logger.debug(null_merged[null_merged > 0])
    
    if merged_df.duplicated(subset = ['CLIENT','SUPPLIER_ID','COMPANY_CODE']).sum()>0:
        logger.warning("⚠️ Warning: Duplicates found post-merge on (Client, Supplier, Company Code) — check source extracts.")

    if merged_df.shape[0] != lfb1.shape[0]:
        logger.warning("⚠️ Row count mismatch — possible data inflation after merge.")   
    else:
        logger.info("✅ Line item counts consistent after merge. LFA1 and LFB1 merged successfully.")

    # Merge coverage check
    pre_rows = lfb1.shape[0]
    post_rows = merged_df.shape[0]
    matched = merged_df['VENDOR_NAME'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: LFB1 + LFA1: {match_pct:.2f}% of line items found matching vendor details without null values.")
    
    return merged_df
    
# optional merge with LFM1 --> Call if needed
# Not Fully Completed
def merge_vendor_master_core_with_lfm1(vendor_master_core:pd.DataFrame,
                                       lfm1:pd.DataFrame)-> pd.DataFrame:
    """
    Merge Vendor Master Core DataFrame with LFM1 DataFrame on key vendor identifiers.

    Args:
        vendor_master_core (pd.DataFrame): Vendor Master Core DataFrame.
        lfm1 (pd.DataFrame): LFM1 table DataFrame with vendor purchasing data.

    Returns:
        pd.DataFrame: Merged DataFrame containing vendor master data enriched with purchasing details.
    """
    if vendor_master_core is None or lfm1 is None:
        raise ValueError("Vendor Master Core and LFM1 DataFrames must be provided for merging.")
    if vendor_master_core.empty or lfm1.empty:
        raise ValueError("Vendor Master Core and LFM1 DataFrames cannot be empty.")
    


    return pd.DataFrame()


def merge_vendor_master_core_with_lfbk(vendor_master_core:pd.DataFrame,
                                       lfbk:pd.DataFrame)-> pd.DataFrame:
    """
    Merge Vendor Master Core DataFrame with LFBK DataFrame on key vendor identifiers.

    Args:
        vendor_master_core (pd.DataFrame): Vendor Master Core DataFrame.
        lfbk (pd.DataFrame): LFBK table DataFrame with vendor bank details.

    Returns:
        pd.DataFrame: Merged DataFrame containing vendor master data enriched with bank details.
    """
    from ..logger_config import get_logger
    logger = get_logger()
    
    if vendor_master_core is None or lfbk is None:
        raise ValueError("Vendor Master Core and LFBK DataFrames must be provided for merging.")
    if vendor_master_core.empty or lfbk.empty:
        raise ValueError("Vendor Master Core and LFBK DataFrames cannot be empty.")
    

    # LFBK rename
    lfbk.rename(columns=LFBK_RENAME_COLUMNS,inplace=True)

    logger.info(f'Shape of Vendor Master Core data before LFBK merge: {vendor_master_core.shape}')
    logger.info(f'Shape of LFBK bank details data: {lfbk.shape}')

    # Null values in LFBK dataframe
    null_lfbk = lfbk.isnull().sum()
    logger.debug(f"Null values in LFBK columns:\n{null_lfbk[null_lfbk > 0]}")
    
    keys = ['CLIENT','SUPPLIER_ID']
    for k in keys:
        if k not  in vendor_master_core.columns or k not in lfbk.columns:
            raise KeyError(f"Missing join key {k} in either Vendor Master Core or LFBK.")
        
    # Check if doc types of keys are consistent
    logger.debug("Vendor Master Core key column datatypes:")
    logger.debug(vendor_master_core[keys].dtypes)
    logger.debug("LFBK key column datatypes:")
    logger.debug(lfbk[keys].dtypes)

    vendor_master_core['SUPPLIER_ID'] = vendor_master_core['SUPPLIER_ID'].astype(int)
    lfbk['SUPPLIER_ID'] = lfbk['SUPPLIER_ID'].astype(int)


    logger.debug("Data types of join keys in Vendor Master Core after type conversion:")
    logger.debug(vendor_master_core[keys].dtypes)
    logger.debug("Data types of join keys in LFBK after type conversion:")
    logger.debug(lfbk[keys].dtypes)

    pre_merge_shape = vendor_master_core.shape[0]    
    merged_df = vendor_master_core.merge(lfbk, on=keys, how='left', suffixes=('_VendorMaster','_LFBK'))
    logger.info(f'After merging Vendor Master Core with LFBK, merged shape: {merged_df.shape}')
    post_merge_shape = merged_df.shape[0]

    # Check for null values in new columns post-merge
    new_columns = set(merged_df.columns) - set(vendor_master_core.columns)
    null_merged = merged_df[list(new_columns)].isnull().sum()
    logger.debug("Null values in merged DataFrame Vendor Master Core + LFBK columns:")
    logger.debug(null_merged[null_merged > 0])

    # Merge coverage check
    matched = merged_df['BANK_COUNTRY'].notna()
    match_pct = 100 * matched.sum() / pre_merge_shape
    logger.info(f"MERGE COVERAGE: Vendor Master Core + LFBK: {match_pct:.2f}% of vendor records found matching bank details without null values.")


    return merged_df


def merge_invoice_line_item_with_vendor_master_core(invoice_line_item:pd.DataFrame,
                                                    vendor_master_core:pd.DataFrame)-> pd.DataFrame:
    """
    Merge Invoice Line Item DataFrame with Vendor Master Core DataFrame on key vendor identifiers.
    Args:
        invoice_line_item (pd.DataFrame): Invoice Line Item DataFrame.
        vendor_master_core (pd.DataFrame): Vendor Master Core DataFrame.
            
    Returns:
        pd.DataFrame: Merged DataFrame containing invoice line item data enriched with vendor master data.
    """
    from ..logger_config import get_logger
    logger = get_logger()
    
    if invoice_line_item is None or vendor_master_core is None:
        raise ValueError("Invoice Line Item and Vendor Master Core DataFrames must be provided for merging.")
    
    if invoice_line_item.empty or vendor_master_core.empty:
        raise ValueError("Invoice Line Item and Vendor Master Core DataFrames cannot be empty.")
    

    keys = ['CLIENT','SUPPLIER_ID','COMPANY_CODE']
    for k in keys:
        if k not  in invoice_line_item.columns or k not in vendor_master_core.columns:
            raise KeyError(f"Missing join key {k} in either Invoice Line Item or Vendor Master Core.")
        
    logger.info(f'Shape of Invoice Line Item data: {invoice_line_item.shape}')
    logger.info(f'Shape of Vendor Master Core data: {vendor_master_core.shape}')

    dup_mask_vendor_master = vendor_master_core.duplicated(subset=keys,keep=False)
    dup_rows_vendor_master = vendor_master_core[dup_mask_vendor_master]
    if not dup_rows_vendor_master.empty:
        logger.warning(f"⚠️ Vendor Master Core has duplicate (Client, Supplier, Company Code) records — investigate possible extraction issue., no of duplicates: {dup_rows_vendor_master.shape[0]} ")
        logger.debug(dup_rows_vendor_master)
        logger.info(f'Before dropping duplicates, Vendor Master Core shape: {vendor_master_core.shape}')
        vendor_master_core = vendor_master_core.drop_duplicates(subset=keys, keep='last')
        logger.info(f'After dropping duplicates, Vendor Master Core shape: {vendor_master_core.shape}')
    else:
        logger.info("No duplicates found in Vendor Master Core based on (Client, Supplier, Company Code).")

    # ✅ Add the post-cleanup sanity check here
    if vendor_master_core.duplicated(subset=keys).any():
        raise ValueError("Vendor Master Core still contains duplicates after cleanup — invalid merge state.")


    invoice_line_item['SUPPLIER_ID'] = invoice_line_item['SUPPLIER_ID'].astype(int)
    vendor_master_core['SUPPLIER_ID'] = vendor_master_core['SUPPLIER_ID'].astype(int)

    merged_df = invoice_line_item.merge(vendor_master_core, on=keys, how='left', suffixes=('_Invoice','_VendorMaster'))
    logger.info(f'After merging Invoice Line Item with Vendor Master Core, merged shape: {merged_df.shape}')
    if merged_df.shape[0]!= invoice_line_item.shape[0]:
        logger.warning("⚠️ Row count mismatch — possible data inflation after merge.")
    else:
        logger.info("✅ Line item counts consistent after merge. Invoice Line Item and Vendor Master Core merged successfully.")
    
    matched = merged_df['SUPPLIER_ID'].notna().sum()
    total = invoice_line_item.shape[0]
    logger.info(f"Vendor match coverage: {matched:,}/{total:,} ({matched/total:.2%})")

    # Check for null values in new columns post-merge
    logger.debug("Null values in merged DataFrame Invoice Line Item + Vendor Master Core columns:")
  
    new_columns = set(merged_df.columns) - set(invoice_line_item.columns)
    null_merged = merged_df[list(new_columns)].isnull().sum()
    logger.debug(null_merged[null_merged > 0])

    # Merge coverage check
    pre_rows = invoice_line_item.shape[0]
    post_rows = merged_df.shape[0]
    matched = merged_df['VENDOR_NAME'].notna()
    match_pct = 100 * matched.sum() / pre_rows
    logger.info(f"MERGE COVERAGE: INVOICE_LINE_ITEM + VENDOR_MASTER_CORE: {match_pct:.2f}% of line items found matching vendor details without null values.")

    return merged_df

def build_vendor_master_core(invoice_level_data:pd.DataFrame,lfa1:Optional[pd.DataFrame],
                              lfb1:Optional[pd.DataFrame],lfbk:Optional[pd.DataFrame],
                              lfm1:Optional[pd.DataFrame],vendor_list_to_be_updated)-> pd.DataFrame:
    from ..logger_config import get_logger
    logger = get_logger()
    
    # Validate invoice_level_data
    if invoice_level_data is None or invoice_level_data.empty:
        raise ValueError("Invoice Level DataFrame must be provided and cannot be empty")
    
    # Handle None for vendor tables - not needed for this pipeline mode
    if lfa1 is None or lfb1 is None:
        logger.info("Vendor master tables (LFA1/LFB1) not required for this pipeline mode, skipping merge")
        return invoice_level_data
    
    if lfbk is None or lfm1 is None:
        logger.info("Vendor bank tables (LFBK/LFM1) not required for this pipeline mode, skipping merge")
        return invoice_level_data
    
    # Validate vendor tables - if provided, must not be empty
    if lfa1.empty or lfb1.empty:
        raise ValueError("LFA1 and LFB1 DataFrames were expected but are empty - data loading failed")
    if lfbk.empty or lfm1.empty:
        raise ValueError("LFBK and LFM1 DataFrames were expected but are empty - data loading failed")
    

    
    vendor_master_core_df = merge_lfa1_lfb1(lfa1, lfb1)
    logger.info(f'Shape of Merged data, LFA1 + LFB1: {vendor_master_core_df.shape}')


    vendor_master_core_with_lfbk_df = merge_vendor_master_core_with_lfbk(vendor_master_core=vendor_master_core_df,
                                                               lfbk=lfbk)
    logger.info(f'Before merging with LFBK shape: {vendor_master_core_df.shape}')
    logger.info(f"Vendor Master Core after merging with LFBK shape: {vendor_master_core_with_lfbk_df.shape}")
    # print(vendor_master_core_df.columns.tolist())

    logger.info(f"Dropping duplicates in Vendor Master Core after LFBK merge if any..., shape before: {vendor_master_core_with_lfbk_df.shape}")
    vendor_master_core_with_lfbk_df.drop_duplicates(inplace=True,keep='first')
    logger.info(f"Shape after dropping duplicates in lfa1 + lfb1 + lfbk: {vendor_master_core_with_lfbk_df.shape}")

    unique_supplier_ids = invoice_level_data['SUPPLIER_ID'].unique()
    payer_ids = invoice_level_data['PAYER'].unique()
    unique_supplier_ids = set(unique_supplier_ids).union(set(payer_ids))
    logger.info(f'Unique supplier count in Invoice Level Data before filtering: {len(unique_supplier_ids)}')
    filtered_vm_lfbk_df = vendor_master_core_with_lfbk_df[vendor_master_core_with_lfbk_df['SUPPLIER_ID'].isin(unique_supplier_ids)]
    logger.info(f"Filtered Vendor Master Core to only include suppliers present in Invoice Level Data, shape: {filtered_vm_lfbk_df.shape}")
    logger.info(f'Unique supplier count after filtering: {filtered_vm_lfbk_df["SUPPLIER_ID"].nunique()}')


    # Check for Null Supplier id rows
    null_supplier_id_rows_inv_data = invoice_level_data['SUPPLIER_ID'].isna()
    if null_supplier_id_rows_inv_data.sum()>0:
        logger.warning('inv data has null supplier id rows')
        logger.info(f'Invoice data shape: {invoice_level_data.shape}')
        logger.warning(f'Null rows: {invoice_level_data[null_supplier_id_rows_inv_data].shape}')

        # Dropping null supplier id rows
        logger.info(f'Before dropping: {invoice_level_data.shape}')
        invoice_level_data = invoice_level_data[invoice_level_data['SUPPLIER_ID'].notna()]
        logger.info(f'After dropping: {invoice_level_data.shape}')

    # find overlapping
    set_invoice_suppliers = set(invoice_level_data['SUPPLIER_ID'].astype(int).unique())
    set_vm_suppliers = set(filtered_vm_lfbk_df['SUPPLIER_ID'].astype(int).unique())
    common_suppliers = set_invoice_suppliers.intersection(set_vm_suppliers)
    logger.info(f"Number of common suppliers between Invoice Level Data and Vendor Master Core after filtering: {len(common_suppliers)}")

    logger.debug(f'LFBK related columns: {lfbk.columns.tolist()}')

    # filename = f'vendor_master_core_with_lfbk_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
    # filtered_vm_lfbk_df.to_csv(filename, index=False)
    from .vendor_data_db import store_vendor_master_data_in_database
    
    # Only attempt to store vendor data if we have matching vendors
    if not filtered_vm_lfbk_df.empty:
        res = store_vendor_master_data_in_database(vendor_master_core_df=filtered_vm_lfbk_df,vendor_list_to_be_updated=vendor_list_to_be_updated)
        if res:
            logger.info("Vendor Master Core data stored successfully in the database.")
        else:
            logger.warning("Failed to store Vendor Master Core data in the database. Continuing with invoice processing without vendor enrichment.")
    else:
        logger.warning(f"No matching vendor data found for suppliers in invoice. Skipping vendor master database insert.")
    
    merged_df = merge_invoice_line_item_with_vendor_master_core(invoice_line_item=invoice_level_data,
                                                                vendor_master_core=vendor_master_core_df)
    logger.info(f'Shape of Merged data, Invoice Line Item + Vendor Master Core: {merged_df.shape}')

    return merged_df