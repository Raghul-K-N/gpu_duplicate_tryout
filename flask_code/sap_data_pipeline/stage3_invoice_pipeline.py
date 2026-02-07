"""
Stage 3: Invoice Pipeline
Assembles final invoice data by joining transactional and master data from parquet files.
This stage reads only from persistent parquet state and produces deterministic output.
"""
import os
import pandas as pd
from typing import Dict, Optional
from dotenv import load_dotenv
from .logger_config import get_logger
from .utils import EXPECTED_TABLES, Z_BLOCK_EXPECTED_TABLES

# Import existing merge functions
from .invoice_core.build_invoice_core_from_sap import build_invoice_core
from .company_code.company_master_lookup import merge_t001
from .payment_terms.payment_terms_lookup import merge_t052u
from .payment_method.payment_method_lookup import merge_t042z
from .payment_reason_code.payment_reason_code_lookup import merge_t053s
from .purchase_order.purchase_order_details_lookup import merge_po_info
from .vendor_master_core.vendor_master_lookup import build_vendor_master_core
from .vim_data.vim_data_lookup import merge_invoice_line_item_with_vim_data
from .doa.vrdoa_rename import VRDOA_RENAME_MAPPING
from .doa.doaredel_rename import DOA_REDEL_RENAME_MAPPING

load_dotenv()


def load_parquet_files(
    master_parquet_path: str,
    transactional_parquet_path: str,
    run_id: str,
    table_list: list,
    logger
) -> tuple[Dict[str, pd.DataFrame], list]:
    """
    Load all required parquet files from Stage 2 output.
    
    Loads from two sources:
    - Master data (persistent): master_parquet/<TABLE>/SAP_<TABLE>_data.parquet
    - Transactional data (run-scoped): transactional_parquet/<run_id>/SAP_<TABLE>_data.parquet
    
    Args:
        master_parquet_path: Path to master_parquet directory
        transactional_parquet_path: Path to transactional_parquet directory
        run_id: Run timestamp from Stage 1/2
        table_list: List of expected table names
        logger: Logger instance
    
    Returns:
        Tuple of (sap_data dict, missing_tables list)
    """
    from .utils import MASTER_FILES
    
    sap_data = {}
    missing_tables = []
    
    for table_name in table_list:
        clean_table_name = table_name.rstrip('_')
        
        # Determine source based on table type
        if table_name in MASTER_FILES:
            # Load from master parquet (persistent)
            parquet_path = os.path.join(
                master_parquet_path,
                clean_table_name,
                f'SAP_{clean_table_name}_data.parquet'
            )
            source_type = "master"
        else:
            # Load from transactional parquet (run-scoped)
            # Note: BKPF and Z_BKPF are stored separately to prevent data mixing
            parquet_path = os.path.join(
                transactional_parquet_path,
                run_id,
                f'SAP_{clean_table_name}_data.parquet'
            )
            source_type = "transactional"
        
        # Try to load the parquet file
        if os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                sap_data[table_name] = df
                logger.info(f" Loaded {table_name} ({source_type}): {df.shape}")
            except Exception as e:
                logger.error(f"Failed to load {table_name} from {parquet_path}: {e}")
                missing_tables.append(table_name)
        else:
            logger.warning(f"Parquet file not found for {table_name}: {parquet_path}")
            missing_tables.append(table_name)
    
    # Report missing tables (but don't raise error - let caller decide)
    if missing_tables:
        logger.warning(
            f"Missing {len(missing_tables)} table(s): {sorted(missing_tables)}"
        )
    else:
        logger.info(f" All {len(table_list)} expected tables loaded successfully")
    
    return sap_data, missing_tables


def build_invoice_data(sap_data: Dict[str, pd.DataFrame], is_zblock: bool, logger,vendor_list_to_be_updated) -> pd.DataFrame:
    """
    Build complete invoice DataFrame by joining all SAP tables.
    
    This function orchestrates all the merge operations in sequence.
    Pipeline-aware: Selects correct BKPF variant based on is_zblock flag:
    - ZBLOCK: uses Z_BKPF data
    - AP: uses normal BKPF data
    
    Args:
        sap_data: Dictionary of table name -> DataFrame
        is_zblock: Whether this is Z-block mode (determines BKPF table name)
        logger: Logger instance
    
    Returns:
        Complete invoice DataFrame with all enrichments
    """
    # Determine which BKPF variant to use
    if is_zblock:
        bkpf_table_name = 'Z_BKPF'
        logger.info("Using Z_BKPF data for ZBLOCK pipeline")
    else:
        bkpf_table_name = 'BKPF'
        logger.info("Using BKPF data for AP pipeline")
    
    # Step 1: Build invoice core
    logger.info("Building invoice core...")
    invoice_core_df = build_invoice_core(
        bseg=sap_data.get('BSEG'),
        bkpf=sap_data.get(bkpf_table_name),
        with_item=sap_data.get('WTH'),
        t003=sap_data.get('T003'),
        retinv=sap_data.get('RETINV'),
        udc=sap_data.get('UDC'),
    )
    logger.info(f"Invoice Core shape: {invoice_core_df.shape}")
    logger.debug(f"Invoice Core REGION_BSEG value counts:\n{invoice_core_df['REGION_BSEG'].value_counts()}")
    
    # Step 2: Merge company data
    logger.info("Merging company data (T001)...")
    invoice_df = merge_t001(
        invoice_df=invoice_core_df,
        t001=sap_data.get('T001')
    )
    logger.info(f"After T001 merge: {invoice_df.shape}")
    
    # Step 3: Merge payment terms
    logger.info("Merging payment terms (T052U)...")
    invoice_df = merge_t052u(
        invoice_df=invoice_df,
        t052u=sap_data.get('T052U')
    )
    logger.info(f"After T052U merge: {invoice_df.shape}")
    
    # Step 4: Merge payment method
    logger.info("Merging payment method (T042Z)...")
    invoice_df = merge_t042z(
        invoice_df=invoice_df,
        t042z=sap_data.get('T042Z')
    )
    logger.info(f"After T042Z merge: {invoice_df.shape}")
    
    # Step 5: Merge payment reason code
    logger.info("Merging payment reason code (T053S)...")
    invoice_df = merge_t053s(
        invoice_df=invoice_df,
        t053s=sap_data.get('T053S')
    )
    logger.info(f"After T053S merge: {invoice_df.shape}")
    
    # Step 6: Merge PO data
    logger.info("Merging purchase order data (EKKO, EKPO)...")
    invoice_df = merge_po_info(
        invoice_df=invoice_df,
        ekko_df=sap_data.get('EKKO'),
        ekpo_df=sap_data.get('EKPO')
    )
    logger.info(f"After PO merge: {invoice_df.shape}")
    
    # Step 7: Merge vendor master data
    logger.info("Merging vendor master data (LFA1, LFB1, LFBK, LFM1)...")
    invoice_df = build_vendor_master_core(
        invoice_level_data=invoice_df,
        lfa1=sap_data.get('LFA1'),
        lfb1=sap_data.get('LFB1'),
        lfbk=sap_data.get('LFBK'),
        lfm1=sap_data.get('LFM1'),
        vendor_list_to_be_updated=vendor_list_to_be_updated
    )
    logger.info(f"After vendor master merge: {invoice_df.shape}")
    
    # Step 8: Merge VIM data
    logger.info("Merging VIM invoice verification data...")
    invoice_df = merge_invoice_line_item_with_vim_data(
        invoice_line_item=invoice_df,
        vim_data=sap_data.get('VIM_'),
        vim_t100t=sap_data.get('VIMT100'),
        vim_t0101=sap_data.get('VIMT101'),
        vim_1log_comm=sap_data.get('1LOGCOMM'),
        vim_8log_comm=sap_data.get('8LOGCOMM'),
        vim_1log=sap_data.get('1LOG_'),
        vim_8log=sap_data.get('8LOG_'),
        vim_apr_log=sap_data.get('APRLOG')
    )
    logger.info(f"After VIM merge: {invoice_df.shape}")
    
    # Step 9: Rename DOCUMENT_NUMBER column if needed
    if 'DOCUMENT_NUMBER_Invoice' in invoice_df.columns:
        invoice_df.rename(columns={'DOCUMENT_NUMBER_Invoice': 'DOCUMENT_NUMBER'}, inplace=True)
        logger.debug("Renamed DOCUMENT_NUMBER_Invoice to DOCUMENT_NUMBER")
    
    # Step 10: Calculate TAX_AMOUNT
    logger.info("Calculating TAX_AMOUNT at account document level...")
    invoice_df = calculate_tax_amount(invoice_df, logger)
    
    return invoice_df


def calculate_tax_amount(invoice_df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Calculate TAX_AMOUNT at account document level.
    
    For each unique accounting document, sum the line items where LINE_ITEM_ID = 'T'.
    
    Args:
        invoice_df: Invoice DataFrame
        logger: Logger instance
    
    Returns:
        DataFrame with TAX_AMOUNT column added
    """
    # Create unique document identifier
    invoice_df['_temp_doc_id'] = (
        invoice_df['CLIENT'].astype(str) + '_' +
        invoice_df['COMPANY_CODE'].astype(str) + '_' +
        invoice_df['FISCAL_YEAR'].astype(str) + '_' +
        invoice_df['DOCUMENT_NUMBER'].astype(str)
    )
    
    # Calculate tax amount by document
    tax_by_doc = (
        invoice_df[invoice_df['LINE_ITEM_ID'] == 'T']
        .groupby('_temp_doc_id')['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY']
        .sum()
        .reset_index()
    )
    tax_by_doc.rename(columns={'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY': 'TAX_AMOUNT'}, inplace=True)
    
    # Merge back to main DataFrame
    invoice_df = invoice_df.merge(tax_by_doc, on='_temp_doc_id', how='left')
    invoice_df['TAX_AMOUNT'] = invoice_df['TAX_AMOUNT'].fillna(0)
    
    # Drop temporary column
    invoice_df.drop(columns=['_temp_doc_id'], inplace=True)
    
    logger.info(f"TAX_AMOUNT calculated. Non-zero tax records: {(invoice_df['TAX_AMOUNT'] != 0).sum()}")
    
    return invoice_df


def validate_data_quality(invoice_df: pd.DataFrame, logger):
    """
    Perform data quality checks on the final invoice DataFrame.
    
    Args:
        invoice_df: Invoice DataFrame
        logger: Logger instance
    """
    logger.info("=" * 80)
    logger.info("DATA QUALITY CHECKS")
    logger.info("=" * 80)
    
    # Check for nulls in critical columns
    critical_columns = [
        'ENTERED_DATE', 'POSTED_DATE', 'DUE_DATE', 'INVOICE_DATE',
        'VENDOR_NAME', 'SUPPLIER_ID'
    ]
    
    for col in critical_columns:
        if col in invoice_df.columns:
            null_count = invoice_df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Column {col} has {null_count} null values.")
            else:
                logger.info(f" Column {col} has no null values.")
        else:
            logger.warning(f"Column {col} not found in DataFrame")
    
    # Log data range
    if 'POSTED_DATE' in invoice_df.columns:
        logger.info(f"Data range: {invoice_df['POSTED_DATE'].min()} to {invoice_df['POSTED_DATE'].max()}")
    
    # Log unique document count
    if all(col in invoice_df.columns for col in ['CLIENT', 'COMPANY_CODE', 'FISCAL_YEAR', 'DOCUMENT_NUMBER']):
        unique_docs = invoice_df.groupby(['CLIENT', 'COMPANY_CODE', 'FISCAL_YEAR', 'DOCUMENT_NUMBER']).ngroups
        logger.info(f"Number of unique accounting documents: {unique_docs}")
    
    logger.info("=" * 80)


def process_doa_data(
    sap_data: Dict[str, pd.DataFrame],
    base_path: str,
    master_folder_name: str,
    doa_parquet_folder_name: str,
    logger
):
    """
    Process and save DOA (Delegation of Authority) data.
    
    Only called when is_zblock=True.
    
    Args:
        sap_data: Dictionary of table name -> DataFrame
        base_path: Base uploads directory
        master_folder_name: Master folder name
        doa_parquet_folder_name: DOA parquet folder name
        logger: Logger instance
    """
    doa_base_path = os.path.join(base_path, master_folder_name, doa_parquet_folder_name)
    os.makedirs(doa_base_path, exist_ok=True)
    
    # Process VRDOA
    doa_df = sap_data.get('VRDOA')
    if doa_df is not None and not doa_df.empty:
        logger.info(f"Processing VRDOA: {doa_df.shape}")
        doa_df = doa_df.copy()
        doa_df.rename(columns=VRDOA_RENAME_MAPPING, inplace=True)
        
        # Fix GL_ACCOUNT_1 conversion issue
        if 'GL_ACCOUNT_1' in doa_df.columns:
            doa_df['GL_ACCOUNT_1'] = pd.to_numeric(doa_df['GL_ACCOUNT_1'], errors='coerce').fillna(0).astype(int)
        
        doa_file_path = os.path.join(doa_base_path, 'doa_data.parquet')
        doa_df.to_parquet(doa_file_path,engine='pyarrow', index=False)
        logger.info(f" Saved VRDOA to: {doa_file_path}")
    else:
        logger.warning("VRDOA table is empty or not found")
    
    # Process DOAREDEL
    doa_redel_df = sap_data.get('DOAREDEL')
    if doa_redel_df is not None and not doa_redel_df.empty:
        logger.info(f"Processing DOAREDEL: {doa_redel_df.shape}")
        doa_redel_df = doa_redel_df.copy()
        doa_redel_df.rename(columns=DOA_REDEL_RENAME_MAPPING, inplace=True)
        
        # Fix GL_ACCOUNT_1 conversion issue
        if 'GL_ACCOUNT_1' in doa_redel_df.columns:
            doa_redel_df['GL_ACCOUNT_1'] = pd.to_numeric(doa_redel_df['GL_ACCOUNT_1'], errors='coerce').fillna(0).astype(int)
        
        doa_redel_file_path = os.path.join(doa_base_path, 'doa_redelivery_data.parquet')
        doa_redel_df.to_parquet(doa_redel_file_path,engine='pyarrow',    index=False)
        logger.info(f" Saved DOAREDEL to: {doa_redel_file_path}")
    else:
        logger.warning("DOAREDEL table is empty or not found")


def assemble_invoices(
    stage2_result: Dict,
    is_zblock: bool = False
) -> Dict:
    """
    Stage 3: Invoice Pipeline - Assemble final invoice data.
    
    Reads parquet files from Stage 2, performs joins and enrichments,
    and outputs final invoice data.
    
    Args:
        stage2_result: Output dictionary from Stage 2 ingest_and_update_state()
        is_zblock: Whether this is Z-block mode
    
    Returns:
        Dictionary containing:
        - status: 'success' or 'failure'
        - run_id: Timestamp identifier
        - invoice_output_path: Path to invoice parquet file
        - invoice_row_count: Number of invoice records
        - doa_processed: Whether DOA data was processed (Z-block only)
    """
    logger = get_logger()
    logger.info("=" * 80)
    logger.info("STAGE 3: INVOICE PIPELINE STARTED")
    logger.info("=" * 80)
    
    try:
        # Extract paths from Stage 2 result
        run_id = stage2_result['run_id']
        master_parquet_path = stage2_result['master_parquet_path']
        vendor_list_to_be_updated = stage2_result.get('vendor_list_to_be_updated', [])
        
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        logger.info(f"Master Parquet Path: {master_parquet_path}")
        
        # Get environment configuration
        base_path = os.getenv('UPLOADS', None)
        master_folder_name = os.getenv('MASTER_FOLDER', 'dow_transformation')
        transactional_parquet_folder_name = os.getenv('TRANSACTIONAL_PARQUET_FOLDER', 'transactional_parquet')
        doa_parquet_folder_name = os.getenv('DOA_PARQUET_PATH', 'doa_parquet')
        invoice_output_folder_name = os.getenv('INVOICE_OUTPUT_FOLDER', 'invoice_output')
        
        if base_path is None:
            error_msg = "UPLOADS environment variable is not set."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Get transactional parquet path
        transactional_parquet_path = os.path.join(base_path, master_folder_name, transactional_parquet_folder_name)
        logger.info(f"Transactional Parquet Path: {transactional_parquet_path}")
        
        # Select table list based on pipeline mode
        table_list = Z_BLOCK_EXPECTED_TABLES
        logger.info(f"Expected tables: {len(table_list)}")
        
        # Step 1: Load all parquet files
        logger.info("-" * 80)
        logger.info("LOADING PARQUET FILES")
        logger.info("-" * 80)
        sap_data, missing_tables = load_parquet_files(
            master_parquet_path=master_parquet_path,
            transactional_parquet_path=transactional_parquet_path,
            run_id=run_id,
            table_list=table_list,
            logger=logger
        )
        
        # Validate mandatory tables (BSEG and pipeline-specific BKPF variant)
        # These are the only truly required tables - all others are optional enrichments
        # For ZBLOCK: Z_BKPF is required; For AP: BKPF is required
        bkpf_variant = 'Z_BKPF' if is_zblock else 'BKPF'
        mandatory_tables = ['BSEG', bkpf_variant]
        missing_mandatory = [t for t in mandatory_tables if t not in sap_data]
        
        if missing_mandatory:
            error_msg = (
                f"Mandatory tables missing: {missing_mandatory}. "
                f"BSEG and {bkpf_variant} are required for invoice processing."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f" Mandatory tables present: {mandatory_tables}")
        
        # Log optional tables status
        if missing_tables:
            optional_missing = [t for t in missing_tables if t not in mandatory_tables]
            if optional_missing:
                logger.warning(
                    f"Optional enrichment tables missing ({len(optional_missing)}): {sorted(optional_missing)}. "
                    f"Pipeline will continue with available data."
                )
        
        # Step 2: Build invoice data
        logger.info("-" * 80)
        logger.info("BUILDING INVOICE DATA")
        logger.info("-" * 80)
        invoice_df = build_invoice_data(sap_data=sap_data, is_zblock=is_zblock, 
                                        logger=logger, vendor_list_to_be_updated=vendor_list_to_be_updated)
        logger.info(f"Final invoice DataFrame shape: {invoice_df.shape}")
        logger.info(f"Final invoice DataFrame columns: {len(invoice_df.columns)}")
        
        # Step 3: Data quality checks
        validate_data_quality(invoice_df, logger)


        invoice_df['ref_unique_id'] = (
            invoice_df['CLIENT'].astype(str) + '_' +
            invoice_df['COMPANY_CODE'].astype(str) + '_' +
            invoice_df['FISCAL_YEAR'].astype(str) + '_' +
            invoice_df['DOCUMENT_NUMBER'].astype(str)
        )
        logger.info(f'Region value counts : {invoice_df["REGION_BKPF"].value_counts(dropna=False)}')
        logger.info(f"NO of rows in the final invoice dataframe: {len(invoice_df)}")
        logger.info(f"NO of unique ACCOUNTING DOCUMENTS in the final invoice dataframe: {invoice_df.groupby(['ref_unique_id']).ngroups}")
        logger.info(f"Count of acc docs for each Regions present in the final invoice dataframe: {invoice_df.groupby('REGION_BKPF')['ref_unique_id'].nunique()}")
        logger.info(f"List of all column names in the final invoice dataframe: {invoice_df.columns.tolist()}")
        
        
        # Step 4: Save invoice output
        logger.info("-" * 80)
        logger.info("SAVING INVOICE OUTPUT")
        logger.info("-" * 80)
        invoice_output_path = os.path.join(base_path, master_folder_name, invoice_output_folder_name, run_id)
        os.makedirs(invoice_output_path, exist_ok=True)
        
        # Determine filename based on pipeline mode
        invoice_filename = 'zblock_invoice_data.parquet' if is_zblock else 'invoice_data.parquet'
        invoice_file_path = os.path.join(invoice_output_path, invoice_filename)
        invoice_df.to_parquet(invoice_file_path, index=False, engine='pyarrow')
        logger.info(f" Saved {invoice_filename} to: {invoice_file_path}")
        
        # Step 5: Process DOA data (Z-block only)
        doa_processed = False
        if is_zblock:
            logger.info("-" * 80)
            logger.info("PROCESSING DOA DATA")
            logger.info("-" * 80)
            process_doa_data(
                sap_data=sap_data,
                base_path=base_path,
                master_folder_name=master_folder_name,
                doa_parquet_folder_name=doa_parquet_folder_name,
                logger=logger
            )
            doa_processed = True
        else:
            logger.info("Skipping DOA processing (AP mode)")
        
        # Summary
        logger.info("=" * 80)
        logger.info("STAGE 3 SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Invoice records: {len(invoice_df)}")
        logger.info(f"Invoice columns: {len(invoice_df.columns)}")
        logger.info(f"Output location: {invoice_output_path}")
        logger.info(f"DOA processed: {doa_processed}")
        logger.info("=" * 80)
        logger.info("STAGE 3: INVOICE PIPELINE COMPLETED")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'run_id': run_id,
            'invoice_output_path': invoice_file_path,
            'invoice_row_count': len(invoice_df),
            'invoice_column_count': len(invoice_df.columns),
            'doa_processed': doa_processed,
            'is_zblock': is_zblock,
            "acc_docs_count": invoice_df.groupby(['ref_unique_id']).ngroups
        }
        
    except Exception as e:
        logger.exception(f"Error in Stage 3 Invoice Pipeline: {e}")
        return {
            'status': 'failure',
            'error': str(e),
            'run_id': stage2_result.get('run_id', 'unknown')
        }
