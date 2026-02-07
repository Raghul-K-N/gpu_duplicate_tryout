"""
Stage 2: SAP Ingestion & State Update
Builds persistent, deduplicated parquet datasets for each SAP master table.
This stage creates the single source of truth across all scheduled runs.
"""
import os
import pandas as pd
from typing import Dict, List
from dotenv import load_dotenv
from .logger_config import get_logger
from .utils import MASTER_FILES,DIRECT_APPEND_TABES,REPLACE_OLD_ROW_WITH_NEW_ROW_TABLES
from .stage2_helpers import (
    UNIQUE_COLUMNS_MAP,
    get_master_parquet_path,
    get_table_parquet_path,
    harmonize_single_dataframe_vectorized,
    read_files_for_table,
    filter_out_duplicate_data,
    get_transactional_parquet_path,
    delete_source_files,
    special_case_handle_for_lfbk,
    replace_old_rows_with_new_rows,
    create_synthetic_bseg_from_bkpf
)

load_dotenv()


def process_master_file_table(
    table_name: str,
    new_df: pd.DataFrame,
    master_parquet_path: str,
    append_new_rows: bool,
    sap_run_folder: str,
    source_files: List[str],
    logger
) -> Dict:
    """
    Process a master file table: read existing parquet, deduplicate, and save.
    
    This function handles the master file ingestion logic for MASTER_FILES tables.
    
    Args:
        table_name: SAP table name
        new_df: New data DataFrame from current run
        master_parquet_path: Path to master_parquet directory
        append_new_rows: If True, append deduplicated rows; if False, placeholder for future logic
        logger: Logger instance
    
    Returns:
        Dictionary with processing statistics
    """
    logger.info(f"Processing MASTER FILE table: {table_name}")
    
    # Get parquet file path
    parquet_path = get_table_parquet_path(master_parquet_path, table_name)
    
    # always will be True
    if append_new_rows:
        # Mode: Append new unique rows to existing data
        logger.info(f"Append mode: Processing new data for {table_name}")
        
        # Check if parquet file exists
        parquet_exists = os.path.exists(parquet_path)
        
        if not parquet_exists:
            # No existing file: save all new data
            logger.info(f"No existing parquet for {table_name}, saving all new data")
            new_df.to_parquet(parquet_path, index=False, engine='pyarrow')
            logger.info(f" Saved new parquet for {table_name}: {parquet_path}")
            res = delete_source_files(source_files=source_files, sap_run_folder=sap_run_folder, logger=logger)
            
            logger.info(f"  Deleted {len(res)}/{len(source_files)} source file(s) after saving new parquet")
            return {
                'existing_rows': 0,
                'new_rows_received': len(new_df),
                'new_rows_added': len(new_df),
                'total_rows': len(new_df),
                'mode': 'new_file'
            }
        
        else:
            # Existing file: load, deduplicate, concatenate, save
            # For each table, specific operations needs to be done...
            # For LFBK - dont check for duplicates,  replace old vendor bank details with new ones
            # for EKKO, EKPO - if a row exists with same PO number, client - replace old with new
            # for otherss - check for duplicates and append new data
            
            # Initialize matching_vendors_list as empty - only LFBK will populate it
            matching_vendors_list = []
            
            logger.info(f"Loading existing parquet for {table_name}")
            try:
                existing_df = pd.read_parquet(parquet_path)
                logger.info(f"Loaded existing data: {len(existing_df)} rows")
            except Exception as e:
                logger.error(f"Failed to load existing parquet for {table_name}: {e}")
                logger.warning(f"Treating as new file and saving new data")
                new_df.to_parquet(parquet_path, index=False, engine='pyarrow')

                res = delete_source_files(source_files=source_files, sap_run_folder=sap_run_folder, logger=logger)

                logger.info(f"  Deleted {len(res)}/{len(source_files)} source file(s) after saving new parquet")
                return {
                    'existing_rows': 0,
                    'new_rows_received': len(new_df),
                    'new_rows_added': len(new_df),
                    'total_rows': len(new_df),
                    'mode': 'new_file_after_error'
                }
            
            # Based on Table Name, Decide what to do
            if table_name == 'LFBK':
                modified_df, matching_vendors_list = special_case_handle_for_lfbk(existing_df,new_df,logger)
                unique_new_df = pd.DataFrame()  # all new rows are already handled
            elif table_name in REPLACE_OLD_ROW_WITH_NEW_ROW_TABLES:
               modified_df  = replace_old_rows_with_new_rows(existing_df,new_df,logger,table_name)
               unique_new_df = pd.DataFrame()  # all new rows are already handled
            else:
                # Filter out duplicates using business keys and align data types
                modified_df = pd.DataFrame()  # placeholder
                unique_new_df = filter_out_duplicate_data(existing_df, new_df, table_name, logger)
                
            if len(unique_new_df) == 0 and modified_df.empty:
                logger.info(f"No new unique rows for {table_name}, keeping existing data unchanged")
                return {
                    'existing_rows': len(existing_df),
                    'new_rows_received': len(new_df),
                    'new_rows_added': 0,
                    'total_rows': len(existing_df),
                    'mode': 'no_new_data'
                }
            
            # Ensure column alignment before concatenation
            # Add missing columns to unique_new_df with NaN
            if modified_df.empty:
                for col in existing_df.columns:
                    if col not in unique_new_df.columns:
                        unique_new_df[col] = pd.NA
                        logger.debug(f"Added missing column '{col}' to new data for {table_name}")
            else:
                for col in existing_df.columns:
                    if col not in modified_df.columns:
                        modified_df[col] = pd.NA
                        logger.debug(f"Added missing column '{col}' to modified data for {table_name}")

            if modified_df.empty:   
                # Reorder columns to match existing (safe approach with reindex)
                unique_new_df = unique_new_df.reindex(columns=existing_df.columns)
            else:
                modified_df = modified_df.reindex(columns=existing_df.columns)
            
            if not unique_new_df.empty:
                # Harmonize dtypes before concatenation to prevent PyArrow type errors
                # harmonized_dfs = harmonize_dataframe_list([existing_df, unique_new_df], logger)
                
                # Concatenate existing + unique new data
                updated_df = pd.concat([existing_df, unique_new_df], ignore_index=True)
                try:
                    updated_df = harmonize_single_dataframe_vectorized(df=updated_df, logger=logger)
                except Exception as e:
                    logger.error(f"Error during harmonization for {table_name}: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    raise Exception(f"Harmonization failed for {table_name}: {e}")
                
                logger.info(f"Combined data for {table_name}: {len(updated_df)} total rows")
            else:
                updated_df = modified_df
                logger.info(f"Updated data for {table_name} after replacements: {len(updated_df)} total rows")
            
            # Force problematic bank/account identifier columns to STRING before Parquet write
            # These columns often contain alphanumeric identifiers that PyArrow can't infer correctly
            bank_identifier_columns = ['Bank Account', 'Bank Key', 'Account Number', 'IBAN']
            for col in bank_identifier_columns:
                if col in updated_df.columns:
                    updated_df[col] = updated_df[col].astype(str)
                    logger.debug(f"Forced column '{col}' to STRING type for {table_name}")
                
            # Save updated data to parquet
            updated_df.to_parquet(parquet_path, index=False, engine='pyarrow')
            logger.info(f" Saved updated parquet for {table_name}")


            
            # Return different stats based on processing mode
            if not unique_new_df.empty:
                # For deduplication mode (filter_out_duplicate_data)
                return {
                    'existing_rows': len(existing_df),
                    'new_rows_received': len(new_df),
                    'new_rows_added': len(unique_new_df),
                    'total_rows': len(updated_df),
                    'mode': 'append_deduplicated',
                    'matching_vendors_list': matching_vendors_list
                }
            else:
                # For replacement mode (LFBK or REPLACE tables)
                return {
                    'existing_rows': len(existing_df),
                    'new_rows_received': len(new_df),
                    'new_rows_added': len(updated_df) - len(existing_df),  # Net change in rows
                    'total_rows': len(updated_df),
                    'mode': 'replaced' if table_name == 'LFBK' else 'replaced_by_business_key',
                    'matching_vendors_list': matching_vendors_list
                }
    
    else:
        # Mode: Placeholder for future logic
        logger.info(f"append_new_rows=False: Placeholder mode for {table_name}")
        logger.warning(f"Future logic to be implemented for non-append mode")
        
        # TODO: Add future logic here when requirements are defined
        # For now, return empty stats
        return {
            'existing_rows': 0,
            'new_rows_received': len(new_df),
            'new_rows_added': 0,
            'total_rows': 0,
            'mode': 'placeholder_not_implemented'
        }


def process_transactional_table(
    table_name: str,
    new_df: pd.DataFrame,
    transactional_parquet_path: str,
    run_id: str,
    source_files: List[str],
    sap_run_folder: str,
    logger
) -> Dict:
    """
    Process transactional table: convert to parquet and delete source files.
    
    Saves parquet in flat structure: transactional_parquet/<run_id>/SAP_<TABLE>_data.parquet
    Deletes source Excel/CSV files after successful conversion.
    
    Args:
        table_name: SAP table name (e.g., 'BKPF', 'VIM_')
        new_df: Combined DataFrame from all files for this table
        transactional_parquet_path: Base path for transactional parquet
        run_id: Current run timestamp (from Stage 1)
        source_files: List of original filenames for this table
        sap_run_folder: Path to run-scoped SAP folder (to delete files)
        logger: Logger instance
    
    Returns:
        Dictionary with processing statistics
    """
    logger.info(f"Processing TRANSACTIONAL table: {table_name}")
    
    # Create run-scoped folder (flat structure - all files directly in timestamp folder)
    run_parquet_folder = os.path.join(transactional_parquet_path, run_id)
    os.makedirs(run_parquet_folder, exist_ok=True)
    
    # Clean table name (remove trailing underscores)
    clean_table_name = table_name.rstrip('_')
    
    # Save as parquet directly in timestamp folder (flat structure)
    parquet_path = os.path.join(run_parquet_folder, f'SAP_{clean_table_name}_data.parquet')
    
    try:
        # Save DataFrame to parquet
        new_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        logger.info(f" Saved transactional parquet for {table_name}: {parquet_path}")
        
        # Delete original Excel/CSV files after successful conversion
        deleted_files = delete_source_files(source_files, sap_run_folder, logger)
        
        if deleted_files:
            logger.info(f"  Deleted {len(deleted_files)}/{len(source_files)} source file(s)")
        
        return {
            'rows': len(new_df),
            'columns': len(new_df.columns),
            'parquet_path': parquet_path,
            'source_files_deleted': deleted_files,
            'mode': 'transactional_converted'
        }
        
    except Exception as e:
        logger.error(f"Failed to save parquet for {table_name}: {e}")
        logger.warning(f"Original files NOT deleted due to conversion error")
        raise


def ingest_and_update_state(
    stage1_result: Dict,
    is_zblock: bool = False,
    append_new_rows: bool = True,
    create_synthetic_bseg_data: bool = False
) -> Dict:
    """
    Stage 2: SAP Ingestion & State Update
    
    For each SAP table:
    1. Read new files from the run-scoped folder
    2. If table is a MASTER_FILE:
       - Load existing persistent parquet (if exists)
       - Align data types between existing and new data
       - Filter out duplicates using business keys
       - Append unique new rows to existing data (if append_new_rows=True)
       - Save updated data back to persistent parquet
    3. If table is NOT a MASTER_FILE:
       - Placeholder for future logic (transactional tables)
    
    Args:
        stage1_result: Output dictionary from Stage 1 classify_files()
        is_zblock: Whether this is Z-block mode (affects table list)
        append_new_rows: If True, append deduplicated rows; if False, placeholder for future logic
    
    Returns:
        Dictionary containing:
        - run_id: Timestamp identifier
        - master_parquet_path: Path to master parquet directory
        - tables_processed: List of table names processed
        - tables_updated: Dict mapping table name to update stats
        - tables_failed: List of tables that failed processing
        - total_tables: Total number of tables processed
    """
    logger = get_logger()
    logger.info("=" * 80)
    logger.info("STAGE 2: SAP INGESTION & STATE UPDATE STARTED")
    logger.info("=" * 80)
    
    
    # Extract info from Stage 1 result
    run_id = stage1_result['run_id']
    sap_run_folder = os.path.normpath(stage1_result['sap_run_folder'])  # Normalize path to handle Windows mixed slashes
    sap_files_by_table = stage1_result['sap_files_by_table']
    
    logger.info(f"Run ID: {run_id}")
    logger.info(f"SAP Run Folder: {sap_run_folder}")
    logger.info(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
    logger.info(f"Append New Rows Mode: {append_new_rows}")
    logger.info(f"Tables with new files: {len(sap_files_by_table)}")
    
    # Get environment configuration
    base_path = os.getenv('UPLOADS', None)
    master_folder_name = os.getenv('MASTER_FOLDER', 'dow_transformation')
    master_parquet_folder_name = os.getenv('MASTER_PARQUET_FOLDER', 'master_parquet')
    transactional_parquet_folder_name = os.getenv('TRANSACTIONAL_PARQUET_FOLDER', 'transactional_parquet')
    
    if base_path is None:
        error_msg = "UPLOADS environment variable is not set."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Get master parquet path
    master_parquet_path = get_master_parquet_path(base_path, master_folder_name, master_parquet_folder_name)
    os.makedirs(master_parquet_path, exist_ok=True)
    logger.info(f"Master Parquet Path: {master_parquet_path}")
    
    # Get transactional parquet path
    transactional_parquet_path = get_transactional_parquet_path(base_path, master_folder_name, transactional_parquet_folder_name)
    os.makedirs(transactional_parquet_path, exist_ok=True)
    logger.info(f"Transactional Parquet Path: {transactional_parquet_path}")
    
    # Track processing results
    tables_updated = {}
    tables_failed = []
    tables_processed = []
    vendor_list_to_be_updated = []
    
    # Process each table that has new files
    for table_name, filenames in sap_files_by_table.items():
        logger.info("-" * 80)
        logger.info(f"Processing Table: {table_name}")
        logger.info(f"New files: {len(filenames)}")       
        
        try:
            # Step 1: Read new files for this table
            new_df = read_files_for_table(sap_run_folder, table_name, filenames, logger)
            print(f"DEBUG Stage2: read_files_for_table returned, new_df type = {type(new_df)}")
            
            if new_df is None:
                print(f"DEBUG Stage2: new_df is None for {table_name}")
                logger.warning(f"No valid data loaded for {table_name}, skipping")
                continue
            
            if len(new_df) == 0:
                print(f"DEBUG Stage2: new_df is empty for {table_name}")
                logger.warning(f"No valid data loaded for {table_name}, skipping")
                continue
            
            print(f"DEBUG Stage2: new_df shape = {new_df.shape} for {table_name}")
            logger.info(f"Loaded new data for {table_name}: {len(new_df)} rows, {len(new_df.columns)} columns")
            
            # Step 2: Check if this is a MASTER_FILE table
            print(f"DEBUG Stage2: Checking if {table_name} is in MASTER_FILES")
            if table_name in MASTER_FILES:
                print(f"DEBUG Stage2: {table_name} IS a MASTER_FILE - processing")
                # Process as master file
                stats = process_master_file_table(
                    table_name=table_name,
                    new_df=new_df,
                    master_parquet_path=master_parquet_path,
                    append_new_rows=append_new_rows,
                    logger=logger,
                    sap_run_folder=sap_run_folder,
                    source_files=filenames
                )
                print(f"DEBUG Stage2: process_master_file_table returned stats: {stats}")
                
                stats['files_processed'] = len(filenames)
                tables_updated[table_name] = stats
                tables_processed.append(table_name)
                if 'matching_vendors_list' in stats:
                    logger.info(f"Matching Vendors List , needs to be updated for {table_name}: {len(stats['matching_vendors_list'])}")
                    vendor_list_to_be_updated.extend(stats['matching_vendors_list'])
                
            else:
                print(f"DEBUG Stage2: {table_name} is TRANSACTIONAL - processing")
                # NOT a master file - process as transactional table
                logger.info(f"{table_name} is a TRANSACTIONAL table")
                stats = process_transactional_table(
                    table_name=table_name,
                    new_df=new_df,
                    transactional_parquet_path=transactional_parquet_path,
                    run_id=run_id,
                    source_files=filenames,
                    sap_run_folder=sap_run_folder,
                    logger=logger
                )
                print(f"DEBUG Stage2: process_transactional_table returned stats: {stats}")
                
                stats['files_processed'] = len(filenames)
                tables_updated[table_name] = stats
                tables_processed.append(table_name)
                
        except Exception as e:
            print(f"DEBUG Stage2: EXCEPTION in table processing loop for {table_name}:")
            print(f"  Type: {type(e).__name__}")
            print(f"  Message: {str(e)}")
            import traceback
            print(f"  Traceback:\n{traceback.format_exc()}")
            logger.error(f"Error processing table {table_name}: {e}")
            tables_failed.append(table_name)
            continue
    
    if (create_synthetic_bseg_data and 'BKPF' in tables_processed and 'BSEG' not in tables_processed):
        logger.info("Synthetic BSEG data creation enabled - BKPF found, BSEG missing")
        
        bkpf_parquet_path = os.path.join(transactional_parquet_path, run_id, 'SAP_BKPF_data.parquet')
        logger.info(f"DEBUG Stage2: BKPF path = {bkpf_parquet_path}")
        logger.info(f"DEBUG Stage2: run_id = {run_id}")
        logger.info(f"DEBUG Stage2: master_parquet_path = {master_parquet_path}")
        
        res = create_synthetic_bseg_from_bkpf(logger=logger, bkpf_path=bkpf_parquet_path, master_parquet_path=master_parquet_path)
        
        # Check if synthetic BSEG creation was successful
        if res:  # res is True (boolean)
            logger.info(f"Successfully created synthetic BSEG")
            
            # Add BSEG to tracking lists (matching transactional table pattern)
            # Using dummy values since downstream flow only needs table name and mode
            tables_updated['BSEG'] = {
                'rows': 0,
                'mode': 'synthetic_from_bkpf',
                'parquet_path': 'SAP_BSEG_data.parquet', 
                'source': 'synthetic',
                'files_processed': 1
            }
            tables_processed.append('BSEG')
        else:
            logger.error("Failed to create synthetic BSEG")
            tables_failed.append('BSEG')
    else:
        logger.info("Synthetic BSEG data creation not requested or conditions not met, skipping.")
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 2 SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tables processed: {len(tables_processed)}")
    logger.info(f"Total tables updated: {len(tables_updated)}")
    logger.info(f"Total tables failed: {len(tables_failed)}")
    
    if tables_updated:
        logger.info("\nTables Updated:")
        for table, stats in tables_updated.items():
            if 'total_rows' in stats:
                # Master table stats
                logger.info(
                    f"  {table} (Master): {stats['existing_rows']} existing + {stats['new_rows_added']} new "
                    f"= {stats['total_rows']} total rows (mode: {stats['mode']})"
                )
            else:
                # Transactional table stats
                logger.info(
                    f"  {table} (Trans): {stats['rows']} rows converted (mode: {stats['mode']})"
                )
    
    if tables_failed:
        logger.warning(f"\nTables Failed: {tables_failed}")
    
    logger.info("=" * 80)
    logger.info("STAGE 2: SAP INGESTION & STATE UPDATE COMPLETED")
    logger.info("=" * 80)
    
    return {
        'run_id': run_id,
        'master_parquet_path': master_parquet_path,
        'tables_processed': tables_processed,
        'tables_updated': tables_updated,
        'tables_failed': tables_failed,
        'total_tables': len(tables_processed),
        'is_zblock': is_zblock,
        'append_new_rows': append_new_rows,
        'vendor_list_to_be_updated': vendor_list_to_be_updated,
        'status':'success'
    }
