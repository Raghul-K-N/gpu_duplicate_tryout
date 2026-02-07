"""
Bootstrap SAP Master Data Setup
One-time initialization of master parquet files from raw SAP data.
Processes only MASTER_FILES tables, skips transactional tables (BKPF, VIM_, etc).
No database interactions, no file deletions.
"""
import os
import sys
import pandas as pd
from typing import Dict, List
from datetime import datetime, timezone
from dotenv import load_dotenv

from .stage1_file_classifier import classify_files
from .stage2_sap_ingestion import ingest_and_update_state
from .logger_config import get_logger, setup_logger
from .utils import MASTER_FILES

load_dotenv()


def bootstrap_sap_setup(incoming_folder: str, pipeline_mode: str) -> Dict:
    """
    One-time bootstrap endpoint to initialize master parquet files.
    
    Executes Stage 1 (File Classification) + Stage 2 (Master-only Ingestion).
    Skips all transactional tables (BKPF, VIM_, etc) and leaves their files untouched.
    
    Args:
        incoming_folder: Path to folder containing raw SAP files
        pipeline_mode: 'AP' or 'ZBLOCK'
    
    Returns:
        Dictionary with bootstrap results:
        {
            "status": "success" | "failure",
            "bootstrap_timestamp": "20250123120000",
            "message": "...",
            "master_tables_processed": [...],
            "transactional_tables_skipped": [...],
            "master_data_summary": {...},
            "total_time_seconds": 45.67
        }
    """
    start_time = datetime.now(timezone.utc)
    is_zblock = pipeline_mode == 'ZBLOCK'
    
    try:
        # Normalize path to handle Windows mixed slashes
        incoming_folder = os.path.normpath(incoming_folder)
        
        # Initialize logger
        # logger = setup_logger()
        print("=" * 100)
        print("SAP MASTER DATA BOOTSTRAP STARTED (ONE-TIME SETUP)")
        print("=" * 100)
        print(f"Incoming Folder: {incoming_folder}")
        print(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        
        # ========================================================================
        # STAGE 1: FILE CLASSIFICATION
        # ========================================================================
        print("\n" + "=" * 100)
        print("STAGE 1: FILE CLASSIFICATION")
        print("=" * 100)
        stage1_start = datetime.now(timezone.utc)
        print('Stage 1: Classifying files...')
        stage1_result = None
        try:
            stage1_result = classify_files(
                incoming_folder=incoming_folder,
                is_zblock=is_zblock,
                skip_bkpf_files=True # Skip BKPF files during One time Setup
            )
        except Exception as e:
            print(f"ERROR in Stage 1 File Classification: {str(e)}")
            import traceback
            print(f"Traceback:\n{traceback.format_exc()}")
            raise
        
        if stage1_result is None:
            print(f"ERROR: stage1_result is None after calling classify_files!")
            sys.stdout.flush()
            raise ValueError("classify_files returned None")
        
        print('Stage 1: File classification completed.')
        sys.stdout.flush()
        print('Stage 1 Result:',stage1_result)
        sys.stdout.flush()
        print(f'DEBUG: type(stage1_result) = {type(stage1_result)}')
        sys.stdout.flush()
        print(f'DEBUG: stage1_result.keys() = {stage1_result.keys() if isinstance(stage1_result, dict) else "NOT A DICT"}')
        sys.stdout.flush()
        
        if not isinstance(stage1_result, dict):
            print(f"ERROR: stage1_result is not a dict! It is {type(stage1_result)}")
            sys.stdout.flush()
            raise ValueError(f"Stage 1 returned non-dict: {stage1_result}")
        
        if 'sap_files_by_table' not in stage1_result:
            print(f"ERROR: 'sap_files_by_table' not in stage1_result!")
            print(f"Available keys: {stage1_result.keys()}")
            sys.stdout.flush()
            raise KeyError("Stage 1 result missing 'sap_files_by_table' key")
        
        stage1_end = datetime.now(timezone.utc)
        stage1_duration = (stage1_end - stage1_start).total_seconds()
        
        print(f" Stage 1 completed in {stage1_duration:.2f} seconds")
        print(f"  Run ID: {stage1_result['run_id']}")
        print(f"  SAP files found: {stage1_result['sap_file_count']}")
        print(f"  Attachments found: {stage1_result['attachment_count']}")
        
        # ========================================================================
        # STAGE 2: MASTER-ONLY PROCESSING (Skip Transactional)
        # ========================================================================
        print("\n" + "=" * 100)
        print("STAGE 2: MASTER DATA INGESTION (TRANSACTIONAL TABLES SKIPPED)")
        print("=" * 100)
        stage2_start = datetime.now(timezone.utc)
        
        print(f"DEBUG: Checking stage1_result keys before Stage 2:")
        print(f"  - Has run_id? {'run_id' in stage1_result}")
        print(f"  - Has sap_run_folder? {'sap_run_folder' in stage1_result}")
        print(f"  - Has sap_files_by_table? {'sap_files_by_table' in stage1_result}")
        print(f"  - sap_files_by_table value: {stage1_result.get('sap_files_by_table', 'KEY NOT FOUND')}")
        sys.stdout.flush()
        
        # Execute full Stage 2 (processes both master and transactional)
        print(f"DEBUG: About to call ingest_and_update_state with:")
        print(f"  - stage1_result['run_id']: {stage1_result['run_id']}")
        print(f"  - stage1_result['sap_run_folder']: {stage1_result['sap_run_folder']}")
        print(f"  - stage1_result['sap_files_by_table']: {len(stage1_result['sap_files_by_table'])} tables")
        print(f"  - is_zblock: {is_zblock}")
        sys.stdout.flush()
        
        try:
            stage2_full_result = ingest_and_update_state(
                stage1_result=stage1_result,
                is_zblock=is_zblock,
                append_new_rows=True
            )
            print(f"DEBUG: Stage 2 completed successfully")
            print(f"DEBUG: stage2_full_result keys: {stage2_full_result.keys()}")
        except Exception as stage2_error:
            print(f"ERROR: Stage 2 FAILED with exception:")
            print(f"  Type: {type(stage2_error).__name__}")
            print(f"  Message: {str(stage2_error)}")
            import traceback
            print(f"  Traceback:\n{traceback.format_exc()}")
            raise
        
        # Filter results to keep only MASTER_FILES tables
        print(f"DEBUG: About to filter results with {len(stage2_full_result.get('tables_processed', []))} tables")
        bootstrap_result = filter_to_master_tables_only(stage2_full_result)
        print(f"DEBUG: Filtering complete - {len(bootstrap_result['master_tables_processed'])} master tables")
        
        stage2_end = datetime.now(timezone.utc)
        stage2_duration = (stage2_end - stage2_start).total_seconds()
        
        print(f" Stage 2 completed in {stage2_duration:.2f} seconds")
        print(f"  Master tables processed: {len(bootstrap_result['master_tables_processed'])}")
        print(f"  Transactional tables skipped: {len(bootstrap_result['transactional_tables_skipped'])}")
        
        if bootstrap_result['transactional_tables_skipped']:
            print(f"  Skipped tables (files left untouched): {bootstrap_result['transactional_tables_skipped']}")
        
        # Log master table details
        if bootstrap_result['master_data_summary']:
            print("\n  Master Files Created/Updated:")
            for table, stats in bootstrap_result['master_data_summary'].items():
                print(
                    f"    {table}: {stats.get('total_rows', 0)} total rows "
                    f"({stats.get('new_rows', 0)} new rows)"
                )
        
        # ========================================================================
        # BOOTSTRAP SUMMARY
        # ========================================================================
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 100)
        print("BOOTSTRAP SUMMARY")
        print("=" * 100)
        print(f"Bootstrap Timestamp: {stage1_result['run_id']}")
        print(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        print(f"Stage 1 Duration: {stage1_duration:.2f}s")
        print(f"Stage 2 Duration: {stage2_duration:.2f}s")
        print(f"Total Duration: {total_duration:.2f}s")
        print(f"Master Tables: {len(bootstrap_result['master_tables_processed'])}")
        print(f"Skipped Tables: {len(bootstrap_result['transactional_tables_skipped'])}")
        print(f"Status: SUCCESS")
        print("=" * 100)
        
        # Build response
        response = {
            "status": "success",
            "bootstrap_timestamp": stage1_result['run_id'],
            "pipeline_mode": "Z-Block" if is_zblock else "AP",
            "message": "One-time SAP master data setup completed successfully",
            "master_tables_processed": bootstrap_result['master_tables_processed'],
            "transactional_tables_skipped": bootstrap_result['transactional_tables_skipped'],
            "master_data_summary": bootstrap_result['master_data_summary'],
            "stage1_result": {
                "sap_files": stage1_result['sap_file_count'],
                "attachments": stage1_result['attachment_count'],
                "total_files": stage1_result['total_files'],
                "duration_seconds": stage1_duration
            },
            "stage2_result": {
                "duration_seconds": stage2_duration
            },
            "total_time_seconds": total_duration
        }
        
        return response
        
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        print(f"\n" + "=" * 100)
        print(f"EXCEPTION IN BOOTSTRAP SAP SETUP")
        print(f"=" * 100)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")
        sys.stdout.flush()
        
        return {
            "status": "failure",
            "error": str(e),
            "error_type": type(e).__name__,
            "total_time_seconds": total_duration
        }


def filter_to_master_tables_only(stage2_result: Dict) -> Dict:
    """
    Filter Stage 2 results to include only MASTER_FILES tables.
    Transactional tables are identified and logged as skipped.
    
    Args:
        stage2_result: Full Stage 2 output containing both master and transactional results
    
    Returns:
        Dictionary with filtered results:
        {
            "master_tables_processed": [...],
            "transactional_tables_skipped": [...],
            "master_data_summary": {...}
        }
    """
    
    master_tables_processed = []
    transactional_tables_skipped = []
    master_data_summary = {}
    
    # Iterate through all processed tables
    all_tables = stage2_result.get('tables_processed', [])
    tables_updated = stage2_result.get('tables_updated', {})
    
    for table_name in all_tables:
        if table_name in MASTER_FILES:
            # This is a MASTER table - include it
            master_tables_processed.append(table_name)
            
            # Extract summary stats for this master table
            if table_name in tables_updated:
                table_stats = tables_updated[table_name]
                master_data_summary[table_name] = {
                    "total_rows": table_stats.get('total_rows', 0),
                    "new_rows": table_stats.get('new_rows_added', 0),
                    "existing_rows": table_stats.get('existing_rows', 0),
                    "mode": table_stats.get('mode', 'unknown')
                }
        else:
            # This is a TRANSACTIONAL table - skip it
            transactional_tables_skipped.append(table_name)
            print(f"  SKIPPED (transactional): {table_name} - files left untouched in source folder")
    
    return {
        "master_tables_processed": master_tables_processed,
        "transactional_tables_skipped": transactional_tables_skipped,
        "master_data_summary": master_data_summary
    }
