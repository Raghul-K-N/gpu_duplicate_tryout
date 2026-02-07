"""
AP One-Time Bootstrap Setup Module
Handles one-time initialization of AP master data before Day 1 Production
Executes Stages 1, 2, 3 and AP preprocessing pipeline
"""

import os
import sys
import requests
import traceback
from datetime import datetime, timezone
from flask import g
from code1.logger import capture_log_message
from .stage1_file_classifier import classify_files
from .stage2_sap_ingestion import ingest_and_update_state
from .stage3_invoice_pipeline import assemble_invoices
from code1.src_load import (
    create_new_batch_entry,
    update_run_timestamp_in_batch_table
)

def bootstrap_ap_setup(incoming_folder, pipeline_mode='AP'):
    """
    Orchestrates one-time AP master data setup (Stages 1, 2, and 3)
    Then calls AP preprocessing via custom_hist_ap API endpoint
    
    Args:
        incoming_folder (str): Path to folder containing SAP raw files and AP documents
        pipeline_mode (str): Pipeline mode identifier (default: 'AP')
    
    Returns:
        dict: Bootstrap result with status, timestamps, and processing summary
    """
    try:
        bootstrap_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        print("Starting AP one-time bootstrap setup")
        
        # Step 1: Create batch entry for AP
        print("Step 1: Creating batch entry for AP module")
        batch_id = create_new_batch_entry(module_nm='AP')
        if batch_id is None:
            return {
                "status": "error",
                "message": "Failed to create batch entry",
                "bootstrap_timestamp": bootstrap_timestamp
            }
        print(f"Batch entry created with batch_id: {batch_id}")
        
        # Step 2: Stage 1 - File Classification
        print("Step 2: Executing Stage 1 - File Classification")
        stage1_result = classify_files(
            incoming_folder=incoming_folder,
            is_zblock=False
        )
        
        # Check if stage1 succeeded (classify_files returns dict with 'run_id', not 'status')
        if stage1_result is None or 'run_id' not in stage1_result:
            print(f"ERROR: Stage 1 failed. Result: {stage1_result}")
            return {
                "status": "error",
                "message": "Stage 1 (File Classification) failed",
                "bootstrap_timestamp": bootstrap_timestamp,
                "batch_id": batch_id,
                "stage1_details": stage1_result
            }
        
        run_timestamp = stage1_result.get('run_id')  # Changed from 'run_timestamp' to 'run_id'
        print(f"Stage 1 completed. Run timestamp: {run_timestamp}")
        
        # Step 3: Update batch with run_timestamp
        print(f"Step 3: Updating batch with run_timestamp: {run_timestamp}")
        ts_update_success = update_run_timestamp_in_batch_table(batch_id, run_timestamp)
        if not ts_update_success:
            return {
                "status": "error",
                "message": "Failed to update run_timestamp in batch table",
                "bootstrap_timestamp": bootstrap_timestamp,
                "batch_id": batch_id
            }
        
        # Step 4: Stage 2 - SAP Data Ingestion
        print("Step 4: Executing Stage 2 - SAP Data Ingestion")
        stage2_result = ingest_and_update_state(
            stage1_result=stage1_result,
            is_zblock=False,
            append_new_rows=True,
            create_synthetic_bseg_data=True
        )
        
        # Check if stage2 succeeded (ingest_and_update_state returns dict with 'run_id' and 'tables_processed')
        if stage2_result is None or 'run_id' not in stage2_result:
            print(f"ERROR: Stage 2 failed. Result: {stage2_result}")
            return {
                "status": "error",
                "message": "Stage 2 (SAP Data Ingestion) failed",
                "bootstrap_timestamp": bootstrap_timestamp,
                "batch_id": batch_id,
                "stage2_details": stage2_result
            }
        
        print("Stage 2 completed. SAP data ingested and master parquets created")
        
        # Step 5: Stage 3 - Merge SAP Data (Call existing function directly)
        print("Step 5: Executing Stage 3 - SAP Data Merge")
        stage3_result = assemble_invoices(stage2_result=stage2_result,is_zblock=False )
        
        # Check if stage3 succeeded
        if stage3_result is None:
            print(f"ERROR: Stage 3 failed. Result: {stage3_result}")
            return {
                "status": "error",
                "message": "Stage 3 (SAP Data Merge) failed",
                "bootstrap_timestamp": bootstrap_timestamp,
                "batch_id": batch_id,
                "stage3_details": stage3_result
            }
        
        print("Stage 3 completed. SAP data merged into consolidated dataset")
        
        # Step 6: Call AP preprocessing via custom_hist_ap API endpoint
        print("Step 6: Calling AP preprocessing (custom_hist_ap API)")
        app_url = os.getenv('APP_URL')
        request_url = f"{app_url}/custom_hist_ap/{batch_id}"
        
        try:
            ap_response = requests.get(request_url, verify=False)
            
            if ap_response is None or ap_response.status_code != 200:
                return {
                    "status": "error",
                    "message": "AP preprocessing (custom_hist_ap API) failed",
                    "bootstrap_timestamp": bootstrap_timestamp,
                    "batch_id": batch_id,
                    "status_code": ap_response.status_code if ap_response else None
                }
            
            ap_result = ap_response.json() if ap_response else {}
        except Exception as e:
            print(f"Error calling AP preprocessing API: {str(e)}")
            return {
                "status": "error",
                "message": f"AP preprocessing API call failed: {str(e)}",
                "bootstrap_timestamp": bootstrap_timestamp,
                "batch_id": batch_id
            }
        
        print("AP preprocessing completed successfully")
        
        # Compile final response
        return {
            "status": "success",
            "message": "AP one-time bootstrap setup completed successfully",
            "bootstrap_timestamp": bootstrap_timestamp,
            "batch_id": batch_id,
            "run_timestamp": run_timestamp,
            "stage1_summary": stage1_result.get('summary', {}),
            "stage2_summary": stage2_result.get('summary', {}),
            "stage3_summary": stage3_result.get('summary', {}),
            "ap_preprocessing_summary": ap_result.get('summary', {})
        }
        
    except Exception as e:
        error_message = f"AP bootstrap setup failed: {str(e)}"
        print(f"\n" + "=" * 100)
        print(f"EXCEPTION IN AP BOOTSTRAP SETUP")
        print(f"=" * 100)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {error_message}")
        print(f"Traceback:\n{traceback.format_exc()}")
        sys.stdout.flush()
        
        return {
            "status": "error",
            "message": error_message,
            "bootstrap_timestamp": bootstrap_timestamp if 'bootstrap_timestamp' in locals() else None,
            "error_type": type(e).__name__
        }
