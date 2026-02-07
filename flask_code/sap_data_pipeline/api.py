"""
Flask endpoint for SAP 3-Stage Pipeline
Orchestrates Stage 1 (File Classification), Stage 2 (SAP Ingestion), and Stage 3 (Invoice Pipeline)
"""
from flask import Blueprint, request, jsonify, g
from datetime import datetime, timezone
import os
import requests
from dotenv import load_dotenv
from multiprocessing import Process

# Import the three stages
from sap_data_pipeline.stage1_file_classifier import classify_files
from sap_data_pipeline.stage2_sap_ingestion import ingest_and_update_state
from sap_data_pipeline.stage3_invoice_pipeline import assemble_invoices
from sap_data_pipeline.logger_config import get_logger, setup_logger
from sap_data_pipeline.bootstrap_sap_setup import bootstrap_sap_setup
from sap_data_pipeline.bootstrap_ap_setup import bootstrap_ap_setup
from code1.src_load import update_run_timestamp_in_batch_table
load_dotenv()

# Create blueprint
sap_pipeline_bp = Blueprint('sap_pipeline', __name__)

# Read all raw data from incoming folder and save it in parquet format
@sap_pipeline_bp.route("/sap-raw-data-pipeline", methods=["POST"])
def run_sap_raw_data_pipeline():
    """
    Run the SAP raw data pipeline.
    
    Request Body (JSON):
    {
        "incoming_folder": "/path/to/incoming/files",
        "pipeline_mode": "AP" | "ZBLOCK",  # no other values
        "append_new_rows": true  // optional, default: true
    }
    
    Returns:
    {
        "status": "success" | "failure",
        "run_id": "20250505120000",
        "stage1_result": {...},
        "stage2_result": {...},
        "stage3_result": {...},
        "message": "Pipeline completed successfully",
        "total_time_seconds": 123.45
    }
    """
    start_time = datetime.now(timezone.utc)
    
    try:
        # Parse request body
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "failure",
                "error": "Request body is required"
            }), 400
        
        incoming_folder = data.get('incoming_folder')
        pipeline_mode = data.get('pipeline_mode')
        batch_id = data.get('batch_id')

        logger = setup_logger(batch_id=batch_id)

        logger.info(f"Received SAP raw data pipeline request with batch_id: {batch_id}")
        if pipeline_mode not in ['AP', 'ZBLOCK']:
            logger.error(f"Invalid pipeline_mode: {pipeline_mode}")
            return jsonify({
                "status": "failure",
                "error": "pipeline_mode must be 'AP' or 'ZBLOCK'"
            }), 400
        
        logger.info(f"Pipeline mode: {pipeline_mode}")
        if pipeline_mode == 'ZBLOCK':
            is_zblock = True
        else:
            is_zblock = False

        append_new_rows = data.get('append_new_rows', True) # default to True always
        logger.info(f"Append new rows: {append_new_rows}")

        if not incoming_folder:
            logger.error("incoming_folder is required")
            return jsonify({
                "status": "failure",
                "error": "incoming_folder is required"
            }), 400
        
        # Validate incoming folder exists
        if not os.path.exists(incoming_folder):
            logger.error(f"Incoming folder does not exist: {incoming_folder}")
            return jsonify({
                "status": "failure",
                "error": f"Incoming folder does not exist: {incoming_folder}"
            }), 400
        
        # Initialize logger
        logger.info("=" * 100)
        logger.info("SAP 3-STAGE PIPELINE STARTED")
        logger.info("=" * 100)
        logger.info(f"Incoming Folder: {incoming_folder}")
        logger.info(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        logger.info(f"Append New Rows: {append_new_rows}")
        
        # ========================================================================
        # STAGE 1: FILE CLASSIFICATION
        # ========================================================================
        logger.info("\n" + "=" * 100)
        logger.info("EXECUTING STAGE 1: FILE CLASSIFICATION")
        logger.info("=" * 100)
        stage1_start = datetime.now(timezone.utc)
        
        stage1_result = classify_files(
            incoming_folder=incoming_folder,
            is_zblock=is_zblock
        )
        
        stage1_end = datetime.now(timezone.utc)
        stage1_duration = (stage1_end - stage1_start).total_seconds()
        
        logger.info(f" Stage 1 completed in {stage1_duration:.2f} seconds")
        logger.info(f"  Run ID: {stage1_result['run_id']}")
        logger.info(f"  SAP files: {stage1_result['sap_file_count']}")
        logger.info(f"  Attachments: {stage1_result['attachment_count']}")


        run_id = stage1_result['run_id']  # timestamp
        # update this run_timestamp in batch table
        res = update_run_timestamp_in_batch_table(batch_id=batch_id,run_timestamp=run_id)
        if res:
            logger.info(f" Run timestamp updated in batch table for batch_id {batch_id}")
        else:
            logger.error(f"Failed to update run timestamp in batch table for batch_id {batch_id}")
        
        # ========================================================================
        # STAGE 2: SAP INGESTION & STATE UPDATE
        # ========================================================================
        logger.info("\n" + "=" * 100)
        logger.info("EXECUTING STAGE 2: SAP INGESTION & STATE UPDATE")
        logger.info("=" * 100)
        stage2_start = datetime.now(timezone.utc)
        
        stage2_result = ingest_and_update_state(
            stage1_result=stage1_result,
            is_zblock=is_zblock,
            append_new_rows=append_new_rows
        )
        
        stage2_end = datetime.now(timezone.utc)
        stage2_duration = (stage2_end - stage2_start).total_seconds()
        
        logger.info(f" Stage 2 completed in {stage2_duration:.2f} seconds")
        logger.info(f"  Tables processed: {stage2_result['total_tables']}")
        logger.info(f"  Tables updated: {len(stage2_result['tables_updated'])}")
        logger.info(f"  Tables failed: {len(stage2_result['tables_failed'])}")
        
        # ========================================================================
        # STAGE 3: INVOICE PIPELINE
        # ========================================================================
        logger.info("\n" + "=" * 100)
        logger.info("EXECUTING STAGE 3: INVOICE PIPELINE")
        logger.info("=" * 100)
        stage3_start = datetime.now(timezone.utc)
        
        stage3_result = assemble_invoices(
            stage2_result=stage2_result,
            is_zblock=is_zblock
        )
        
        stage3_end = datetime.now(timezone.utc)
        stage3_duration = (stage3_end - stage3_start).total_seconds()
        
        if stage3_result['status'] == 'success':
            logger.info(f" Stage 3 completed in {stage3_duration:.2f} seconds")
            logger.info(f"  Invoice records: {stage3_result['invoice_row_count']}")
            logger.info(f"  Invoice columns: {stage3_result['invoice_column_count']}")
            logger.info(f"  Output: {stage3_result['invoice_output_path']}")
            logger.info(f"  DOA processed: {stage3_result['doa_processed']}")
        else:
            logger.error(f"✗ Stage 3 failed: {stage3_result.get('error', 'Unknown error')}")
        
        # ========================================================================
        # PIPELINE SUMMARY
        # ========================================================================
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 100)
        logger.info("SAP 3-STAGE PIPELINE SUMMARY")
        logger.info("=" * 100)
        logger.info(f"Run ID: {stage1_result['run_id']}")
        logger.info(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        logger.info(f"Stage 1 Duration: {stage1_duration:.2f}s")
        logger.info(f"Stage 2 Duration: {stage2_duration:.2f}s")
        logger.info(f"Stage 3 Duration: {stage3_duration:.2f}s")
        logger.info(f"Total Duration: {total_duration:.2f}s")
        logger.info(f"Status: {'SUCCESS' if stage3_result['status'] == 'success' else 'FAILURE'}")
        logger.info("=" * 100)
        
        stage1_status = stage1_result['status']
        stage2_status = stage2_result['status']
        stage3_status = stage3_result['status']
        # Determine overall final status
        if (stage1_status == 'success') and (stage2_status == 'success') and (stage3_status == 'success'):
            final_status = 'success'
        else:
            final_status = 'failure'
        # Build response
        response = {
            "status": stage3_result['status'],
            "run_id": stage1_result['run_id'],
            "pipeline_mode": "Z-Block" if is_zblock else "AP",
            "stage1_result": {
                "sap_files": stage1_result['sap_file_count'],
                "attachments": stage1_result['attachment_count'],
                "total_files": stage1_result['total_files'],
                "duration_seconds": stage1_duration
            },
            "stage2_result": {
                "tables_processed": stage2_result['total_tables'],
                "tables_updated": len(stage2_result['tables_updated']),
                "tables_failed": len(stage2_result['tables_failed']),
                "duration_seconds": stage2_duration
            },
            "stage3_result": {
                "invoice_records": stage3_result.get('invoice_row_count', 0),
                "invoice_columns": stage3_result.get('invoice_column_count', 0),
                "output_path": stage3_result.get('invoice_output_path', ''),
                "doa_processed": stage3_result.get('doa_processed', False),
                "duration_seconds": stage3_duration
            },
            "total_time_seconds": total_duration,
            "final_status": final_status,
            "message": "Pipeline completed successfully" if stage3_result['status'] == 'success' else f"Pipeline failed: {stage3_result.get('error', 'Unknown error')}"
        }
        
        status_code = 200 if stage3_result['status'] == 'success' else 500
        return jsonify(response), status_code
        
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        logger = get_logger()
        logger.exception(f"Error in SAP Pipeline: {e}")
        
        return jsonify({
            "status": "failure",
            "error": str(e),
            "total_time_seconds": total_duration
        }), 500


@sap_pipeline_bp.route("/sap-pipeline/status", methods=["GET"])
def pipeline_status():
    """
    Get the status of the SAP pipeline (health check).
    
    Returns:
    {
        "status": "healthy",
        "message": "SAP Pipeline is ready"
    }
    """
    return jsonify({
        "status": "healthy",
        "message": "SAP Pipeline is ready",
        "stages": ["File Classification", "SAP Ingestion", "Invoice Pipeline"]
    }), 200


@sap_pipeline_bp.route("/zblock-onetime-sap-setup", methods=["POST"])
def zblock_onetime_sap_setup():
    """
    One-time bootstrap endpoint to initialize master SAP data.
    
    Processes ONLY master tables (MARA, LFA1, LFBK, EKKO, EKPO, etc).
    Skips transactional tables (BKPF, VIM_, etc) and leaves their files untouched.
    
    NO database interactions. NO file deletions.
    
    Request Body (JSON):
    {
        "incoming_folder": "/path/to/bootstrap/files",
        "pipeline_mode": "AP" | "ZBLOCK"
    }
    
    Returns:
    {
        "status": "success" | "failure",
        "bootstrap_timestamp": "20250123120000",
        "pipeline_mode": "AP" | "Z-Block",
        "message": "One-time SAP master data setup completed successfully",
        "master_tables_processed": [...],
        "transactional_tables_skipped": [...],
        "master_data_summary": {
            "MARA": {"total_rows": 500, "new_rows": 500, "mode": "new_file"},
            ...
        },
        "total_time_seconds": 45.67
    }
    """
    start_time = datetime.now(timezone.utc)
    
    try:
        # Parse request body
        data = request.get_json()
        print(data)
        if not data:
            return jsonify({
                "status": "failure",
                "error": "Request body is required"
            }), 400
        
        incoming_folder = data.get('incoming_folder')
        pipeline_mode = data.get('pipeline_mode')
        
        # Validate required fields
        if not incoming_folder:
            return jsonify({
                "status": "failure",
                "error": "incoming_folder is required"
            }), 400
        
        if not pipeline_mode:
            return jsonify({
                "status": "failure",
                "error": "pipeline_mode is required"
            }), 400
        
        if pipeline_mode not in ['AP', 'ZBLOCK']:
            return jsonify({
                "status": "failure",
                "error": "pipeline_mode must be 'AP' or 'ZBLOCK'"
            }), 400
        
        # Validate folder exists
        if not os.path.exists(incoming_folder):
            return jsonify({
                "status": "failure",
                "error": f"Incoming folder does not exist: {incoming_folder}"
            }), 400
        
        # Execute bootstrap
        bootstrap_result = bootstrap_sap_setup(
            incoming_folder=incoming_folder,
            pipeline_mode=pipeline_mode
        )
        
        end_time = datetime.now(timezone.utc)
        
        if bootstrap_result['status'] == 'success':
            return jsonify(bootstrap_result), 200
        else:
            return jsonify(bootstrap_result), 500
        
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        print(f"Error in bootstrap endpoint: {e}")
        import traceback
        print(traceback.format_exc())
        return jsonify({
            "status": "failure",
            "error": str(e),
            "total_time_seconds": total_duration
        }), 500

@sap_pipeline_bp.route("/ap-onetime-sap-setup", methods=["POST"])
def ap_onetime_sap_setup():
    """
    One-time bootstrap endpoint to initialize AP master data and preprocessing.
    
    Processes SAP master tables (MARA, LFA1, LFBK, EKKO, EKPO, etc).
    Includes transactional tables (BKPF, VIM_, etc) via Stage 3 merge.
    Executes all 3 stages of SAP pipeline + AP preprocessing.
    
    Request Body (JSON):
    {
        "incoming_folder": "/path/to/bootstrap/files",
        "pipeline_mode": "AP"
    }
    
    Returns:
    {
        "status": "success" | "error",
        "message": "AP one-time bootstrap setup completed successfully",
        "bootstrap_timestamp": "20250123120000",
        "batch_id": 12345,
        "run_timestamp": "20250123120000",
        "stage1_summary": {...},
        "stage2_summary": {...},
        "stage3_summary": {...},
        "ap_preprocessing_summary": {...}
    }
    """
    start_time = datetime.now(timezone.utc)
    
    try:
        # Parse request body
        data = request.get_json()
        print('Request data',data)
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        incoming_folder = data.get('incoming_folder')
        pipeline_mode = data.get('pipeline_mode', 'AP')
        print(pipeline_mode)
        print(incoming_folder)
        # Validate required fields
        if not incoming_folder:
            return jsonify({
                "status": "error",
                "message": "incoming_folder is required"
            }), 400
        
        # Validate folder exists
        if not os.path.exists(incoming_folder):
            return jsonify({
                "status": "error",
                "message": f"Incoming folder does not exist: {incoming_folder}"
            }), 400
        
        # Execute AP bootstrap
        bootstrap_result = bootstrap_ap_setup(
            incoming_folder=incoming_folder,
            pipeline_mode=pipeline_mode
        )
        
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        bootstrap_result['total_time_seconds'] = total_duration
        
        if bootstrap_result.get('status') == 'success':
            return jsonify(bootstrap_result), 200
        else:
            return jsonify(bootstrap_result), 500
        
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        print(f"Error in AP bootstrap endpoint: {e}")
        
        return jsonify({
            "status": "error",
            "message": str(e),
            "total_time_seconds": total_duration
        }), 500


def execute_ap_bootstrap_background(incoming_folder, pipeline_mode):
    """Call existing AP bootstrap endpoint in background"""
    try:
        requests.post(
            f"{os.getenv('APP_URL')}/ap-onetime-sap-setup",
            json={'incoming_folder': incoming_folder, 'pipeline_mode': pipeline_mode},
            timeout=3600
        )
    except Exception as e:
        print(f"Background process error: {e}")
        import traceback
        print(traceback.format_exc())


@sap_pipeline_bp.route("/ap-onetime-sap-setup-background", methods=["POST"])
def ap_onetime_sap_setup_background():
    """Background version - returns immediately with 202"""
    try:
        data = request.get_json()
        incoming_folder = data.get('incoming_folder')
        pipeline_mode = data.get('pipeline_mode', 'AP')
        
        # Spawn background process
        process = Process(target=execute_ap_bootstrap_background, args=(incoming_folder, pipeline_mode))
        process.start()
        
        # Return immediately
        return jsonify({
            "status": "accepted",
            "message": "AP bootstrap started in background"
        }), 202
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500





# curl -X POST "http://localhost:5005/zblock-onetime-sap-setup" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "incoming_folder": "/app/uploads/dow_transformation/bootstrap/zblock",
#     "pipeline_mode": "ZBLOCK"
#   }'


# curl -X POST "http://localhost:5005/ap-onetime-sap-setup" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "incoming_folder": "/app/uploads/dow_transformation/bootstrap/ap",
#     "pipeline_mode": "AP"
#   }'


# curl -X POST "http://localhost:5005/ap-onetime-sap-setup-background" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "incoming_folder": "/app/uploads/dow_transformation/bootstrap/ap",
#     "pipeline_mode": "AP"
#   }'