"""
Stage 1: File Classification
Classifies incoming files into SAP data files and attachments, organizing them per folder structure.
"""
import os
import shutil
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
from .logger_config import get_logger
from .utils import EXPECTED_TABLES, Z_BLOCK_EXPECTED_TABLES

load_dotenv()


def classify_files(
    incoming_folder: str, 
    is_zblock: bool = False, 
    skip_bkpf_files: bool = False,
    run_timestamp: Optional[str] = None
) -> Dict:
    """
    Stage 1: File Classification - Organize incoming files into appropriate folders.
    
    Folder Structure:
    - UPLOADS/MASTER_FOLDER/ATTACHMENTS_FOLDER/ - All attachments (not run-scoped)
    - UPLOADS/MASTER_FOLDER/SAP_RUNS_FOLDER/<timestamp>/ - SAP files (run-scoped)
    
    Classifies files based on table name matching:
    - SAP files: Filename contains table name from expected list
    - Special: When is_zblock=True and table='BKPF', filename must start with 'z'
    - For other cases: filename must NOT start with 'z'
    - Attachments: Everything else (PDFs, images, unmatched files)
    
    Parameters:
        incoming_folder: Path to folder containing mixed incoming files
        is_zblock: If True, use Z_BLOCK_EXPECTED_TABLES, else EXPECTED_TABLES
        run_timestamp: Optional timestamp string (YYYYMMDDHHMM00), auto-generated if None
    
    Returns:
        Dictionary containing:
        - run_id: Timestamp identifier
        - sap_run_folder: Path to SAP run folder
        - attachments_folder: Path to attachments folder
        - sap_files_by_table: Dict mapping table names to list of files
        - attachment_files: List of attachment filenames
        - sap_file_count: Total SAP files
        - attachment_count: Total attachments
        - total_files: Total files processed
    """
    try:

        logger = get_logger()
        logger.info("=" * 80)
        logger.info("STAGE 1: FILE CLASSIFICATION STARTED")
        logger.info("=" * 80)
        
        # Generate timestamp if not provided (UTC time, rounded to hour start)
        if run_timestamp is None:
            utc_now = datetime.now(timezone.utc)
            utc_hour_start = utc_now.replace(minute=0, second=0, microsecond=0)
            run_timestamp = utc_hour_start.strftime("%Y%m%d%H%M%S")
            # Example: 2025-05-05 12:05:10 UTC → "20250505120000"

        
        logger.info(f"Run Timestamp: {run_timestamp}")
        logger.info(f"Incoming Folder: {incoming_folder}")
        logger.info(f"Pipeline Mode: {'Z-Block' if is_zblock else 'AP'}")
        
        # Select table list based on pipeline mode
        # table_list = Z_BLOCK_EXPECTED_TABLES if is_zblock else EXPECTED_TABLES
        table_list = Z_BLOCK_EXPECTED_TABLES # check for all z block tables irespective of pipeline mode
        logger.info(f"Expected Tables Count: {len(table_list)}")
        logger.debug(f"Expected Tables: {table_list}")
        
        # Get base paths from environment
        base_path = os.getenv('UPLOADS', None)
        master_folder_name = os.getenv('MASTER_FOLDER', 'dow_transformation')
        attachments_folder_name = os.getenv('ATTACHMENTS_FOLDER', 'attachments')
        sap_runs_folder_name = os.getenv('SAP_RUNS_FOLDER', 'sap_runs')
        print(base_path,master_folder_name,attachments_folder_name,sap_runs_folder_name)
        if base_path is None:
            error_msg = "UPLOADS environment variable is not set."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Base Path (UPLOADS): {base_path}")
        print(base_path)
        
        # Create folder structure
        master_folder = os.path.join(base_path, master_folder_name)
        attachments_folder = os.path.join(master_folder, attachments_folder_name)
        sap_runs_base = os.path.join(master_folder, sap_runs_folder_name)
        sap_run_folder = os.path.join(sap_runs_base, run_timestamp)
        
        os.makedirs(attachments_folder, exist_ok=True)
        os.makedirs(sap_run_folder, exist_ok=True)

        print(attachments_folder,sap_run_folder)
        
        logger.info(f"Created/Verified folder structure:")
        logger.info(f"  - Master Folder: {master_folder}")
        logger.info(f"  - Attachments: {attachments_folder}")
        logger.info(f"  - SAP Runs Base: {sap_runs_base}")
        logger.info(f"  - SAP Run Folder: {sap_run_folder}")
        
        # Check if incoming folder exists and has files
        if not os.path.exists(incoming_folder):
            print(f"Incoming folder does not exist: {incoming_folder}")
            error_msg = f"Incoming folder does not exist: {incoming_folder}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        all_files = [f for f in os.listdir(incoming_folder) if os.path.isfile(os.path.join(incoming_folder, f))]
        
        if len(all_files) == 0:
            print(f"No files found in incoming folder: {incoming_folder}")
            logger.warning(f"No files found in incoming folder: {incoming_folder}")
            return {
                'run_id': run_timestamp,
                'sap_run_folder': sap_run_folder,
                'attachments_folder': attachments_folder,
                'sap_files_by_table': {},
                'attachment_files': [],
                'sap_file_count': 0,
                'attachment_count': 0,
                'total_files': 0
            }
        
        logger.info(f"Total files found in incoming folder: {len(all_files)}")
        
        # Initialize tracking structures
        sap_files_by_table = {}
        attachment_files = []
        sap_file_count = 0
        attachment_count = 0
        
        # Classify and copy each file
        for filename in all_files:
            source_path = os.path.join(incoming_folder, filename)
            filename_lower = filename.lower()
            matched_table = None
            
            # Try to match filename with table names
            for table_name in table_list:
                if table_name.lower() in filename_lower:
                    # Special handling for BKPF variants
                    if table_name == 'BKPF':
                        # Check if this is a Z_BKPF variant
                        # Matches: Z_<SYS><ClientID>_BKPF_<YYYYMMDDHHMMSS> pattern
                        if (filename_lower.startswith('z_') and '_bkpf_' in filename_lower) or \
                        'z_bkpf' in filename_lower or 'z-bkpf' in filename_lower:
                            # Z-variant BKPF files match Z_BKPF table (separate from normal BKPF)
                            matched_table = 'Z_BKPF'
                            logger.debug(f"File '{filename}' matched table 'Z_BKPF' (z-variant BKPF)")
                            break
                        else:
                            # Normal BKPF (no z-variant pattern)
                            matched_table = 'BKPF'
                            logger.debug(f"File '{filename}' matched table 'BKPF'")
                            break
                    elif table_name == 'Z_BKPF':
                        # Z_BKPF table entry should be skipped - we handle it in BKPF logic above
                        continue
                    else:
                        # All other tables: standard matching (must NOT contain z_/z- prefix)
                        if 'z_' not in filename_lower and 'z-' not in filename_lower:
                            matched_table = table_name
                            logger.debug(f"File '{filename}' matched table '{table_name}'")
                            break
            if skip_bkpf_files and matched_table == 'BKPF':
                logger.info(f"Skipping BKPF file as per configuration: '{filename}'")
                continue
            # Copy file to appropriate folder
            if matched_table:
                # SAP file - copy to run-scoped sap_run_folder
                dest_path = os.path.join(sap_run_folder, filename)
                
                try:
                    shutil.copy2(source_path, dest_path)
                    
                    # Track by table
                    if matched_table not in sap_files_by_table:
                        sap_files_by_table[matched_table] = []
                    sap_files_by_table[matched_table].append(filename)
                    
                    sap_file_count += 1
                    logger.info(f" SAP File: '{filename}' → Table: {matched_table}")
                except Exception as e:
                    logger.error(f"Failed to copy SAP file '{filename}': {e}")
            else:
                # Attachment - copy to central attachments folder (not run-scoped)
                dest_path = os.path.join(attachments_folder, filename)
                
                try:
                    shutil.copy2(source_path, dest_path)
                    attachment_files.append(filename)
                    attachment_count += 1
                    logger.info(f" Attachment: '{filename}'")
                except Exception as e:
                    logger.error(f"Failed to copy attachment '{filename}': {e}")
        
        # Log summary
        print("File classification summary:")
        logger.info("-" * 80)
        logger.info("CLASSIFICATION SUMMARY")
        logger.info("-" * 80)
        logger.info(f"Total Files Processed: {len(all_files)}")
        logger.info(f"SAP Files: {sap_file_count}")
        logger.info(f"Attachments: {attachment_count}")
        
        if sap_file_count > 0:
            logger.info(f"SAP Files by Table ({len(sap_files_by_table)} tables):")
            for table, files in sorted(sap_files_by_table.items()):
                logger.info(f"  - {table}: {len(files)} file(s)")
                for f in files:
                    logger.debug(f"      • {f}")
        else:
            logger.warning("⚠ No SAP files found in incoming folder!")
        
        if attachment_count > 0:
            logger.info(f"Attachments: {attachment_count} file(s) copied to central attachments folder")
            for f in attachment_files[:5]:  # Log first 5
                logger.debug(f"  • {f}")
            if attachment_count > 5:
                logger.debug(f"  ... and {attachment_count - 5} more")
        
        logger.info("=" * 80)
        logger.info("STAGE 1: FILE CLASSIFICATION COMPLETED")
        logger.info("=" * 80)
        print("File classification completed.")
        
        return {
            'run_id': run_timestamp,
            'sap_run_folder': sap_run_folder,
            'attachments_folder': attachments_folder,
            'sap_files_by_table': sap_files_by_table,
            'attachment_files': attachment_files,
            'sap_file_count': sap_file_count,
            'attachment_count': attachment_count,
            'total_files': len(all_files),
            'is_zblock': is_zblock,
            'status': 'success'
        }
    except Exception as e:
        logger = get_logger()
        logger.error(f"Error in classify_files: {str(e)}", exc_info=True)
        raise e
