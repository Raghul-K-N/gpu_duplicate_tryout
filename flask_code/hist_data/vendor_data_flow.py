import pandas as pd
import os
from flask import g
from code1.logger import capture_log_message
from code1 import src_load
from app import get_folder_path_for_client
import hist_data.utilities as utils
from sqlalchemy import create_engine, text


def store_vendor_data():
    """
    Store vendor data with new logic:
    1. Process DB operations first with status management
    2. Query entire table from DB and save as parquet
    """
    input_folder = g.input_folder_path_for_historical_data
    all_files = os.listdir(input_folder)
    vendor_files = [file for file in all_files if str(file).lower().startswith('vendor')]
    
    if len(vendor_files) == 0:
        capture_log_message("No vendor files found in the input folder")
        return False, (0,0)
    
    # Read and combine all vendor files
    vendor_dataframes = []
    for file in vendor_files:
        file_path = os.path.join(input_folder, file)
        vendor_dataframes.append(pd.read_csv(file_path))
    
    vendor_data = pd.concat(vendor_dataframes, ignore_index=True)
    capture_log_message("Shape of the vendor data: " + str(vendor_data.shape))
    
    vendor_data = vendor_data.rename(columns=utils.VENDOR_COLUMN_MAPPING)

    # missing_similarity_cols = [col for col in utils.VENDOR_SIMILARITY_COLUMNS if col not in vendor_data.columns]
    # if missing_similarity_cols:
    #     capture_log_message(current_logger=g.error_logger, log_message=f"Missing similarity columns in vendor data: {missing_similarity_cols}")
    #     return False, vendor_data.shape
    
    # Step 1: Process database operations first
    if not _process_vendor_data_in_database(vendor_data):
        return False, vendor_data.shape
    
    # Step 2: Query entire table and save to parquet
    if not _save_entire_vendor_data_to_parquet():
        return False, vendor_data.shape
    
    return True, vendor_data.shape


def _process_vendor_data_in_database(vendor_data):
    """
    Process vendor data in database with sequential update logic
    """
    try:
        # Get existing data from database
        existing_data = src_load.read_table(utils.TABLE_NAME_FOR_VENDOR_DATA)
        
        # Get column lists from utils
        duplicate_columns = utils.VENDOR_DUPLICATE_COLUMNS
        similarity_columns = utils.VENDOR_SIMILARITY_COLUMNS
        
        if existing_data.empty:
            # No existing data - just insert new data with default STATUS=1
            vendor_data['client_id'] = g.client_id if hasattr(g, 'client_id') else None
            # STATUS will default to 1 as per table definition, no need to set explicitly
            # CREATED_DATE and MODIFIED_DATE will use database defaults
            
            src_load.upload_data_to_database(
                data=vendor_data, 
                tablename=utils.TABLE_NAME_FOR_VENDOR_DATA
            )
            capture_log_message("Initial vendor data stored in database")
            return True
        
        capture_log_message("Processing vendor data with existing records")
        capture_log_message("Shape of existing data: " + str(existing_data.shape))
        capture_log_message("Shape of new data: " + str(vendor_data.shape))
        
        # Ensure data types are consistent
        for col in duplicate_columns:
            if col in vendor_data.columns and col in existing_data.columns:
                vendor_data[col] = vendor_data[col].fillna('').astype(str)
                existing_data[col] = existing_data[col].fillna('').astype(str)
        
        # Step 1: Remove exact duplicates from new data
        vendor_data_filtered = _remove_exact_duplicates(vendor_data, existing_data, duplicate_columns)
        
        if vendor_data_filtered.empty:
            capture_log_message("No new data to process after duplicate removal")
            return True
        
        vendor_data_filtered = vendor_data_filtered.mask(
                                (vendor_data_filtered == '') | vendor_data_filtered.isna(), None)

        # Step 2: Process remaining data with sequential update logic
        return _process_sequential_vendor_updates(vendor_data_filtered, existing_data, similarity_columns)
        
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, 
                            log_message=f"Error processing vendor data in database: {str(e)}")
        return False


def _remove_exact_duplicates(vendor_data, existing_data, duplicate_columns):
    """
    Remove exact duplicates where all 5 columns match
    """
    # Create composite key for exact duplicate detection
    vendor_data['composite_key'] = vendor_data[duplicate_columns].fillna('').apply(lambda x: '|'.join(x), axis=1)
    existing_data['composite_key'] = existing_data[duplicate_columns].fillna('').apply(lambda x: '|'.join(x), axis=1)
    
    # Filter out exact duplicates
    exact_duplicates = vendor_data['composite_key'].isin(existing_data['composite_key'])
    vendor_data_filtered = vendor_data[~exact_duplicates].copy()
    vendor_data_filtered = vendor_data_filtered.drop('composite_key', axis=1)
    
    capture_log_message(f"Found {exact_duplicates.sum()} exact duplicates to skip")
    capture_log_message(f"Processing {len(vendor_data_filtered)} records after duplicate removal")
    
    return vendor_data_filtered


def _process_sequential_vendor_updates(new_data, existing_data, similarity_columns):
    """
    Process vendor updates with sequential change logic:
    - Each row represents one field change
    - Multiple rows for same vendor are processed sequentially
    - Only final state should have STATUS=1
    """
    
    # Group new data by vendor code to handle sequential updates
    new_data_by_vendor = new_data.groupby('VENDORCODE')
    
    for vendor_code, vendor_group in new_data_by_vendor:
        capture_log_message(f"Processing vendor {vendor_code} with {len(vendor_group)} new records")
        
        # Get existing active records for this vendor code
        existing_vendor_records = existing_data[
            (existing_data['VENDORCODE'] == vendor_code) & 
            (existing_data.get('STATUS', 1) == 1)  # Only active records
        ].copy()
        
        if existing_vendor_records.empty:
            # New vendor - add all records with database defaults
            capture_log_message(f"New vendor {vendor_code} - adding {len(vendor_group)} records")
            for idx, row in vendor_group.iterrows():
                row_dict = row.to_dict()
                row_dict['client_id'] = g.client_id if hasattr(g, 'client_id') else None
                # Remove explicit date assignments - let database handle defaults
                
                new_records_df = pd.DataFrame([row_dict])
                src_load.upload_data_to_database(
                    data=new_records_df, 
                    tablename=utils.TABLE_NAME_FOR_VENDOR_DATA
                )
        else:
            # Existing vendor - process sequential updates
            capture_log_message(f"Existing vendor {vendor_code} - processing sequential updates")
            
            # Sort new records to ensure consistent processing order
            vendor_group_sorted = vendor_group.sort_index()
            
            # Keep track of currently active records for this vendor
            current_active_records = existing_vendor_records.copy()
            
            # Process each new record sequentially
            for record_index, new_record in vendor_group_sorted.iterrows():
                capture_log_message(f"Processing record {record_index} for vendor {vendor_code}")
                
                # Find the most similar active record
                matching_record, similarity_count = _find_most_similar_record(
                    new_record, current_active_records, similarity_columns
                )
                
                records_to_deactivate = []
                
                if matching_record is not None:
                    # This is an update to an existing record
                    capture_log_message(f"Found similar record (similarity: {similarity_count}/{len(similarity_columns)})")
                    capture_log_message(f"Will deactivate VENDORID: {matching_record['VENDORID']}")
                    
                    # Mark the matching record for deactivation
                    records_to_deactivate.append(matching_record['VENDORID'])
                    
                    # Remove the matched record from current active records
                    current_active_records = current_active_records[
                        current_active_records['VENDORID'] != matching_record['VENDORID']
                    ]
                    
                else:
                    # No similar record found - this is a completely new record for this vendor
                    capture_log_message(f"No similar record found - treating as new record for vendor {vendor_code}")
                
                # Deactivate the matched record first (if any)
                if records_to_deactivate:
                    _deactivate_records_by_ids(records_to_deactivate)
                
                # Insert the new record
                new_record_dict = new_record.to_dict()
                new_record_dict['client_id'] = g.client_id if hasattr(g, 'client_id') else None
                # Remove explicit date assignments - let database handle defaults
                
                new_records_df = pd.DataFrame([new_record_dict])
                src_load.upload_data_to_database(
                    data=new_records_df, 
                    tablename=utils.TABLE_NAME_FOR_VENDOR_DATA
                )
                
                # Get the newly inserted record with its auto-generated VENDORID
                # Query the last inserted record for this vendor
                newly_inserted_record = _get_latest_record_for_vendor(vendor_code)
                
                if newly_inserted_record is not None:
                    # Add the newly inserted record to current_active_records for next iteration
                    current_active_records = pd.concat([
                        current_active_records, 
                        pd.DataFrame([newly_inserted_record])
                    ], ignore_index=True)
    
    capture_log_message("Sequential vendor data processing completed successfully")
    return True


def _find_most_similar_record(new_record, existing_records, similarity_columns):
    """
    Find the most similar record in existing active records
    Returns the matching record and count of matching fields
    """
    max_similarity = 0
    best_match = None
    threshold = utils.VENDOR_SIMILARITY_THRESHOLD
    
    for idx, existing_record in existing_records.iterrows():
        similarity_count = 0
        
        # Count matching fields
        for col in similarity_columns:
            new_val = '' if pd.isna(new_record[col]) else str(new_record[col])
            existing_val = '' if pd.isna(existing_record[col]) else str(existing_record[col])
            if new_val == existing_val:
                similarity_count += 1
        
        if similarity_count > max_similarity:
            max_similarity = similarity_count
            best_match = existing_record
    
    # Only return a match if threshold is met
    if max_similarity >= threshold:
        return best_match, max_similarity
    else:
        return None, max_similarity


def _get_latest_record_for_vendor(vendor_code):
    """
    Get the most recently inserted record for a vendor code
    """
    try:
        from code1.src_load import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, connect_args
        
        engine = create_engine(f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", 
                              connect_args=connect_args)
        
        with engine.connect() as connection:
            query = f"""
            SELECT * FROM {utils.TABLE_NAME_FOR_VENDOR_DATA} 
            WHERE VENDORCODE = '{vendor_code}' 
            AND STATUS = 1
            """
            
            # Add client_id filter if available
            if hasattr(g, 'client_id') and g.client_id:
                query += f" AND client_id = {g.client_id}"
            
            query += " ORDER BY VENDORID DESC LIMIT 1"
            
            result = connection.execute(text(query))
            row = result.fetchone()
            
            if row:
                # Convert to dictionary
                columns = result.keys()
                return dict(zip(columns, row))
            else:
                return None
                
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, 
                            log_message=f"Error getting latest record for vendor {vendor_code}: {str(e)}")
        return None
    finally:
        engine.dispose()


def _deactivate_records_by_ids(record_ids):
    """
    Set STATUS=0 for records with specific VENDORID values
    Note: MODIFIED_DATE will be updated by database trigger/default
    """
    from code1.src_load import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, connect_args
    
    engine = create_engine(f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", 
                          connect_args=connect_args)
    
    with engine.connect() as connection:
        # Convert list to string for IN clause
        ids_str = ",".join(str(id) for id in record_ids)
        
        # Remove explicit MODIFIED_DATE setting if database handles it automatically
        update_query = f"""
        UPDATE {utils.TABLE_NAME_FOR_VENDOR_DATA} 
        SET STATUS = 0
        WHERE VENDORID IN ({ids_str})
        """
        
        # Add client_id filter if available
        if hasattr(g, 'client_id') and g.client_id:
            update_query += f" AND client_id = {g.client_id}"
        
        connection.execute(text(update_query))
        connection.commit()
    
    engine.dispose()


def _save_entire_vendor_data_to_parquet():
    """
    Query entire vendor table from DB and save to parquet
    """
    try:
        # Read entire vendor table from database
        all_vendor_data = src_load.read_table(utils.TABLE_NAME_FOR_VENDOR_DATA)
        
        if all_vendor_data.empty:
            capture_log_message("No data found in vendor table")
            return False
        
        # Set up parquet file paths
        main_path = os.getenv("UPLOADS")
        client_base_path = os.getenv("CLIENT_BASE_FOLDER_PATH",None)
        if client_base_path is not None:
            client_folder = client_base_path
        else:
            client_folder=client_folder = get_folder_path_for_client(g.client_id)
        client_folder = str(client_folder).strip('/')
        
        if client_folder is None or main_path is None:
            capture_log_message("Client folder or main path is None")
            return False
        
        client_path = os.path.join(main_path, client_folder)
        folder_path_for_vendor_data_parquet = os.path.join(client_path, utils.PARQUET_FOLDER_NAME_FOR_VENDOR_DATA)
        
        # Create folder if it doesn't exist
        if not os.path.exists(folder_path_for_vendor_data_parquet):
            os.makedirs(folder_path_for_vendor_data_parquet)
        
        # Remove existing parquet files (delete all together approach)
        if os.path.exists(folder_path_for_vendor_data_parquet):
            for file in os.listdir(folder_path_for_vendor_data_parquet):
                if file.endswith('.parquet'):
                    os.remove(os.path.join(folder_path_for_vendor_data_parquet, file))
        
        # Save entire vendor data as parquet
        dest_file_path = os.path.join(folder_path_for_vendor_data_parquet, utils.VENDOR_DATA_PARQUET_FILE_NAME)
        all_vendor_data.to_parquet(dest_file_path, engine='pyarrow')
        
        capture_log_message(f"Entire vendor data saved to parquet. Shape: {all_vendor_data.shape}")
        return True
        
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, 
                            log_message=f"Error saving vendor data to parquet: {str(e)}")
        return False


# Example usage:
# store_vendor_data()