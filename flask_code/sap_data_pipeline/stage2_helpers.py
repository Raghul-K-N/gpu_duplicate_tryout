"""
Stage 2 Helper Functions
Helper utilities for SAP data ingestion and state management.
"""
import os
import pandas as pd
from typing import Dict, Optional, List, Tuple
from pandas.api.types import (
    is_integer_dtype, is_float_dtype, is_datetime64_any_dtype, 
    is_object_dtype, is_string_dtype, is_numeric_dtype
)

from sap_data_pipeline.data_cleaning import clean_amount_column, clean_date_column


# ============================================================================
# BUSINESS KEY MAPPINGS
# ============================================================================
# Mapping: Table name → list of unique columns for deduplication
# These are the actual column names as they appear in the SAP data files
UNIQUE_COLUMNS_MAP = {
    'BSEG': ['Client', 'Company Code', 'Document Number', 'Pstg per.var.', 'Fiscal Year'],
    'UDC': ['Client', 'Inv. Doc. No.', 'Fiscal Year'],
    'WTH': ['Client', 'Company Code', 'Document Number', 'Fiscal Year'],
    'EKKO': ['Client', 'Purchasing Doc.', 'Company Code'],
    'EKPO': ['Client', 'Purchasing Doc.', 'Item'],
    'LFA1': ['Client', 'Supplier'],
    'LFB1': ['Client', 'Supplier', 'Company Code'],
    'LFM1': ['Client', 'Supplier', 'Purch. Org.'],
    'LFBK': ['Client', 'Supplier'],
    'VRDOA': ['Client', 'Company Code'],
    'DOAREDEL': ['Client', 'User Name', 'DOA Type'],
    'T001': ['Client', 'Company Code', 'Address', 'Name'],
    'T003': ['Client', 'Document Type'],
    'T042Z': ['Client', 'Country', 'Pymt Method'],
    'T052U': ['Client', 'Pyt Terms'],
    'T053S': ['Client', 'Reason code'],
    'VIMT100': ['Client', 'DP Document Type'],
    'VIMT101': ['Client', 'Document Status'],
    
    # Additional tables (using common patterns)
    'BKPF': ['Client', 'Company Code', 'Document Number', 'Pstg per.var.', 'Fiscal Year'],
    'VIM_': ['Client', 'Document Id'],
    '1LOG_': ['Client', 'Document Id'],
    '8LOG_': ['Client','Object Type','Object Key','Document Log Id'],
    'APRLOG': ['Client', 'ID'],
    '1LOGCOMM': ['Client','Document Id', 'Document Log Id'],
    '8LOGCOMM': ['Client', 'Object Type','Object Key'],
    'RETINV': ['Client', 'Company Code', 'Document Number', 'Fiscal Year'],
}


# ============================================================================
# PATH UTILITIES
# ============================================================================

def get_master_parquet_path(base_path: str, master_folder_name: str, master_parquet_folder_name: str) -> str:
    """
    Get the path to the master_parquet directory where persistent parquet files are stored.
    
    Path structure: UPLOADS/dow_transformation/master_parquet/
    
    Args:
        base_path: Base uploads directory
        master_folder_name: Master folder name (e.g., 'dow_transformation')
        master_parquet_folder_name: Master parquet folder name (e.g., 'master_parquet')
    
    Returns:
        Absolute path to master_parquet directory
    """
    return os.path.join(base_path, master_folder_name, master_parquet_folder_name)


def perform_data_cleaning_on_dataframe(df: pd.DataFrame, logger, table_name: str, filename: str) -> pd.DataFrame:
    """
    Perform any table-specific data cleaning on the DataFrame after reading.
    
    Args:
        df: DataFrame to clean
        logger: Logger instance
        table_name: SAP table name
        filename: Source filename
    Returns:
        Cleaned DataFrame
    """

    amount_cleanup_tables = {'BSEG':['Amount in LC','Amount'],
                             'BKPF':['Amount in LC','Amount','Exchange rate','Exchange rate 2'],
                             'EKKO':['Exchange Rate'],
                             'EKPO':['Net Price','Gross value','PO Quantity'],
                             'WTH':['W/tax base LC','W/tax base FC'],
                             'UDC':['Amount']}
    
    # date_cleanup_tables = {
    #     'BKPF':['Document Date','Posting Date','Entered on','Inv. recpt date','Baseline Date','Planning date',"Clearing"],
    #     'EKKO':['Document Date','Created On'],

    # }
    if table_name in amount_cleanup_tables.keys():
        col_to_clean = amount_cleanup_tables[table_name]
        for col in col_to_clean:
            if col in df.columns:
                df[col] = clean_amount_column(df[col])
            else:
                logger.debug(f"Column {col} not found in {filename} for table {table_name}, skipping amount cleaning.")

    # if table_name in date_cleanup_tables.keys():
    #     col_to_clean = date_cleanup_tables[table_name]
    #     for col in col_to_clean:
    #         if col in df.columns:
    #             df[col] = clean_date_column(df[col], can_be_null=True)
    #         else:
    #             logger.debug(f"Column {col} not found in {filename} for table {table_name}, skipping date cleaning.")

    return df
def get_table_parquet_path(master_parquet_path: str, table_name: str) -> str:
    """
    Get the full path to a table's persistent parquet file.
    
    Handles table names with trailing underscores (e.g., VIM_, 1LOG_, 8LOG_)
    by stripping them for folder/file naming.
    
    Args:
        master_parquet_path: Path to master_parquet directory
        table_name: SAP table name (e.g., 'BSEG', 'VIM_', '1LOG_')
    
    Returns:
        Full path to the parquet file: master_parquet/<TABLE_NAME>/SAP_<TABLE_NAME>_data.parquet
        Example: master_parquet/VIM/SAP_VIM_data.parquet (for table 'VIM_')
    """
    # Strip trailing underscores for folder/file naming
    clean_table_name = table_name.rstrip('_')
    
    table_dir = os.path.join(master_parquet_path, clean_table_name)
    os.makedirs(table_dir, exist_ok=True)
    return os.path.join(table_dir, f'SAP_{clean_table_name}_data.parquet')


# ============================================================================
# FILE READING
# ============================================================================

def read_files_for_table(
    sap_run_folder: str,
    table_name: str,
    filenames: List[str],
    logger
) -> Optional[pd.DataFrame]:
    """
    Read and concatenate all files for a specific table from the run folder.
    
    Supports: Excel (.xlsx, .xls), CSV (.csv), Parquet (.parquet)
    
    Args:
        sap_run_folder: Path to the run-scoped SAP folder
        table_name: SAP table name
        filenames: List of filenames belonging to this table
        logger: Logger instance
    
    Returns:
        Combined DataFrame or None if no valid data
    """
    if not filenames:
        logger.warning(f"No files provided for table {table_name}")
        return None
    
    dfs = []
    expected_cols = None
    
    for filename in filenames:
        file_path = os.path.join(sap_run_folder, filename)

        try:
            # Read based on file extension
            if filename.lower().endswith('.parquet'):
                df = pd.read_parquet(file_path)
            elif filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            elif filename.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, engine='openpyxl')
            else:
                logger.warning(f"Unsupported file format for {filename}, skipping")
                continue
            
            logger.info(f"Successfully read {filename}, shape={df.shape}") 
            # Normalize column names (strip whitespace)
            df.columns = [str(c).strip() for c in df.columns]
            
            # Check column consistency
            if expected_cols is None:
                expected_cols = len(df.columns)
            elif len(df.columns) != expected_cols:
                logger.error(
                    f"Column count mismatch in {filename} for table {table_name}. "
                    f"Expected {expected_cols}, got {len(df.columns)}"
                )
                
            
            # Log basic info
            logger.info(f"Read {filename} for {table_name}: shape {df.shape}")
            
            # Drop exact duplicates within the file
            initial_rows = len(df)
            df.drop_duplicates(inplace=True, keep='first')
            if len(df) < initial_rows:
                logger.info(f"Dropped {initial_rows - len(df)} duplicate rows from {filename}")
            
            df = perform_data_cleaning_on_dataframe(df, logger, table_name, filename)

            dfs.append(df)
            
        except Exception as e:
            print(f"DEBUG read_files: ERROR reading {filename}: {type(e).__name__}: {str(e)}")
            logger.error(f"Failed to read {filename} for table {table_name}: {e}")
            # Continue processing other files
            continue
    
    if not dfs:
        print(f"DEBUG read_files: No valid data loaded for table {table_name}")
        logger.warning(f"No valid data loaded for table {table_name}")
        return None
    
    print(f"DEBUG read_files: Harmonizing {len(dfs)} dataframes")
    # Harmonize dtypes across all DataFrames before concatenation
    # This prevents 'object' columns with mixed types (int and str) that crash Parquet
    # dfs = harmonize_dataframe_list(dfs, logger)
    
    print(f"DEBUG read_files: Concatenating {len(dfs)} dataframes")
    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    try:

        combined_df = harmonize_single_dataframe_vectorized(df=combined_df, logger=logger)
    except Exception as e:
        logger.error(f"Error during harmonization for {table_name}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise Exception(f"Harmonization failed for {table_name}: {e}")
    print(f"DEBUG read_files: Combined dataframe shape = {combined_df.shape}")
    logger.info(f"Combined {len(dfs)} file(s) for {table_name}: total shape {combined_df.shape}")
    
    # Drop duplicates in combined data
    initial_rows = len(combined_df)
    combined_df.drop_duplicates(inplace=True, keep='first')
    if len(combined_df) < initial_rows:
        logger.info(f"Dropped {initial_rows - len(combined_df)} duplicate rows after combining")
    else:
        logger.info(f"No duplicate rows found after combining for {table_name}")


 
    return combined_df


# ============================================================================
# DATA TYPE ALIGNMENT
# ============================================================================

def align_column_dtypes(existing_df: pd.DataFrame, new_df: pd.DataFrame, logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Align data types between existing and new DataFrames to ensure accurate comparison.
    
    Based on the pattern from handle_hist_data.py
    
    Args:
        existing_df: Existing persistent DataFrame
        new_df: New incoming DataFrame
        logger: Logger instance
    
    Returns:
        Tuple of (aligned_existing_df, aligned_new_df)
    """
    for col in new_df.columns:
        if col in existing_df.columns:
            existing_dtype = existing_df[col].dtype
            new_dtype = new_df[col].dtype
            
            try:
                # Skip if types already match
                if existing_dtype == new_dtype:
                    continue
                
                # Handle datetime types
                elif is_datetime64_any_dtype(existing_dtype) or is_datetime64_any_dtype(new_dtype):
                    existing_df[col] = pd.to_datetime(existing_df[col], errors='coerce')
                    new_df[col] = pd.to_datetime(new_df[col], errors='coerce')
                    logger.debug(f"Aligned datetime column '{col}'")
                
                # Handle string/object types
                elif is_string_dtype(existing_dtype) or is_string_dtype(new_dtype) or \
                     is_object_dtype(existing_dtype) or is_object_dtype(new_dtype):
                    existing_df[col] = existing_df[col].fillna("").astype(str)
                    new_df[col] = new_df[col].fillna("").astype(str)
                    logger.debug(f"Aligned string column '{col}'")
                
                # Handle int/float mismatch
                elif (is_integer_dtype(existing_dtype) and is_float_dtype(new_dtype)) or \
                     (is_float_dtype(existing_dtype) and is_integer_dtype(new_dtype)):
                    existing_df[col] = existing_df[col].astype(float)
                    new_df[col] = new_df[col].astype(float)
                    logger.debug(f"Aligned numeric column '{col}' to float")
                
                # Handle other numeric types
                elif is_numeric_dtype(existing_dtype) and is_numeric_dtype(new_dtype):
                    existing_df[col] = existing_df[col].fillna(0).astype(int)
                    new_df[col] = new_df[col].fillna(0).astype(int)
                    logger.debug(f"Aligned numeric column '{col}' to int")
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting column '{col}': {e}")
            except Exception as e:
                logger.error(f"Unexpected error aligning column '{col}': {e}")
    
    return existing_df, new_df


def harmonize_single_dataframe_vectorized(
    df: pd.DataFrame,
    logger,
    numeric_threshold: float = 0.80
) -> pd.DataFrame:
    """
    Harmonize a single DataFrame to prevent Parquet schema failures using vectorized operations.
    
    Use AFTER concatenating multiple DataFrames.
    
    Strategy:
    - Phase 1: Detect mixed-type columns using vectorized to_numeric() (checks ALL rows, no sampling)
    - Phase 2: Smart classification with leading zeros, alphanumeric, and numeric ratio analysis
    - Phase 3: Apply targeted conversions (INT64 for mostly-numeric, STRING for others)
    - Works efficiently on datasets of ANY size (1M+ rows)
    
    Args:
        df: Single DataFrame to harmonize
        logger: Logger instance
        numeric_threshold: Ratio for INT64 conversion (default 0.80 = 80%)
    
    Returns:
        Harmonized DataFrame with consistent column types
    """
    try:
        if df is None or df.empty:
            logger.info("DataFrame is empty or None, returning as-is")
            return df
        
        logger.info(f"Starting vectorized harmonization for DataFrame with shape {df.shape}")
        
        column_issues: Dict[str, str] = {}
        numeric_ratios: Dict[str, float] = {}
        fixes_applied: Dict[str, str] = {}
        
        # ---- Phase 1 & 2: Detect and Classify (VECTORIZED) ----
        for col in df.columns:
            series = df[col].dropna()

            str_series = series.astype(str)
            
            if len(series) == 0:
                continue

            if str_series.str.contains(r'[A-Za-z]', na=False).any():
                column_issues[col] = "alphanumeric_identifier"
                numeric_ratios[col] = 0
                continue

            
            # Skip if already pure numeric dtype
            if pd.api.types.is_numeric_dtype(series):
                continue

            
            # ---- VECTORIZED DETECTION: Try to convert entire column ----
            numeric_attempt = pd.to_numeric(series, errors='coerce')
            
            converted_count = numeric_attempt.notna().sum()
            non_converted_count = numeric_attempt.isna().sum()
            total_count = len(series)
            
            # Skip if no conversions possible (all are non-numeric)
            if converted_count == 0:
                continue
            
            # Skip if all converted (pure numeric strings)
            if non_converted_count == 0:
                continue
            
            # ---- At this point: HAS BOTH numeric AND non-numeric (MIXED) ----
            numeric_ratio = converted_count / total_count
            numeric_ratios[col] = numeric_ratio
            
            # ---- SMART CLASSIFICATION ----
            # Check for leading zeros (vectorized)
            has_leading_zero = (str_series.str.strip()
                            .str.match(r'^0\d').any())
            
            # Check for alphanumeric (vectorized)
            has_alpha = str_series.str.contains(r'[a-zA-Z]', na=False).any()
            
            # Classification priority
            if has_leading_zero:
                # FIRST: Preserve leading zeros → STRING
                column_issues[col] = "leading_zero_identifier"
            elif has_alpha:
                # SECOND: Alphanumeric → STRING
                column_issues[col] = "alphanumeric_identifier"
            elif numeric_ratio > numeric_threshold:
                # THIRD: Mostly numeric → INT64 (smart)
                column_issues[col] = "mostly_numeric_with_garbage"
            else:
                # FOURTH: Mixed with low numeric ratio → STRING
                column_issues[col] = "mixed_python_types"
        
        # ---- Phase 3: Apply Fixes ----
        for col, issue in column_issues.items():
            numeric_ratio = numeric_ratios.get(col, 0)
            
            if issue == "mostly_numeric_with_garbage":
                # Smart INT64 conversion
                logger.warning(
                    f"Column '{col}': {numeric_ratio:.0%} numeric → Converting to INT64 "
                    f"(non-numeric values → NaN)"
                )
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    fixes_applied[col] = f"INT64 (ratio={numeric_ratio:.0%})"
                except Exception as e:
                    logger.warning(f"INT64 conversion failed for '{col}': {e}, fallback to STRING")
                    df[col] = df[col].astype(str)
                    fixes_applied[col] = "STRING (INT64 failed)"
            else:
                # All other issues → STRING (safe)
                logger.warning(
                    f"Column '{col}': {issue} ({numeric_ratio:.0%} numeric) → Converting to STRING"
                )
                df[col] = df[col].astype(str)
                fixes_applied[col] = f"STRING ({issue})"
        
        # ---- Summary Report ----
        if fixes_applied:
            logger.warning(f"\n{'='*70}")
            logger.warning(f"HARMONIZATION COMPLETE: {len(fixes_applied)} columns fixed")
            logger.warning(f"{'='*70}")
            for col, fix in sorted(fixes_applied.items()):
                logger.warning(f"  {col:35} → {fix}")
            logger.warning(f"{'='*70}\n")
        else:
            logger.info(" No mixed-type columns detected\n")
    except Exception as e:
        logger.error(f"Error during vectorized harmonization: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise Exception(f"Vectorized harmonization failed: {e}")   
    
    return df

# ============================================================================
# DEDUPLICATION
# ============================================================================

def filter_out_duplicate_data(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    table_name: str,
    logger
) -> pd.DataFrame:
    """
    Filter out rows from new_df that already exist in existing_df based on business keys.
    
    Uses tuple-based comparison pattern from handle_hist_data.py
    
    Args:
        existing_df: Existing persistent DataFrame
        new_df: New incoming DataFrame
        table_name: SAP table name
        logger: Logger instance
    
    Returns:
        DataFrame containing only new, unique rows
    """
    if existing_df is None or len(existing_df) == 0:
        logger.info(f"No existing data for {table_name}, all new data is unique")
        return new_df
    
    if new_df is None or len(new_df) == 0:
        logger.info(f"No new data for {table_name}")
        return pd.DataFrame()
    
    # Get business keys for this table
    subset_columns = UNIQUE_COLUMNS_MAP.get(table_name, None)
    
    if subset_columns is None:
        logger.warning(
            f"No business keys defined for table {table_name}. "
            "Using all common columns for deduplication."
        )
        # Use all common columns as fallback
        common_cols = list(set(existing_df.columns).intersection(set(new_df.columns)))
        if not common_cols:
            logger.error(f"No common columns between existing and new data for {table_name}")
            return new_df
        subset_columns = common_cols
    
    # Verify business keys exist in both dataframes
    missing_in_existing = [k for k in subset_columns if k not in existing_df.columns]
    missing_in_new = [k for k in subset_columns if k not in new_df.columns]
    
    if missing_in_existing or missing_in_new:
        logger.warning(
            f"Business keys missing for {table_name}. "
            f"Missing in existing: {missing_in_existing}, Missing in new: {missing_in_new}"
        )
        # Use only available keys
        available_keys = [k for k in subset_columns if k in existing_df.columns and k in new_df.columns]
        if not available_keys:
            logger.error(f"No valid business keys available for {table_name}, cannot deduplicate")
            return new_df
        subset_columns = available_keys
        logger.info(f"Using available keys for {table_name}: {subset_columns}")
    
    # Align data types before comparison
    existing_df, new_df = align_column_dtypes(existing_df, new_df, logger)
    
    # Use tuple-based comparison (pattern from handle_hist_data.py)
    logger.info(f"Deduplicating {table_name} using keys: {subset_columns}")
    
    try:
        # Create tuple representation of subset columns for comparison
        unique_df = new_df[
            ~(new_df[subset_columns].apply(tuple, axis=1).isin(
                existing_df[subset_columns].apply(tuple, axis=1)
            ))
        ]
        
        duplicates_count = len(new_df) - len(unique_df)
        logger.info(
            f"Deduplication for {table_name}: "
            f"{len(new_df)} new rows, {duplicates_count} duplicates filtered, "
            f"{len(unique_df)} unique rows to add"
        )
        
        if unique_df.empty:
            return pd.DataFrame()
        else:
            return unique_df
        
    except Exception as e:
        logger.error(f"Error during deduplication for {table_name}: {e}")
        logger.warning(f"Returning all new data without deduplication")
        return new_df


# ============================================================================
# TRANSACTIONAL TABLE UTILITIES
# ============================================================================

def get_transactional_parquet_path(base_path: str, master_folder_name: str, transactional_parquet_folder_name: str) -> str:
    """
    Get the path to the transactional_parquet directory where run-scoped parquet files are stored.
    
    Path structure: UPLOADS/dow_transformation/transactional_parquet/
    
    Args:
        base_path: Base uploads directory
        master_folder_name: Master folder name (e.g., 'dow_transformation')
        transactional_parquet_folder_name: Transactional parquet folder name (e.g., 'transactional_parquet')
    
    Returns:
        Absolute path to transactional_parquet directory
    """
    return os.path.join(base_path, master_folder_name, transactional_parquet_folder_name)


def delete_source_files(source_files: List[str], sap_run_folder: str, logger) -> List[str]:
    """
    Delete source Excel/CSV files after successful parquet conversion.
    
    Args:
        source_files: List of filenames to delete
        sap_run_folder: Path to run-scoped SAP folder
        logger: Logger instance
    
    Returns:
        List of successfully deleted filenames
    """
    deleted_files = []
    
    for filename in source_files:
        file_path = os.path.join(sap_run_folder, filename)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_files.append(filename)
                logger.info(f"   Deleted source file: {filename} in path {sap_run_folder}")
            else:
                logger.warning(f"   Source file not found (already deleted?): {filename}")
        except Exception as e:
            logger.warning(f"   Failed to delete {filename}: {e}")
    
    return deleted_files




def replace_old_rows_with_new_rows(existing_df,new_df,logger,table_name)-> pd.DataFrame:
    """ Replace old rows in existing_df with new rows from new_df based on business keys."""
    if existing_df is None or existing_df.empty:
        logger.info(f"No existing data for {table_name}, all new data will be added")
        return new_df

    if new_df is None or new_df.empty:
        logger.info(f"No new data for {table_name}, existing data remains unchanged")
        return existing_df

    # Get business keys for this table
    subset_columns = UNIQUE_COLUMNS_MAP.get(table_name, None)

    if subset_columns is None:
        logger.warning(
            f"No business keys defined for table {table_name}. "
            "Using all common columns for replacement."
        )
        # Use all common columns as fallback
        common_cols = list(set(existing_df.columns).intersection(set(new_df.columns)))
        if not common_cols:
            logger.error(f"No common columns between existing and new data for {table_name}")
            return existing_df
        subset_columns = common_cols

    # Verify business keys exist in both dataframes
    missing_in_existing = [k for k in subset_columns if k not in existing_df.columns]
    missing_in_new = [k for k in subset_columns if k not in new_df.columns]

    if missing_in_existing or missing_in_new:
        logger.warning(
            f"Business keys missing for {table_name}. "
            f"Missing in existing: {missing_in_existing}, Missing in new: {missing_in_new}"
        )
        # Use only available keys
        available_keys = [k for k in subset_columns if k in existing_df.columns and k in new_df.columns]
        if not available_keys:
            logger.error(f"No valid business keys available for {table_name}, cannot replace rows")
            return existing_df
        subset_columns = available_keys
        logger.info(f"Using available keys for {table_name}: {subset_columns}")

    # Align data types before comparison
    existing_df, new_df = align_column_dtypes(existing_df, new_df, logger)


    # Identify rows in existing_df that match new_df on business keys
    mask = existing_df[subset_columns].apply(tuple, axis=1).isin(
        new_df[subset_columns].apply(tuple, axis=1)
    )

    # Remove matching rows from existing_df
    filtered_existing_df = existing_df[~mask]
    logger.info(
        f"Replacement for {table_name}: "
        f"{len(existing_df) - len(filtered_existing_df)} rows replaced, "
        f"{len(filtered_existing_df)} rows remain from existing data"
    )
    # Combine filtered existing data with all new data
    updated_df = pd.concat([filtered_existing_df, new_df], ignore_index=True)
    
    # Harmonize dtypes to prevent Parquet conversion errors
    updated_df = harmonize_single_dataframe_vectorized(df=updated_df, logger=logger)

    return updated_df


def special_case_handle_for_lfbk(existing_df,new_df,logger):
    """ Special case handling for LFBK table during data ingestion."""
    matching_vendors_list = []
    
    if existing_df is None or existing_df.empty:
        logger.info("No existing data for LFBK, all new data will be added")
        return new_df,matching_vendors_list

    if new_df is None or new_df.empty:
        logger.info("No new data for LFBK, existing data remains unchanged")
        return existing_df, matching_vendors_list

    # Business keys for LFBK
    subset_columns = UNIQUE_COLUMNS_MAP.get('LFBK', None)

    if subset_columns is None:
        logger.error("Business keys not defined for LFBK, cannot perform special handling")
        return existing_df,matching_vendors_list
    
    # Verify business keys exist in both dataframes
    missing_in_existing = [k for k in subset_columns if k not in existing_df.columns]
    missing_in_new = [k for k in subset_columns if k not in new_df.columns] 

    if missing_in_existing or missing_in_new:
        logger.warning(
            f"Business keys missing for LFBK. "
            f"Missing in existing: {missing_in_existing}, Missing in new: {missing_in_new}"
        )
        # Use only available keys
        available_keys = [k for k in subset_columns if k in existing_df.columns and k in new_df.columns]
        if not available_keys:
            logger.error("No valid business keys available for LFBK, cannot perform special handling")
            return existing_df,matching_vendors_list
        subset_columns = available_keys
        logger.info(f"Using available keys for LFBK: {subset_columns}")

    # Align data types before comparison
    existing_df, new_df = align_column_dtypes(existing_df, new_df, logger)

    # Identify rows in existing_df that match new_df on business keys
    mask = existing_df[subset_columns].apply(tuple, axis=1).isin(
        new_df[subset_columns].apply(tuple, axis=1)
    )

    matching_rows_df = existing_df[mask]
    logger.info(
        f"LFBK Special Handling: "
        f"Identified {len(matching_rows_df)} matching rows in existing data to be replaced"
    )

    matching_vendors_list = matching_rows_df['Supplier'].unique().tolist()
    logger.info(
        f"LFBK Special Handling: "
        f"Vendor IDs with matching rows count: {len(matching_vendors_list)}"
    )


    # Remove matching rows from existing_df
    filtered_existing_df = existing_df[~mask]
    logger.info(
        f"LFBK Special Handling: "
        f"{len(existing_df) - len(filtered_existing_df)} rows replaced, "
        f"{len(filtered_existing_df)} rows remain from existing data"
    )

    # Combine filtered existing data with all new data
    updated_df = pd.concat([filtered_existing_df, new_df], ignore_index=True)
    
    # Harmonize dtypes to prevent Parquet conversion errors
    updated_df = harmonize_single_dataframe_vectorized(df=updated_df, logger=logger)

    return updated_df, matching_vendors_list



def create_synthetic_bseg_from_bkpf(logger, bkpf_path, master_parquet_path):
    """
    Create synthetic BSEG from BKPF when BSEG file is unavailable.
    
    Args:
        logger: Logger instance
        bkpf_path: Path to BKPF parquet file
        master_parquet_path: Path to master_parquet directory (for saving BSEG as master file)
    
    Returns:
        Dict with status, rows count, and parquet path OR False if validation fails
    """
    
    # Step 1: Check if BKPF path exists
    if not os.path.exists(bkpf_path):
        logger.error(f"BKPF parquet not found at {bkpf_path}")
        return False
    
    # Step 2: Load BKPF and check if empty
    try:
        bkpf_df = pd.read_parquet(bkpf_path)
        logger.info(f"Loaded BKPF: {len(bkpf_df)} rows")
    except Exception as e:
        logger.error(f"Failed to read BKPF parquet: {e}")
        return False
    
    if len(bkpf_df) == 0:
        logger.warning("BKPF is empty, cannot create synthetic BSEG")
        return False
    
    bseg_columns = [
    'Client',
    'Company Code',
    'Document Number',
    'Pstg per.var.',
    'Fiscal Year',
    'Debit/Credit',
    'G/L',
    'Amount in LC',
    'Amount',
    'Purchasing Doc.',
    'Item.1',
    'Tax code',
    'Short Text',
    'Line item ID'
    ]

    # Empty dataframe with these columns
    dummy_bseg = pd.DataFrame(columns=bseg_columns)

    # Main BKPF columns used for BSEG merge
    BKPF_MAIN_COLUMNS = [
    'Client',
    'Company Code',
    'Document Number',
    'Pstg per.var.',
    'Fiscal Year'
    ]

    # Extract temp data from BKPF with main columns
    temp_bseg = bkpf_df[BKPF_MAIN_COLUMNS].copy()

    if 'Amount' in bkpf_df.columns:
        temp_bseg['Amount'] = bkpf_df['Amount']
    
    if 'Amount in LC' in bkpf_df.columns:
        temp_bseg['Amount in LC'] = bkpf_df['Amount in LC']
    
    # Add missing BSEG columns with NULL values
    missing_columns = [col for col in bseg_columns if col not in temp_bseg.columns]
    for col in missing_columns:
        temp_bseg[col] = pd.NA
    
    # Concat with dummy to ensure column order
    synthetic_bseg = pd.concat([temp_bseg, dummy_bseg], ignore_index=True)
    synthetic_bseg = synthetic_bseg[bseg_columns]  # Ensure correct column order
    
    synthetic_bseg['Short Text'].fillna("",inplace=True)

    logger.info(f"Created synthetic BSEG: {len(synthetic_bseg)} rows with {len(bseg_columns)} columns")
    
    # Save synthetic BSEG to master parquet location (REQUIRED parameter)
    if not master_parquet_path:
        logger.error("Master parquet path not provided, cannot save synthetic BSEG")
        return False
    
    # Create master BSEG directory and build file path
    master_bseg_folder = os.path.join(master_parquet_path, 'BSEG')
    os.makedirs(master_bseg_folder, exist_ok=True)
    synthetic_bseg_path = os.path.join(master_bseg_folder, 'SAP_BSEG_data.parquet')
    
    # Save synthetic BSEG parquet to master location
    try:
        synthetic_bseg.to_parquet(synthetic_bseg_path, index=False, engine='pyarrow')
        logger.info(f"Saved synthetic BSEG to master location: {synthetic_bseg_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save synthetic BSEG parquet: {e}")
        return False
