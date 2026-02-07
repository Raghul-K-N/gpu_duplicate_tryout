from datetime import datetime, timezone
import re
from code1.logger import capture_log_message
from flask import g
import pandas as pd
import os
from cleanco import basename
from rapidfuzz import fuzz

import pandas as pd
from code1.logger import capture_log_message


def similar_supplier_names(historical_df, vendor_list):
    
    capture_log_message(f"Length of unique historical suppliers' list: {len(historical_df['SUPPLIER_NAME'].unique())}")
    capture_log_message(f"Null vendor names in vendor list: {sum(pd.Series(vendor_list).isnull())}")
    preprocessed_suppliers = historical_df['SUPPLIER_NAME'].apply(preprocess_name)

    cleaned_vendors = [preprocess_name(vendor) for vendor in vendor_list]
    capture_log_message(f"Length of cleaned unique vendors' list: {len(cleaned_vendors)}")
    supplier_matches = preprocessed_suppliers.apply(lambda x: check_supplier_match(x, cleaned_vendors))
    # Convert to numpy boolean array to avoid PyArrow dtype conflicts
    return supplier_matches.astype(bool).to_numpy()
        
        

def check_supplier_match(supl_1:str , vendor_list, threshold = 90):
    supl_1 = supl_1.lower()
    for vendor in vendor_list:
        vendor = vendor.lower()
        if supl_1.startswith(vendor[:2]):
            if fuzz.partial_ratio(supl_1, vendor) >= threshold:
                return True
                
    return False

def preprocess_name(name):
    """
    Preprocess supplier or vendor name by removing special characters and cleaning up suffixes.

    Parameters:
    name (str): The name to preprocess.

    Returns:
    str: Cleaned name.
    """
    # Remove all non-alphanumeric characters
    name = re.sub('[^A-Za-z0-9 ]+', '', name).strip()
    # Use cleanco to remove suffixes like Ltd., Pvt., Inc.
    return basename(name)



def deduplicate_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate duplicate groups across scenarios by removing:
    1. Exact duplicate groups (same invoice set) - keep first occurrence
    2. Subset groups (group A is subset of group B) - keep only superset
    
    Args:
        df: DataFrame with columns [SCENARIO_ID, DUPLICATES_ID, primarykeysimple, ...]
        
    Returns:
        Filtered DataFrame with only non-redundant duplicate groups
    """
    capture_log_message('Deduplicating groups...')
    # Step 1: Create Group Signatures
    capture_log_message("Step 1: Creating group signatures...")
    group_signatures = []

    for (scenario_id, dup_id), group_df in df.groupby(['SCENARIO_ID', 'DUPLICATE_ID']):
        invoice_set = frozenset(group_df['PrimaryKeySimple'].values)
        group_size = len(invoice_set)
        
        # Skip empty groups (shouldn't happen, but safety check)
        if group_size == 0:
            continue

        group_signatures.append({
            'scenario_id': scenario_id,
            'dup_id': dup_id,
            'invoice_set': invoice_set,
            'size': group_size,
            'hash': hash(invoice_set)  # For exact duplicate detection
        })

    capture_log_message(f"  Total groups: {len(group_signatures)}")

    # Step 2: Hash-Based Exact Duplicate Removal
    capture_log_message("\nStep 2: Removing exact duplicates Hash-based...")
    hash_groups = {}

    for sig in group_signatures:
        h = sig['hash']
        if h not in hash_groups:
            hash_groups[h] = []
        hash_groups[h].append(sig)

     # Keep first occurrence (by scenario_id, then dup_id)
    unique_groups = []
    duplicates_removed = 0

    for h, sigs in hash_groups.items():
        if len(sigs) > 1:
            # Sort by scenario_id, then dup_id
            sigs.sort(key=lambda x: (x['scenario_id'], x['dup_id']))
            duplicates_removed += len(sigs) - 1
        unique_groups.append(sigs[0])

    capture_log_message(f"  Exact duplicates removed: {duplicates_removed}")
    capture_log_message(f"  Unique groups remaining: {len(unique_groups)}")

     # Step 3: Sort for Subset Checking
    capture_log_message("\nStep 3: Sorting groups...")
    # Sort by: size DESC, scenario_id ASC, dup_id ASC
    unique_groups.sort(key=lambda x: (-x['size'], x['scenario_id'], x['dup_id']))


    # Step 4: Subset Checking with Size Filter
    capture_log_message("\nStep 4: Checking for subset groups...")
    kept_groups = []
    subsets_removed = 0
    comparison_count = 0

    for current in unique_groups:
        is_subset = False
        
        # Only compare with kept groups that are LARGER
        for kept in kept_groups:
            if kept['size'] > current['size']:
                comparison_count += 1
                
                # Check if current is subset of kept
                if current['invoice_set'].issubset(kept['invoice_set']):
                    is_subset = True
                    subsets_removed += 1
                   
                    break
        
        if not is_subset:
            kept_groups.append(current)

    capture_log_message(f"  Subsets removed: {subsets_removed}")
    capture_log_message(f"  Total comparisons: {comparison_count}")
    capture_log_message(f"  Final groups retained: {len(kept_groups)}")

    # Step 5: Filter Original DataFrame
    print("\nStep 5: Filtering original dataframe...")
    kept_keys = {(g['scenario_id'], g['dup_id']) for g in kept_groups}
    
    result_df = df[df.apply(
        lambda row: (row['SCENARIO_ID'], row['DUPLICATE_ID']) in kept_keys, 
        axis=1
    )].copy()
    
    capture_log_message(f"  Original rows: {len(df)}")
    capture_log_message(f"  Filtered rows: {len(result_df)}")

    return result_df


def has_strong_overlap(a: str, b: str, min_len=4) -> bool:
    """
    Return True if one string is contained in the other with at least `min_len` overlap.
    """
    if len(a) < min_len or len(b) < min_len:
        return False

    return (a in b and len(a) >= min_len) or (b in a and len(b) >= min_len)



def find_matching_invoice(reversal_row, invoice_rows):
    """
    Find a matching invoice for a reversal entry using tie-breaker rules.

    Matching Logic:
    1. Primary criteria: Same supplier, date, and absolute amount
    2. If exactly one match -> return it
    3. If multiple matches -> apply tie-breakers:
    a. Exact invoice number match (first one if multiple)
    b. If invoice number >= 4 chars: startswith/endswith match (first one if multiple)
    c. Otherwise -> skip (ambiguous)

    Parameters:
    -----------
    reversal_row : pandas.Series
        Reversal entry (S) to match
    invoice_rows : pandas.DataFrame  
        Invoice entries (H) to match against
        
    Returns:
    --------
    tuple: (int or None, str or None)
        Index of matching invoice if found and match type, (None, None) otherwise
    """

    vendor_name = reversal_row['SUPPLIER_NAME']
    invoice_date = reversal_row['INVOICE_DATE']
    invoice_amount_abs = abs(reversal_row['INVOICE_AMOUNT'])
    reversal_invoice_number = str(reversal_row.get('INVOICE_NUMBER', '')).strip()
    # Find candidate rows matching primary criteria
    matching_rows = invoice_rows[
        (invoice_rows['SUPPLIER_NAME'] == vendor_name) &
        (invoice_rows['INVOICE_DATE'] == invoice_date) &
        (abs(invoice_rows['INVOICE_AMOUNT']) == invoice_amount_abs)
    ]
    n_matches = len(matching_rows)
    # No matches found
    if n_matches == 0:
        return None, None
    # Exactly one match - return it
    if n_matches == 1:
        return matching_rows.index[0], 'single'
    # Multiple matches - apply tie-breakers
    # Tie-breaker 1: Exact invoice number match
    inv_numbers = matching_rows['INVOICE_NUMBER'].astype(str).str.strip()
    exact_matches = matching_rows[inv_numbers == reversal_invoice_number]
    if len(exact_matches) >= 1:
        return exact_matches.index[0], 'exact'  # Return first exact match
    # Tie-breaker 2: Invoice number similarity (only if length >= 4)
    if len(reversal_invoice_number) >= 4:

        similarity_mask = inv_numbers.apply(
        lambda x: has_strong_overlap(str(x), reversal_invoice_number, 4)
        )

        similarity_matches = matching_rows[similarity_mask]
        if len(similarity_matches) >= 1:
            return similarity_matches.index[0], 'similarity'  # Return first similarity match
    # No resolution possible - return None (ambiguous case)
    return None, None




def filter_reversal_entries_before_duplicate_check(df,audit_id):
    """
    Filter out reversal entries (S) that match with invoice entries (H) before duplicate processing.
    Uses sophisticated matching with tie-breaker rules for ambiguous cases.

    Parameters:
    -----------
    df : pandas.DataFrame 
        Input dataframe with DEBIT_CREDIT_INDICATOR column
        
    Returns:
    --------
    pandas.DataFrame
        Filtered dataframe with matched reversal-invoice pairs removed
    """

    start_time = datetime.now(timezone.utc)
    capture_log_message(f"Starting reversal filtering. Input data shape: {df.shape}")

    # Create working copy
    filtered_df = df.copy()

    # Separate invoice and reversal entries
    invoice_entries = filtered_df[filtered_df['DEBIT_CREDIT_INDICATOR'] == 'H'].copy()
    reversal_entries = filtered_df[filtered_df['DEBIT_CREDIT_INDICATOR'] == 'S'].copy()

    capture_log_message(f"Invoice entries (H): {len(invoice_entries)}, Reversal entries (S): {len(reversal_entries)}")

    # Return early if no processing needed
    if reversal_entries.empty or invoice_entries.empty:
        capture_log_message("No reversal or invoice entries found. Returning original data.")
        return filtered_df

    # Track processing results by match type
    rows_to_drop = []
    match_counts = {
        'single': 0,      # Single candidate match
        'exact': 0,       # Exact invoice number match
        'similarity': 0,  # Startswith/endswith match
    }
    no_matches = 0

    # Process each reversal entry
    for reversal_idx, reversal_row in reversal_entries.iterrows():
        try:
            # Find matching invoice using tie-breaker logic
            matching_invoice_idx, match_type = find_matching_invoice(reversal_row, invoice_entries)
            
            if matching_invoice_idx is not None:
                # Found a match - mark both for removal
                rows_to_drop.extend([reversal_idx, matching_invoice_idx])
                match_counts[match_type] += 1
                
                # Remove matched invoice to prevent duplicate matching
                invoice_entries = invoice_entries.drop(matching_invoice_idx)
            else:
                no_matches += 1
                    
        except Exception as e:
            capture_log_message(current_logger=g.error_logger, log_message=f"Error processing reversal entry {reversal_idx}: {str(e)}", store_in_db=False)
            continue


    # Apply all drops at once (more efficient)
    if rows_to_drop:
        rows_to_drop = list(set(rows_to_drop))  # Remove duplicates
        # Save dropped rows to CSV before filtering
        try:
            dropped_rows_df = filtered_df.loc[rows_to_drop].copy()
            
            csv_filename = f"reversal_invoice_matched_pairs_audit_{audit_id}.csv"
            csv_filepath = os.path.join(g.processed_output_folder, csv_filename)
            
            dropped_rows_df.to_csv(csv_filepath, index=True)
            capture_log_message(f"Saved {len(rows_to_drop)} dropped reversal-invoice matched pairs to: {csv_filepath}")
            
        except Exception as csv_error:
            capture_log_message(current_logger=g.error_logger, 
                            log_message=f"Error saving dropped rows to CSV: {str(csv_error)}")
        filtered_df = filtered_df.drop(rows_to_drop)

    # Calculate total matched pairs
    total_matched_pairs = sum(match_counts.values())

    # Log summary results with match type breakdown
    capture_log_message(f"Duplicate Reversal filtering completed:")
    capture_log_message(f" - Total matched pairs: {total_matched_pairs}")
    capture_log_message(f"    * Single matches: {match_counts['single']}")
    capture_log_message(f"    * Exact number matches: {match_counts['exact']}")
    capture_log_message(f"    * Similarity matches: {match_counts['similarity']}")
    capture_log_message(f" - No matches: {no_matches}")
    capture_log_message(f" - Total rows dropped: {len(rows_to_drop)}")
    capture_log_message(f"Reversal filtering Input shape: {df.shape}, Reversal filtering Output shape: {filtered_df.shape}")

    total_time = datetime.now(timezone.utc) - start_time
    capture_log_message(f"Total reversal filtering time: {total_time}")

    return filtered_df
        

def filter_out_duplicate_credit_debit_pairs(existing_df: pd.DataFrame, new_df: pd.DataFrame):
    """
    Filter out duplicate credit/debit pairs based on key columns
    Args:
        existing_df (pd.DataFrame): Existing data from DB
        new_df (pd.DataFrame): New data to be inserted
    Returns:
        pd.DataFrame: Filtered new data with only unique records
    """
    subset_columns = ['ENTRY_ID', 'COMPANY_CODE', 'POSTED_DATE', 'INVOICE_NUMBER', 'INVOICE_DATE']

    # Convert to tuples for comparison
    existing_tuples = set(existing_df[subset_columns].apply(tuple, 1))
    new_tuples = new_df[subset_columns].apply(tuple, 1)
    # Keep only rows not in existing data
    unique_mask = ~new_tuples.isin(existing_tuples)
    unique_df = new_df[unique_mask].copy()

    return unique_df


def is_sequential_series(str1: str, str2: str) -> bool:
    """
    Check if two alphanumeric strings are sequential series numbers
    based on the last 2–3 digits (only if they end with numbers).
    Example: INV-1234 vs INV-1235
            INV-1204 vs INV-1304
    """
    if len(str1)!=len(str2):
        return False

    # Fetch Numbers at the end of the string (last char is a number)
    match1 = re.search(r'(\d+)$', str1)
    match2 = re.search(r'(\d+)$', str2)


    if not match1 or not match2:
        return False

    prefix1, num_str1 = str1[:match1.start()], match1.group(1)
    prefix2, num_str2 = str2[:match2.start()], match2.group(1)


    # Only proceed if prefixes match
    if prefix1 != prefix2:
        return False
        # print('Prefix is same')

    # Check last 2 or 3 digits
    for k in (2, 3):
        if len(num_str1) >= k and len(num_str2) >= k:
            last1 = num_str1[-k:]
            last2 = num_str2[-k:]
            # print(last1,last2)
            if last1 != last2:   # <-- ANY difference, not only +1
                return True

    return False



def get_matching_invoice_rows(historical_df, invoices_list):
            
    """
    Get rows from historical_df where INVOICE_NUMBER matches any in invoices_list.
    """
    # Convert invoices_list to a set for faster lookup
    invoices_set = set(invoices_list)
    # Filter the DataFrame based on the condition
    matching_rows_mask = historical_df['INVOICE_NUMBER'].isin(invoices_set)
    # Convert to numpy boolean array to avoid PyArrow dtype conflicts
    return matching_rows_mask.to_numpy()
    # invoice_numbers = historical_df['INVOICE_NUMBER'].astype(str).fillna('')
    # match_mask = pd.Series(False, index=historical_df.index)

    # capture_log_message(f"Finding matches for invoice list: {len(invoices_list)}")

    # for inv in invoices_list:
    #     if not isinstance(inv, str):
    #         inv = str(inv)
    #     inv = inv.strip()
    #     if not inv:
    #         continue

    #     # Numeric invoices → exact match or suffix starting with letter
    #     if inv.isdigit():
    #         pattern = rf"^(?:{re.escape(inv)}$|{re.escape(inv)}[A-Za-z][A-Za-z0-9]*)$"
    #     else:
    #         # Alphanumeric invoices → prefix + optional alphanumeric suffix
    #         pattern = rf"^{re.escape(inv)}([A-Za-z0-9]*)?$"

    #     mask = invoice_numbers.str.match(pattern, case=False, na=False)
    #     match_mask |= mask

    # return match_mask


def get_matching_rows_with_same_invoice_date(historical_df,invoices_date_list):
    """
    Get rows from historical_df where INVOICE_DATE matches any in invoices_date_list.
    """
    # Convert invoices_list to a set for faster lookup
    invoices_date_set = set(invoices_date_list)
    # Filter the DataFrame based on the condition
    matching_rows_mask = historical_df['INVOICE_DATE'].isin(invoices_date_set)
    # Convert to numpy boolean array to avoid PyArrow dtype conflicts
    return matching_rows_mask.to_numpy()