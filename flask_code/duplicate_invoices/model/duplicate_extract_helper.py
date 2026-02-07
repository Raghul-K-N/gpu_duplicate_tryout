from code1.src_load import read_table
from duplicate_invoices.config.config import SCENARIO_TABLE_NAME,THRESHOLD_VALUE
from rapidfuzz.distance import Levenshtein
from flask import g
import pandas as pd
import uuid
import networkx as nx
from datetime import datetime
import re



SCORE_THRESOLD = THRESHOLD_VALUE
SHORT_LENGTH_THRESHOLD = 4 # Any input with length less than or equal to this will be considered short strings
SEQUENCE_THRESHOLD = 99  # if the absolute difference between two numbers is less than or equal to this, they are considered as sequence numbers
NUMBER_THRESHOLD = 9999  # if the absolute difference between two numbers is greater than this, they are considered as numbers that are not similar

def get_similarity_score(val1,val2):
    """
    Calculate the similarity score between two values.
    
    Args:
        val1 (str): The first value.
        val2 (str): The second value.
        
    Returns:
        float: Similarity score between 0 and 100.
    """
    # if either Invoice number is not given
    if not val1 or not val2:
        return False, 0.0
    
    # Calculate Score requirements based on Levenshtein
    distance = len(Levenshtein.editops(val1, val2))
    length = max(len(val1), len(val2))
    
    # If there are no difference between inputs
    if length == 0:
        return False, 0.0
    
    # score based results
    score = (length - distance) * 100 / length
    return (score >= SCORE_THRESOLD, score)

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

def check_for_single_number_edit(str1: str,str2: str,len_str1: int,len_str2: int,forbid_ends: bool = True) -> bool:
    """
    Return True if exactly one Levenshtein edit (insert/delete/substitute)
    transforms str1 into str2, excluding edits at first/last positions if forbid_ends.
    """
    ops = Levenshtein.editops(str1, str2)
    if len(ops) != 1:
        return False

    op, src_i, dst_i = ops[0]  # op in {'replace','insert','delete'}

    # optionally forbid editing first or last character
    if forbid_ends:
        if len_str1==len_str2:
            if op == 'replace' and (src_i == 0 or src_i == len_str1 - 1):
                return False
            if op == 'insert' and (dst_i == 0 or dst_i == len_str2 - 1):
                return False
            if op == 'delete' and (src_i == 0 or src_i == len_str1 - 1):
                return False

    # same-length: substitution
    if len_str1 == len_str2:
        return op == 'replace'
    # length difference: insertion or deletion only
    if abs(len_str1 - len_str2) == 1:
        return op in ('insert', 'delete')

    return False

def check_similarity_for_short_inputs_numbers(val1:str,val2:str, isnum1:bool, isnum2:bool, invoice1_length:int, invoice2_length:int):
    """
    Check similarity for short inputs based on sequence numbers or similar.
    
    Args:
        val1 (str): The first value.
        val2 (str): The second value.
        isnum1 (bool): True if val1 is a number, False otherwise.
        isnum2 (bool): True if val2 is a number, False otherwise.
        
    Returns:
        bool: True if the values are similar, False otherwise.
    """
    if isnum1 and isnum2:
        if val1 == val2:
            return True, 100.0
        
        # If both are numbers, check if they are sequence numbers and return True if they are sequence numbers
        # diff = abs(int(val1) - int(val2))
        # is_sequence = diff <= sequence_threshold
        # score = 75.0 if is_sequence else 0  # Adjust score as needed for sequence numbers in future
        
        # if numbers are sequence or not , marking as not similar
        return False , 0
    
    elif isnum1 or isnum2:
        # Logic for IN condition
        if check_for_special_case_in_condition(invoice1_length, invoice2_length, val1, val2):
            return True, 90.0
        # If one is a number and the other is not, check if they are similar
        return get_similarity_score(val1, val2)
    
    else:
        # If both are not numbers, check if they are similar
        return get_similarity_score(val1, val2)
    

def check_similarity_for_long_inputs_numbers(val1:str,val2:str, isnum1:bool, isnum2:bool, invoice1_length:int, invoice2_length:int):
    """
    Check similarity for long inputs based on sequence numbers or similar.
    
    Args:
        val1 (str): The first value.
        val2 (str): The second value.
        isnum1 (bool): True if val1 is a number, False otherwise.
        isnum2 (bool): True if val2 is a number, False otherwise.
        
    Returns:
        bool: True if the values are similar, False otherwise.
    """
    if isnum1 and isnum2:
        # If both are numbers, check if they are sequence numbers and return True if they are sequence numbers
        if val1 == val2:
            return True, 100.0
        
        if check_for_single_number_edit(val1, val2, invoice1_length, invoice2_length):
            return True, 95.0
        
        #  if the numbers are sequence or not , marking as not similar
        return False, 0.0

    if (isnum1 or isnum2) or (not isnum1 and not isnum2):
        if check_for_special_case_in_condition(invoice1_length, invoice2_length, val1, val2):
            return True, 90.0
        # Shared substring matching logic
        if abs(invoice1_length - invoice2_length) < 3:
            if (val1 in val2) or (val2 in val1):
                return True, 95
        # if invoice1_length == invoice2_length:
        #     if is_sequential_series(val1,val2):
        #         return False, 0.0
        # (One is numeric, the other isn't) or (Bothe are Alpha numeric) — defer to general similarity score
        return get_similarity_score(val1, val2)


    else:
        # If both are not numbers, check if they are similar
        return get_similarity_score(val1, val2)

def check_for_special_case_in_condition(len1, len2, invoice1_str, invoice2_str):
    """
    Check similarity for inputs based on sub-string search.
    
    Args:
        len1 (str): The first input length.
        len2 (str): The second input length.
        invoice1 (str): The content of the first invoice.
        invoice2 (str): The content of the second invoice.
        
    Returns:
        bool: True if the values are similar, False otherwise.
    """
    # Defining the variables required
    max_len = max(len1,len2)
    min_len = min(len1,len2)

    

    # defining the special case condition for sub-string search
    if (min_len * 2) >= max_len:
        # Calculating sub-string condition flag
        in_condition = (invoice1_str in invoice2_str) or (invoice2_str in invoice1_str)
        if in_condition:
            return True
    return False


def is_invoice_similar(invoice1, invoice2):
    """
    Check if two invoices are similar based on their content.
    
    Args:
        invoice1 (str): The content of the first invoice.
        invoice2 (str): The content of the second invoice.
        
    Returns:
        bool: True if the invoices are similar, False otherwise.
        score (float): Similarity score between 0 and 100.
    """
    
    # Convert to string and check if numeric
    invoice1_str = str(invoice1)
    invoice2_str = str(invoice2)
    
    is_num1 = invoice1_str.isdigit()
    is_num2 = invoice2_str.isdigit()
    
    # Convert numbers to normalized string format
    if is_num1:
        invoice1_str = str(int(invoice1_str))
    if is_num2:
        invoice2_str = str(int(invoice2_str))
    
    # Early returns for edge cases
    if not invoice1_str or not invoice2_str:
        return False, 0.0
    
    # Logic for Exact matching inputs
    if invoice1_str == invoice2_str:
        return True, 100.0
    
    # Input length Calculation and it purpose
    len1, len2 = len(invoice1_str), len(invoice2_str)
    both_short = len1 <= SHORT_LENGTH_THRESHOLD and len2 <= SHORT_LENGTH_THRESHOLD
    one_short = len1 <= SHORT_LENGTH_THRESHOLD or len2 <= SHORT_LENGTH_THRESHOLD
    
    # Logic for short inputs
    if both_short:
        if is_num1 and is_num2:
            return check_similarity_for_short_inputs_numbers(invoice1_str, invoice2_str, True, True, len1, len2)
        elif is_num1 or is_num2:
            return check_similarity_for_short_inputs_numbers(invoice1_str, invoice2_str, is_num1, is_num2, len1, len2)
        else:
            # Both not numbers - check substring match first
            return get_similarity_score(invoice1_str, invoice2_str)
    
    # One short, one long
    elif one_short:
        min_len, max_len = min(len1, len2), max(len1, len2)
        
        # If longer input is more than double the shorter, return False
        if max_len >= (min_len * 2):
            return False, 0.0
        
        # Handle numeric cases (pass for both numbers as per original logic)
        if is_num1 and is_num2:
            pass
        elif is_num1 or is_num2:
            # Logic for IN condition
            if check_for_special_case_in_condition(len1, len2, invoice1_str, invoice2_str):
                return True, 90.0
            return get_similarity_score(invoice1_str, invoice2_str)
        else:
            # Logic for IN condition
            if check_for_special_case_in_condition(len1, len2, invoice1_str, invoice2_str):
                return True, 90.0
            # Shared substring matching logic
            if abs(len1 - len2) < 3 and (invoice1_str in invoice2_str or invoice2_str in invoice1_str):
                return True, 95
            return get_similarity_score(invoice1_str, invoice2_str)
    
    # Long inputs logic for both numbers
    if is_num1 and is_num2:
        return check_similarity_for_long_inputs_numbers(invoice1_str, invoice2_str, True, True, len1, len2)
    elif not is_num1 or not is_num2:
        # Long inputs logic for both Alphanumeric or (Number and Alphanumeric)
        return check_similarity_for_long_inputs_numbers(invoice1_str, invoice2_str, is_num1, is_num2, len1, len2)
    
    # if is_sequential_series(invoice1_str,invoice2_str):
    #     return False, 0.0
    # Default case - alphanumeric inputs
    return get_similarity_score(invoice1_str, invoice2_str)



def get_scenario_data_for_duplicates():
    """
    Get scenario data for duplicates from the database or a predefined configuration
        
    Returns:
        pd.DataFrame: DataFrame containing scenario data for duplicates.
    """
    # Fetching from database
    try:
        scenarios_df = read_table(SCENARIO_TABLE_NAME)
        # Filter for only scenarios present in the dict keys
        # valid_scenarios = set(g.scenario_threshold_map.keys())
        # scenarios_df = scenarios_df[scenarios_df['SCENARIO_ID'].isin(valid_scenarios)]
        if scenarios_df.empty:
            raise ValueError("No scenario data found in the database.")
        # print(f"Filtered scenarios_df shape {scenarios_df.shape}")
        return scenarios_df
    except Exception as e:
        raise ValueError(f"Failed to fetch scenario data: {e}")
def create_graph_based_groups(pairs_df, scenario_id):
    """
    Create groups using NetworkX graph analysis
    More efficient than the previous dict-based approach
    """
    if pairs_df.empty:
        return pd.DataFrame()
    
    # Build graph
    G = nx.Graph()
    for _, row in pairs_df.iterrows():
        G.add_edge(row['source_pk'], row['dest_pk'], weight=row['score'])
    
    # Find connected components
    components = list(nx.connected_components(G))
    
    # Build result records directly
    records = []
    for component in components:
        group_uuid = str(uuid.uuid4())
        
        # Calculate average score for the component
        subgraph = G.subgraph(component)
        if subgraph.edges:
            avg_score = sum(d['weight'] for _, _, d in subgraph.edges(data=True)) / len(subgraph.edges)
        else:
            avg_score = 0.0
        
        # Add all nodes in this component
        for pk in component:
            records.append({
                'PrimaryKeySimple': pk,
                'SCENARIO_ID': scenario_id,
                'group_uuid': group_uuid,
                'DUPLICATE_RISK_SCORE': avg_score
            })
    
    return pd.DataFrame(records)

def _posted_date_similarity(source_val, dest_val, threshold = 365):
    source_date = datetime.strptime(source_val, "%Y-%m-%d %H:%M:%S")
    dest_date = datetime.strptime(dest_val, "%Y-%m-%d %H:%M:%S")
    return abs((source_date - dest_date).days) <= threshold, 65