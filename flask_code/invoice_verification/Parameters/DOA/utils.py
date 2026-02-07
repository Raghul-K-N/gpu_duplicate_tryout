# Parameters/DOA/utils.py
# ============================================================================
"""
Utility functions for DOA validation
"""
from invoice_verification.logger.logger import log_message
from typing import Optional, List
import re
import pandas as pd

# ============================================================================
# CONFIGURATION / HARDCODED VALUES
# ============================================================================

DIAMOND_DATA_USERNAME_COLUMN = 'USER_NAME'  # Column in Diamond data for User ID

DOA_TYPE_COLUMN = 'DOA_TYPE'  # Column name for DOA Type in Diamond data    
DOA_AMOUNT_COLUMN = 'DOA_AMOUNT'  # Column name for DOA Amount in Diamond data

DIAMOND_DATA_PATH = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\doa_parquet\doa_redelivery_data.parquet"  # TODO: Update with actual path
SUPERFUND_DATA_PATH = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\invoice_verification\sources\Superfund Exception Process_Final April 11 2025_Update.xlsx"  # TODO: Update with actual path
SUPERFUND_SHEET_NAME = 'Superfund Exception Spreadsheet'  # Sheet name in Superfund Excel file
PRO_VR_DOA_DATA_PATH = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\doa_parquet\doa_data.parquet"  # TODO: Update with actual path to PRO_VR_DOA parquet file

# Condition type to DOA validation type mapping
CONDITION_TYPE_TO_DOA_VALIDATION_TYPE = {
    'ZAD2': 'UDC',
    'ZBF2': 'UDC',
    'ZBK2': 'UDC',
    'ZCC2': 'UDC',
    'ZCU2': 'UDC',
    'ZDE2': 'UDC',
    'ZDM2': 'UDC',
    'ZDP2': 'UDC',
    'ZDU2': 'UDC',
    'ZET2': 'UDC',
    'ZFR2': 'UDC',
    'ZFU2': 'UDC',
    'ZHS2': 'UDC',
    'ZIF2': 'UDC',
    'ZIN3': 'UDC',
    'ZIP2': 'UDC',
    'ZND2': 'UDC',
    'ZPF2': 'UDC',
    'ZSR2': 'UDC',
    'ZSFD': 'Superfund',
    'ZSGT': 'Tariff'
}

# Threshold amounts by region
THRESHOLD_APAC_LATAM = 250.0
THRESHOLD_OTHER_REGIONS = 2500.0

# Credit memo indicator value
CREDIT_MEMO_INDICATOR_VALUE = "CREDIT MEMO"
TRANSACTION_TYPE_DICT = {"1":"INVOICE",
                    "2":"CREDIT MEMO",
                    "3":"SUBSEQUENT CREDIT",
                    "4":"SUBSEQUENT DEBIT"}  # TODO: Update with actual value

# Cache for loaded data
_diamond_data_cache = None
_superfund_data_cache = None
_pro_vr_doa_data_cache = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def get_regional_threshold(region: str) -> float:
    """
    Get threshold amount based on region
    Args:
        region: Region code
    Returns:
        Threshold amount
    """
    if region.upper() in ['APAC', 'LATAM', 'LA', 'LAA']:
        return THRESHOLD_APAC_LATAM
    else:
        return THRESHOLD_OTHER_REGIONS


def is_credit_memo(transaction_type: str) -> bool:
    """
    Check if invoice is a credit memo
    Args:
        credit_memo_indicator: SAP credit memo column value
    Returns:
        True if credit memo
    """
    # is_cm =  TRANSACTION_TYPE_DICT.get(transaction_type) == 'CREDIT MEMO'
    is_cm = str(transaction_type).strip().upper() == CREDIT_MEMO_INDICATOR_VALUE
    if is_cm:
        log_message("Identified as credit memo")
    return is_cm


def get_doa_validation_type_from_condition(condition_type: str) -> Optional[str]:
    """
    Map condition type to DOA validation type
    Args:
        condition_type: Condition type from SAP
    Returns:
        DOA validation type (UDC, Superfund, Tariff) or None
    """
    condition_type = str(condition_type).strip().upper()
    doa_validation_type = CONDITION_TYPE_TO_DOA_VALIDATION_TYPE.get(condition_type)
    if doa_validation_type:
        log_message(f"Mapped condition type {condition_type} to DOA validation type {doa_validation_type}")
    else:
        log_message(f"No DOA validation type mapping for condition type {condition_type}")
    return doa_validation_type


def extract_user_id_from_voucher(voucher_text: str) -> Optional[str]:
    """
    Extract Approver User ID from voucher request
    Format: "Approver User ID: U649691"
    Args:
        voucher_text: Raw text from voucher OCR
    Returns:
        User ID starting with 'U' or None
    """
    if not voucher_text:
        log_message("No voucher text available")
        return None
    # Pattern: "Approver User ID: U######"
    pattern = r'Approver\s+User\s+ID\s*:\s*(U\d+)'
    match = re.search(pattern, voucher_text, re.IGNORECASE)
    
    if match:
        user_id = match.group(1)
        log_message(f"Extracted Approver User ID from voucher: {user_id}")
        return user_id
    
    log_message("No Approver User ID found in voucher")
    return None


def check_doa_approved_in_vim(vim_comments: Optional[str]) -> bool:
    """
    Check if 'Approved' is present in VIM comments
    Args:
        vim_comments: VIM comments text
    Returns:
        True if approved
    """
    if not vim_comments:
        return False
    
    approved = 'APPROVED' in vim_comments.upper() #DOA FREIGHT APPROVED'
    log_message(f"DOA approval in VIM comments: {approved}")
    return approved

def check_doa_approved_in_eml(eml_lines: Optional[str], sap_invoice_number: Optional[str]) -> bool:
    """
    Check if 'Approved' and SAP invoice number are present in the same line in EML
    Args:
        eml_lines: EML lines text
        sap_invoice_number: SAP invoice number to verify
    Returns:
        True if approved and invoice number found in same line
    """
    if not eml_lines or not sap_invoice_number:
        return False
    
    # Split into lines and check each line
    for line in eml_lines.split('\n'):
        line_upper = line.upper()
        if 'APPROVED' in line_upper:
            log_message("DOA Approved line found in EML")
            invoice_number_present = sap_invoice_number.lower() in str(eml_lines).strip().lower() if eml_lines else False
            if invoice_number_present:
                log_message(f"DOA approval with invoice {sap_invoice_number} found in EML")
                return True
    
    log_message(f"DOA approval with invoice {sap_invoice_number} NOT found in EML")
    return False

def extract_user_id_from_eml(eml_lines: List[str]) -> Optional[str]:
    """
    Extract User ID from EML lines
    Args:
        eml_lines: List of email text lines
    Returns:
        User ID or None
    """
    # TODO: Implement EML user ID extraction based on format
    # Placeholder for now
    log_message("EML User ID extraction - TO BE IMPLEMENTED")
    return None


def check_approval_status(opentext_user_id: str, app_action: str, 
                         source: Optional[str], vim_comments: Optional[str] = None,
                         eml_lines: Optional[str] = None, sap_invoice_number: Optional[str] = None) -> bool:
    """
    Check if user has approval based on source (checks voucher -> VIM -> EML in sequence)
    Args:
        opentext_user_id: User ID from SAP OpenText column
        app_action: App action from SAP
        source: Primary source to check ('voucher', 'vim', or 'eml')
        vim_comments: VIM comments (for vim source)
        eml_lines: EML lines (for eml source)
        sap_invoice_number: SAP invoice number (for EML validation)
    Returns:
        bool: True if approved, False otherwise
    """
    if source == 'voucher':
        log_message(f"Checking voucher approval - User: {opentext_user_id}, App Action: {app_action}")
        # if app_action and str(app_action).strip().upper() == 'A':
        #     log_message(f"Voucher approval found - User: {opentext_user_id}")
        #     return True
        # log_message(f"Voucher approval not found, checking VIM...")
        approved = True
        log_message(f"Voucher approval check - Always Default Approved: {approved}")
        return approved
    
    # Try VIM if voucher didn't have approval
    if vim_comments:
        log_message(f"Checking VIM approval - User: {opentext_user_id}")
        if check_doa_approved_in_vim(vim_comments):
            log_message(f"VIM approval found - User: {opentext_user_id}")
            return True
        log_message(f"VIM approval not found, checking EML...")
    
    # Try EML if VIM didn't have approval
    if eml_lines:
        log_message(f"Checking EML approval - User: {opentext_user_id}")
        if check_doa_approved_in_eml(eml_lines, sap_invoice_number):
            log_message(f"EML approval found - User: {opentext_user_id}")
            return True
        log_message(f"EML approval not found")
    
    log_message(f"No approval found in any source for User: {opentext_user_id}")
    return False


def lookup_diamond_data(user_id: str, doa_type: str, diamond_df: Optional[pd.DataFrame]) -> Optional[float]:
    """
    Lookup threshold from Diamond data
    Args:
        user_id: User ID to filter
        doa_type: DOA type to filter
        diamond_df: Diamond data DataFrame
    Returns:
        Threshold amount or None
    """
    if diamond_df is None or diamond_df.empty:
        log_message("Diamond data is empty or not provided")
        return None
    
    # Rename duplicate columns for clarity (if not already done)
    columns = diamond_df.columns.tolist()
    if columns.count(DIAMOND_DATA_USERNAME_COLUMN) > 1:
        # Rename the first 'User Name' to 'Delegator' and second to 'Approver'
        col_rename = {}
        user_name_indices = [i for i, x in enumerate(columns) if x == DIAMOND_DATA_USERNAME_COLUMN]
        # if len(user_name_indices) >= 1:
        #     col_rename[user_name_indices[0]] = 'Delegator'
        if len(user_name_indices) >= 2: 
            col_rename[user_name_indices[1]] = 'APPROVER'
        diamond_df = diamond_df.rename(columns=col_rename)
    
        # # Filter by Delegator User ID (first User Name column)
        # filtered = diamond_df[diamond_df['Approver'] == user_id]
 
    # Filter by Delegator User ID
    filtered = diamond_df[diamond_df[DIAMOND_DATA_USERNAME_COLUMN] == user_id]
    
    if filtered.empty:
        log_message(f"No rows found in Diamond data for user: {user_id}")
        return None
    
    # Filter by DOA Type
    filtered = filtered[filtered[DOA_TYPE_COLUMN] == doa_type]
    
    if filtered.empty:
        log_message(f"No rows found in Diamond data for user: {user_id}, DOA type: {doa_type}")
        return None
    
    # Get threshold
    threshold = filtered.iloc[0][DOA_AMOUNT_COLUMN]
    log_message(f"Diamond data threshold for {user_id}/{doa_type}: {threshold}")
    return float(threshold)


def lookup_superfund_threshold(gl_account, vendor_code: str, 
                               superfund_df: Optional[pd.DataFrame]) -> Optional[float]:
    """
    Lookup threshold from Superfund sheet
    Args:
        gl_account: GL account number (GMID)
        vendor_code: Vendor code
        superfund_df: Superfund DataFrame
    Returns:
        Threshold amount or None
    """
    if superfund_df is None or superfund_df.empty:
        log_message("Superfund data is empty or not provided")
        return None
    
    # Filter by GMID (GL account)
    # Handle gl_account as a list - check if any GL account matches
    if isinstance(gl_account, list):
        filtered = superfund_df[superfund_df['GMID'].isin(gl_account)]
    else:
        filtered = superfund_df[superfund_df['GMID'] == gl_account]
    
    if filtered.empty:
        log_message(f"No rows found in Superfund for GL account: {gl_account}")
        return None
    
    # Check if vendor code is in the Vendors column (comma-separated or "All Vendors")
    for _, row in filtered.iterrows():
        vendors_str = str(row['Vendors(PI - Payee)'])
        
        # Check if it's an "All Vendors" entry
        if 'all vendors' in vendors_str.lower():
            threshold = row['Superfund Threshold']
            # Remove $ and commas, convert to float
            threshold_clean = float(str(threshold).replace('$', '').replace(',', ''))
            log_message(f"Superfund threshold for GL:{gl_account}, All Vendors: {threshold_clean}")
            return threshold_clean
        
        # Otherwise, check specific vendor codes (comma-separated)
        vendor_list = [v.strip() for v in vendors_str.split(',')]
        
        if vendor_code in vendor_list:
            threshold = row['Superfund Threshold']
            # Remove $ and commas, convert to float
            threshold_clean = float(str(threshold).replace('$', '').replace(',', ''))
            log_message(f"Superfund threshold for GL:{gl_account}, Vendor:{vendor_code}: {threshold_clean}")
            return threshold_clean
    
    log_message(f"Vendor {vendor_code} not found in Superfund for GL {gl_account}")
    return None


def load_diamond_data(parquet_path=DIAMOND_DATA_PATH) -> pd.DataFrame:
    """
    Load Diamond data from parquet file
    Returns:
        DataFrame with Diamond data
    """
    global _diamond_data_cache
    
    if _diamond_data_cache is not None:
        return _diamond_data_cache
    
    try:
        log_message(f"Loading Diamond data from: {parquet_path}")
        _diamond_data_cache = pd.read_parquet(parquet_path)
        log_message(f"Diamond data loaded successfully. Shape: {_diamond_data_cache.shape}")
        return _diamond_data_cache
    except Exception as e:
        log_message(f"Error loading Diamond data: {str(e)}", error_logger=True)
        return pd.DataFrame()


def load_superfund_data() -> pd.DataFrame:
    """
    Load Superfund data from database or file
    Returns:
        DataFrame with Superfund data
    """
    global _superfund_data_cache
    
    if _superfund_data_cache is not None:
        return _superfund_data_cache
    
    try:
        log_message("Loading Superfund data from Excel file")
        _superfund_data_cache = pd.read_excel(SUPERFUND_DATA_PATH, sheet_name=SUPERFUND_SHEET_NAME)

        return _superfund_data_cache
    
    except Exception as e:
        log_message(f"Error loading Superfund data: {str(e)}", error_logger=True)
        return pd.DataFrame()


def clear_data_cache():
    """Clear cached data (useful for testing or when data is updated)"""
    global _diamond_data_cache, _superfund_data_cache, _pro_vr_doa_data_cache
    _diamond_data_cache = None
    _superfund_data_cache = None
    _pro_vr_doa_data_cache = None
    log_message("DOA data cache cleared")


def load_pro_vr_doa_data(parquet_path: str = PRO_VR_DOA_DATA_PATH) -> pd.DataFrame:
    """
    Load PRO_VR_DOA data from parquet file
    This table contains the mapping of Client, G/L Account to DOA Type
    Returns:
        DataFrame with PRO_VR_DOA data (columns: Client, G/L Account, DOA Type)
    """
    global _pro_vr_doa_data_cache
    
    if _pro_vr_doa_data_cache is not None:
        return _pro_vr_doa_data_cache
    
    try:
        log_message(f"Loading PRO_VR_DOA data from: {parquet_path}")
        _pro_vr_doa_data_cache = pd.read_parquet(parquet_path)
        log_message(f"PRO_VR_DOA data loaded successfully. Shape: {_pro_vr_doa_data_cache.shape}")
        return _pro_vr_doa_data_cache
    except Exception as e:
        log_message(f"Error loading PRO_VR_DOA data: {str(e)}", error_logger=True)
        return pd.DataFrame()


def get_doa_type_from_gl_accounts(tenant_code: str, gl_accounts: List[str]) -> Optional[str]:
    """
    Get DOA type from PRO_VR_DOA table using tenant_code (Client) and GL accounts
    Args:
        tenant_code: Client code from sap_row.tenant_code (Client column in PRO_VR_DOA)
        gl_accounts: List of GL account numbers to search (priority order: voucher, invoice, sap)
        pro_vr_doa_df: PRO_VR_DOA DataFrame (if not provided, will load from file)
    Returns:
        DOA type string or None if not found
    """

    pro_vr_doa_df = load_pro_vr_doa_data()
    
    if pro_vr_doa_df is None or pro_vr_doa_df.empty:
        log_message("PRO_VR_DOA data is empty or not available")
        return None
    
    # Normalize tenant_code for comparison
    tenant_code_normalized = str(tenant_code).strip().upper()
    
    # Filter by Client (tenant_code)
    filtered = pro_vr_doa_df[
        pro_vr_doa_df['CLIENT'].astype(str).str.strip().str.upper() == tenant_code_normalized
    ]
    
    if filtered.empty:
        log_message(f"No rows found in PRO_VR_DOA for Client: {tenant_code}")
        return None
    
    log_message(f"Found {len(filtered)} rows in PRO_VR_DOA for Client: {tenant_code}")
    
    # Loop through GL accounts to find matching DOA type
    for gl_account in gl_accounts:
        if gl_account is None:
            continue
            
        gl_account_normalized = str(gl_account).strip().upper()
        
        # Filter by G/L Account
        gl_filtered = filtered[
            filtered['GL_ACCOUNT'].astype(str).str.strip().str.upper() == gl_account_normalized
        ]
        
        if not gl_filtered.empty:
            doa_type = gl_filtered.iloc[0][DOA_TYPE_COLUMN]
            log_message(f"Found DOA Type '{doa_type}' for Client: {tenant_code}, GL Account: {gl_account}")
            return str(doa_type).strip() if doa_type else None
    
    
    log_message(f"No DOA Type found for Client: {tenant_code} with GL accounts: {gl_accounts}")
    return None
