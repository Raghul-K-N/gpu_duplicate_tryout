# Parameters/DOA/apac.py
# ============================================================================
"""
APAC region DOA validation logic
"""
from typing import List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow

def validate_apac_doa(sap_row: SAPRow, voucher_text: str, gl_accounts_list: List[str]) -> dict:
    """
    APAC-specific DOA validation
    
    First checks APAC exceptions, then calls global logic if no exception matches
    
    Args:
        sap_row: SAPRow object with all SAP data
        voucher_text: Voucher OCR text
        gl_accounts_list: Prioritized list of GL accounts (Voucher -> Invoice -> SAP)
    """
    log_message(f"DOA validation for region: {sap_row.region}")

    from .global_logic import validate_global_doa
    
    return validate_global_doa(
        sap_row=sap_row,
        voucher_text=voucher_text,
        gl_accounts_list=gl_accounts_list
    )