# Parameters/DOA/la.py
# ============================================================================
"""
Latin America (LATAM) DOA validation logic
"""
from typing import List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow

def validate_latam_doa(sap_row: SAPRow, voucher_text: str, gl_accounts_list: List[str]) -> dict:
    """
    LATAM-specific DOA validation
    
    First checks LATAM exceptions, then calls global logic if no exception matches
    
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