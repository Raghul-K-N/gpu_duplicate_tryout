# Parameters/UDC/la.py
# ============================================================================
"""
Latin America (LATAM) UDC validation logic
"""
from typing import Optional, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow

def validate_latam_udc(sap_row: SAPRow, voucher_text: str,
                       gl_accounts_list: Optional[List[str]] = None) -> dict:
    """
    LATAM-specific UDC validation
    
    First checks LATAM exceptions, then calls global logic if no exception matches
    
    Args:
        sap_row: SAPRow object with all SAP data
        voucher_text: Voucher OCR text
        gl_accounts_list: Combined list of GL accounts (priority: Voucher OCR -> Invoice OCR -> SAP)
    """
    log_message(f"UDC validation for region: {sap_row.region}")

    from .global_logic import validate_global_udc
    
    return validate_global_udc(
        sap_row=sap_row,
        voucher_text=voucher_text,
        gl_accounts_list=gl_accounts_list
    )