# Parameters/UDC/na.py
# ============================================================================
"""
North America (NAA) UDC validation logic
"""
from typing import Optional, List
from invoice_verification.Parameters.utils import kc_ka_doctype_common_handling
from invoice_verification.constant_field_names import UDC
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow

def validate_naa_udc(sap_row: SAPRow, voucher_text: str,
                     gl_accounts_list: Optional[List[str]] = None) -> dict:
    """
    NAA-specific UDC validation
    
    First checks NAA exceptions, then calls global logic if no exception matches
    
    Args:
        sap_row: SAPRow object with all SAP data
        voucher_text: Voucher OCR text
        gl_accounts_list: Combined list of GL accounts (priority: Voucher OCR -> Invoice OCR -> SAP)
    """
    log_message(f"UDC validation for region: {sap_row.region}")

    # Invoice is Attached Condition 1: KC document type - Skip validation
    # if sap_row.doc_type in ["KC", "KA"]:
    #     log_message("NAA: KC/KA document type - skip UDC validation")
    #     return kc_ka_doctype_common_handling(parameter=UDC)
        
    from .global_logic import validate_global_udc
    
    return validate_global_udc(
        sap_row=sap_row,
        voucher_text=voucher_text,
        gl_accounts_list=gl_accounts_list
    )