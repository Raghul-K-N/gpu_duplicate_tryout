# Parameters/DOA/emeai.py
"""
EMEAI region-specific DOA validation logic
"""
from typing import List
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.constant_field_names import DOA

def validate_emeai_doa(sap_row: SAPRow, voucher_text: str, gl_accounts_list: List[str]) -> dict:
    """
    EMEAI-specific DOA validation
    First checks EMEAI exceptions, then calls global logic if no exception matches
    Args:
        sap_row: SAPRow object with all SAP data
        voucher_text: Voucher OCR text
        gl_accounts_list: Prioritized list of GL accounts (Voucher -> Invoice -> SAP)
    Returns:
        Dict with standardized output format
    """
    log_message(f"Validating EMEAI DOA , Company: {sap_row.company_code}")
    
    # Rule 3: India special vendor
    if sap_row.company_code == '921' and sap_row.vendor_code == '1590807':
        log_message("EMEAI: India special vendor - manual review")
        return build_validation_result(
            extracted_value={DOA: None},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Manual',
            supporting_details={"Summary": "India Vendor-1590807 requires manual review for DOA"}
        )
    
    from .global_logic import validate_global_doa
    
    return validate_global_doa(
        sap_row=sap_row,
        voucher_text=voucher_text,
        gl_accounts_list=gl_accounts_list
    )
