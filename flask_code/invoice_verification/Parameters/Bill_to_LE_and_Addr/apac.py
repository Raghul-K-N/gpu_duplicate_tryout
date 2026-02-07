# Parameters/Bill to LE and Addr/apac.py
"""APAC region-specific bill to legal entity validation logic"""
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Bill_to_LE_and_Addr.utils import check_apac_vip_veson_emails
from invoice_verification.Parameters.utils import build_validation_result
from typing import Optional, List

def validate_apac_legal_entity(extracted_name: Optional[str], extracted_address: Optional[str],
                                sap_row: SAPRow, extracted_remit_to_name: Optional[str],
                                extracted_remit_to_address: Optional[str], eml_lines: List[str]) -> dict:
    """
    APAC region legal entity validation
    
    Flow:
    1. Check APAC-specific exceptions
    2. If no exceptions apply, call global logic
    
    APAC Exceptions:
    - Rule 1: Doc type KE + VIP VESON email (RPVIP01@dow.com) - Combined review
    """
    log_message(f"APAC Legal Entity, Doc type: {sap_row.doc_type}")
    
    # Rule 1: Doc type KE + VIP VESON email check (APAC specific)
    if sap_row.doc_type == 'KE' and check_apac_vip_veson_emails(eml_lines):
        log_message("APAC: KE doc type with VIP VESON email (RPVIP01@dow.com)")
        return build_validation_result(
            extracted_value={'name': extracted_name, 'address': extracted_address},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={}
        )
    
    # No APAC-specific exceptions - call global logic
    log_message("APAC: No region-specific exceptions, calling global logic")
    from .global_logic import validate_global_legal_entity
    return validate_global_legal_entity(
        extracted_name=extracted_name,
        extracted_address=extracted_address,
        sap_row=sap_row,
        extracted_remit_to_name=extracted_remit_to_name,
        extracted_remit_to_address=extracted_remit_to_address
    )