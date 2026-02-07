# Parameters/Bill to LE and Addr/emeai.py
"""EMEAI region-specific bill to legal entity validation logic"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Bill_to_LE_and_Addr.utils import (normalize_legal_entity_name,
                    partial_match_legal_entity, check_emeai_vip_veson_emails)
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.Parameters.constants import FRANCE_COMPANY_CODES, INDIA_COMPANY_CODES
from invoice_verification.constant_field_names import LEGAL_ENTITY_NAME, LEGAL_ENTITY_ADDRESS
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional, List

def validate_emeai_legal_entity(extracted_name: Optional[str], extracted_address: Optional[str],
                                sap_row: SAPRow, extracted_remit_to_name: Optional[str],
                                extracted_remit_to_address: Optional[str], eml_lines: List[str]) -> dict:
    """
    EMEAI region legal entity validation
    
    Flow:
    1. Check EMEAI-specific exceptions
    2. If no exceptions apply, call global logic
    
    EMEAI Exceptions:
    - Rule 1: Doc type KE + VIP VESON email (FTNAPLO@dow.com) - Combined review
    - Rule 2: NOT France/India - Name only validation (return immediately)
    - Rule 3: France/India - Fall through to global logic for full validation
    """
    log_message(f"EMEAI Legal Entity, Doc type: {sap_row.doc_type}")
    
    sap_legal_entity_name = sap_row.legal_entity_name
    sap_legal_entity_address = sap_row.legal_entity_address
    company_code = sap_row.company_code
    

    # Rule 1: Doc type KE + VIP VESON email check (EMEAI specific)
    if sap_row.doc_type == 'KE' and check_emeai_vip_veson_emails(eml_lines):
        log_message("EMEAI: KE doc type with VIP VESON email (FTNAPLO@dow.com)")
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: extracted_name, LEGAL_ENTITY_ADDRESS: extracted_address},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"DOCTYPE-KE with VIP VESON email requires Combined review."}
        )
    #NOTE: Should the empty extracted check come at the start
    # Check for empty extracted name
    if is_empty_value(extracted_name) or is_empty_value(extracted_address):
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"Extracted legal entity name or address is missing."}
        )
    
    # Check if France or India
    is_france = (company_code in FRANCE_COMPANY_CODES) or (sap_legal_entity_address and 'FRANCE' in str(sap_legal_entity_address).upper())
    is_india = (company_code in INDIA_COMPANY_CODES)
    
    # Rule 2: NOT France/India - Name only validation and return
    if not (is_france or is_india):
        log_message("EMEAI: Non-France/Non-India country - name only validation")
        norm_extracted_name = normalize_legal_entity_name(extracted_name)
        norm_sap_name = normalize_legal_entity_name(sap_legal_entity_name)
        
        name_match = partial_match_legal_entity(norm_extracted_name, norm_sap_name)
        
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: extracted_name, LEGAL_ENTITY_ADDRESS: extracted_address},
            is_anomaly=not name_match,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Name only validation for Non-France/Non-India country."}
        )
    
    # Rule 3: France/India - Call global logic for full name and address validation
    log_message("EMEAI: France/India - calling global logic for full validation")
    from .global_logic import validate_global_legal_entity
    return validate_global_legal_entity(
        extracted_name=extracted_name,
        extracted_address=extracted_address,
        sap_row=sap_row,
        extracted_remit_to_name=extracted_remit_to_name,
        extracted_remit_to_address=extracted_remit_to_address
    )