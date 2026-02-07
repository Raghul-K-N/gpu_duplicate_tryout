# Parameters/Vendor name & Remmit to Addr/emeai.py
"""
EMEAI region-specific vendor name and remit to address validation logic
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Vendor_name_and_Remit_to_Addr.utils import (normalize_vendor_name, normalize_address, fuzzy_match_vendor)
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.Parameters.constants import INDIA_COMPANY_CODES, FRANCE_COMPANY_CODES
from invoice_verification.constant_field_names import VENDOR_NAME, VENDOR_ADDRESS
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional

def validate_emeai_vendor_info(extracted_vendor_name: Optional[str], extracted_vendor_address: Optional[str],
                             sap_row: SAPRow) -> dict:
    """
    EMEAI region vendor validation with country-specific address rules
    Rules:
    1. If coutry is India or legal entity is in France: Validate only vendor name
    Args:
        extracted_vendor_name: Vendor name from invoice OCR
        extracted_vendor_address: Remit to address from invoice OCR
        sap_row: SAPRow object with all SAP data
    Returns:
        Dict with standardized output format
    """
    company_code = sap_row.company_code
    sap_vendor_name = sap_row.vendor_name
    sap_vendor_address = sap_row.vendor_address
    legal_entity_address = sap_row.legal_entity_address
    
    log_message(f"Validating EMEAI vendor info - Company code: {company_code}")
    
    is_india = company_code in INDIA_COMPANY_CODES
    is_france_entity = legal_entity_address and 'FRANCE' in legal_entity_address.upper()

    if not (is_india or is_france_entity):
        # Check for empty/None vendor name first
        if is_empty_value(extracted_vendor_name):
            log_message("EMEAI: Extracted vendor name is Empty or None")
            return build_validation_result(
                extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=True,
                method='Combined',
                supporting_details={"Summary":"Extracted vendor name is missing."}
            )
        # Rule 1: Other countries validate only vendor name
        log_message("EMEAI: Other country/entity than India/France - validating only vendor name")
        # Normalize vendor name
        norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
        norm_sap_name = normalize_vendor_name(sap_vendor_name)

        # Always validate vendor name
        name_match = fuzzy_match_vendor(str1=norm_extracted_name, str2=norm_sap_name)
        return build_validation_result(
            extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
            is_anomaly=not name_match,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Vendor name validation for other countries/entities than India/France."}
        )
    
    from .global_logic import validate_global_vendor_info
    return validate_global_vendor_info(
        extracted_vendor_name=extracted_vendor_name,
        extracted_vendor_address=extracted_vendor_address,
        sap_row=sap_row
    )