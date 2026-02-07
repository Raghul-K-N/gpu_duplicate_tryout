# Parameters/Vendor name & Remmit to Addr/apac.py
"""
APAC region-specific vendor name and remit to address validation logic
"""
from logger.logger import log_message
from .utils import (normalize_vendor_name, normalize_address, fuzzy_match_vendor, 
                   exact_match_vendor, detect_language)
from Parameters.utils import build_validation_result
from typing import Optional

def validate_apac_vendor_info(extracted_vendor_name: Optional[str], extracted_vendor_address: Optional[str],
                               sap_vendor_name: str, sap_vendor_address: str,
                               payment_method: str, vendor_code: str, country: str,
                               bank_account_number: Optional[str], bank_account_holder_name: Optional[str],
                               vendor_tax_id: str) -> dict:
    """
    APAC region vendor validation with language and country-specific rules
    Rules:
    1-3. Always validate name, skip address if bank details present
    4. English invoices: exact match for vendor name
    5. Local language: validate if SAP has local language, else check Tax ID (Taiwan/Korea)
    6. Chinese invoices: match bank account holder name with SAP vendor name
    7. Vendor code 1921833: Manual validation
    8-9. Country-specific address validation for check payments
    Args:
        extracted_vendor_name: Vendor name from invoice OCR
        extracted_vendor_address: Remit to address from invoice OCR
        sap_vendor_name: Vendor name from SAP
        sap_vendor_address: Vendor address from SAP
        payment_method: Payment method code
        vendor_code: Vendor code
        country: Country
        bank_account_number: Bank account number from OCR
        bank_account_holder_name: Bank holder name from OCR
        vendor_tax_id: Vendor tax ID from SAP
    
    Returns:
        Dict with standardized output format
    """
    log_message(f"Validating APAC vendor info - Country: {country}, Payment: {payment_method}")
    
    # Rule 7: Special vendor - manual validation
    if vendor_code == '1921833':
        log_message("APAC: Vendor 1921833 - manual validation")
        return build_validation_result(
            extracted_value={'name': extracted_vendor_name, 'address': extracted_vendor_address},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Manual',
            supporting_details={'special_vendor': '1921833'}
        )
    
    # Detect language
    language = detect_language(extracted_vendor_name)
    
    # Determine name validation based on language
    if language == 'english':
        # Rule 4: Exact match for English
        norm_extracted_name = normalize_vendor_name(extracted_vendor_name, strict=True)
        norm_sap_name = normalize_vendor_name(sap_vendor_name, strict=True)
        name_match = exact_match_vendor(norm_extracted_name, norm_sap_name)
        log_message("APAC: English invoice - exact match")
    elif language == 'chinese':
        # Rule 6: Use bank holder name
        if bank_account_holder_name:
            norm_extracted_name = normalize_vendor_name(bank_account_holder_name)
            norm_sap_name = normalize_vendor_name(sap_vendor_name)
            name_match = fuzzy_match_vendor(norm_extracted_name, norm_sap_name, threshold=0.8)
            log_message(f"APAC: Chinese invoice - using bank holder name")
        else:
            norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
            norm_sap_name = normalize_vendor_name(sap_vendor_name)
            name_match = fuzzy_match_vendor(norm_extracted_name, norm_sap_name, threshold=0.8)
    elif language == 'local':
        # Rule 5: Check Tax ID for Taiwan/Korea
        if country in ['TW', 'KR', 'TAIWAN', 'KOREA']:
            if vendor_tax_id:
                log_message(f"APAC: Local language - Tax ID present: {vendor_tax_id}")
                name_match = True
                norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
            else:
                log_message("APAC: Local language - Tax ID missing")
                return build_validation_result(
                    extracted_value={'name': extracted_vendor_name, 'address': extracted_vendor_address},
                    is_anomaly=True,
                    edit_operation=True,
                    highlight=True,
                    method='Automated',
                    supporting_details={'tax_id_missing': True}
                )
        else:
            norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
            norm_sap_name = normalize_vendor_name(sap_vendor_name)
            name_match = fuzzy_match_vendor(norm_extracted_name, norm_sap_name, threshold=0.75)
    else:
        norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
        norm_sap_name = normalize_vendor_name(sap_vendor_name)
        name_match = fuzzy_match_vendor(norm_extracted_name, norm_sap_name)
    
    # Determine address validation
    validate_address = False
    
    if bank_account_number:
        # Rule 2: Skip address if bank details present
        log_message("APAC: Bank details present - skipping address")
        validate_address = False
    elif payment_method == 'C':
        # Rules 3, 8: Check payment
        if country in ['TW', 'TAIWAN']:
            # Rule 8: Taiwan check payment
            validate_address = True
        elif country in ['PH', 'MY', 'PHILIPPINES', 'MALAYSIA']:
            # Rule 9: Philippines/Malaysia C
            validate_address = True
    elif payment_method == 'Z':
        # Rule 9: Other APAC Z
        validate_address = True
    
    # Validate address if required
    address_match = True
    if validate_address:
        norm_extracted_addr = normalize_address(extracted_vendor_address)
        norm_sap_addr = normalize_address(sap_vendor_address)
        address_match = fuzzy_match_vendor(norm_extracted_addr, norm_sap_addr, threshold=0.7)
    
    # Return result
    if not norm_extracted_name:
        return build_validation_result(
            extracted_value={'name': None, 'address': extracted_vendor_address},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Automated',
            supporting_details={}
        )
    
    if name_match and address_match:
        return build_validation_result(
            extracted_value={'name': norm_extracted_name, 'address': extracted_vendor_address},
            is_anomaly=False,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={}
        )
    else:
        return build_validation_result(
            extracted_value={'name': norm_extracted_name, 'address': extracted_vendor_address},
            is_anomaly=True,
            edit_operation=True,
            highlight=True,
            method='Automated',
            supporting_details={}
        )