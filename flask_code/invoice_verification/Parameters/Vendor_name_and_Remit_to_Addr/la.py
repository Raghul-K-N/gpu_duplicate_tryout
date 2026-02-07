# Parameters/Vendor name & Remmit to Addr/la.py
"""
LATAM region-specific vendor name validation logic
"""
from invoice_verification.logger.logger import log_message
from .utils import standard_vendor_validation
from invoice_verification.Parameters.utils import build_validation_result
from typing import Optional

def validate_latam_vendor_info(extracted_vendor_name: Optional[str], extracted_vendor_address: Optional[str],
                                sap_vendor_name: str, sap_vendor_address: str,
                               vendor_code: str, country: str) -> dict:
    """
    LATAM region vendor validation with specific vendor exceptions
    Rules:
    1. Vendor 273768 with company 0184/4027: Skip validation for 'Tesoreria de la Federacion'
    2. Other vendors: Standard name validation only
    Args:
        extracted_vendor_name: Vendor name from invoice OCR
        extracted_vendor_address: Remit to address from invoice OCR
        sap_vendor_name: Vendor name from SAP
        sap_vendor_address: Vendor address from SAP
        vendor_code: Vendor code
        country: Country code
    
    Returns:
        Dict with standardized output format
    """
    log_message(f"Validating LATAM vendor - Vendor: {vendor_code}, Country: {country}")
    
    # Rule 1: Special vendor exception
    if vendor_code == '273768' and country in ['MEXICO', 'MX']:
        if 'TESORERIA' in str(sap_vendor_name).upper():
            log_message("LATAM: Tesoreria de la Federacion - skipping validation")
            return build_validation_result(
                extracted_value={'name': extracted_vendor_name, 'address': extracted_vendor_address},
                is_anomaly=False,
                edit_operation=False,
                highlight=False,
                method='Automated',
                supporting_details={}
            )
    
    # Rule 2: Standard global validation
    from .global_logic import validate_global_vendor_info
    return validate_global_vendor_info(
        extracted_vendor_name=extracted_vendor_name,
        extracted_vendor_address=extracted_vendor_address,
        sap_vendor_name=sap_vendor_name,
        sap_vendor_address=sap_vendor_address
    )