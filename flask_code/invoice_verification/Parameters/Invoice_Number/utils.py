# Parameters/Invoice Number/utils.py
"""
Utility functions for invoice number validation
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_NUMBER

custom_house_agent_vendors_india = ["1786527","2457422","2473990","2457427","2483474",
                                    "2483246","2483264","2522700","2435793","2566027",
                                    "2488166","2516551","2517655","2563076","2242305"]

def normalize_invoice_number(invoice_number, alphanumeric_only:bool =False) -> str:
    """
    Normalize invoice number
    Args:
        invoice_number: Raw invoice number
        alphanumeric_only: If True, keep only alphanumeric characters
    Returns:
        Normalized invoice number
    """
    if not invoice_number:
        return ""
    
    invoice_str = str(invoice_number)    # No lstrip("0")#.rstrip("0")
    
    if alphanumeric_only:
        # Strip spaces, dashes, and special characters - keep only alphanumeric
        return ''.join(c for c in invoice_str if c.isalnum())
    else:
        # Just strip leading/trailing whitespace
        return invoice_str.strip()

def standard_invoice_number_validation(extracted_value: str|None, 
                                       sap_value: str
                                       ) -> dict:
    """
    Standardized invoice number validation logic
    Args:
        extracted_value: Invoice number from OCR extraction
        sap_value: Invoice number from SAP data
    Returns:
        Dict with standardized output format
    """
    log_message("Using standard invoice number validation")
    
    # Simple normalization - alphanumeric only
    normalized_extracted = normalize_invoice_number(extracted_value, alphanumeric_only=False)
    normalized_sap = normalize_invoice_number(sap_value, alphanumeric_only=False)
    
    # Validation and return
    if not normalized_extracted:
        log_message("No extracted invoice number")
        return build_validation_result(
            extracted_value={INVOICE_NUMBER: extracted_value},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"No extracted invoice number available."}
        )
    
    anomaly_flag = normalized_extracted.upper() != normalized_sap.upper()
    log_message("Invoice number matches with SAP value, not an anomaly")
    return build_validation_result(
        extracted_value={INVOICE_NUMBER: extracted_value},
        is_anomaly=anomaly_flag,
        edit_operation=False,
        highlight=False,
        method='Automated',
        supporting_details={"Summary":"Invoice number matches with SAP value."}
    )