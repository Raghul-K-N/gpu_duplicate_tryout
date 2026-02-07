# Parameters/Invoice Number/na.py
"""
NAA region-specific invoice number validation logic
"""
from invoice_verification.logger.logger import log_message
from .utils import standard_invoice_number_validation
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import INVOICE_NUMBER
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional, Dict

def validate_naa_invoice_number(
        sap_row: SAPRow,
        extracted_value: Optional[str]
        ) -> Dict:
    """
    NAA region invoice number validation with region-specific rules
    
    Rules:
    1. If invoice number > 16 digits, check only last 16 digits (right to left)
    2. Allow spaces and special characters only for Ariba invoices
    3. For non-Ariba invoices, strip all spaces, dashes, and special characters (alphanumeric only)
    
    Args:
        extracted_value: Invoice number from OCR extraction
        sap_row: SAPRow object with all SAP data
    
    Returns:
        Dict with standardized output format
    """
    log_message("Validating NAA invoice number")

    special_charecters_not_allowed = ["*","@","#"," "]

    if is_empty_value(extracted_value):
        log_message("Extracted invoice number is empty or None")
        return build_validation_result(
            extracted_value={INVOICE_NUMBER:extracted_value},
            is_anomaly=None,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"Summary": "Invoice Number missing/Documents not attached"}
        )
    
    if extracted_value is not None and any(True if char in extracted_value else False for char in special_charecters_not_allowed):
        log_message("Special characters or spaces found in extracted invoice number")
        return build_validation_result(
            extracted_value={INVOICE_NUMBER:extracted_value},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"Summary": "Special characters or spaces not allowed in NAA invoice numbers"}
        )
    
    if extracted_value is not None and len(str(extracted_value)) > 16:
        log_message("Extracted invoice number exceeds 16 characters, applying rightmost 16 characters rule")
        extracted_value_processed = str(extracted_value)[-16:]
        normalized_invoice_number = str(extracted_value_processed).strip().lower()
        sap_invoice_number = str(sap_row.invoice_number).strip().lower()
        anomaly_flag = normalized_invoice_number != sap_invoice_number
        return build_validation_result(
            extracted_value={INVOICE_NUMBER:extracted_value_processed},
            is_anomaly=anomaly_flag,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"Summary": "Used rightmost 16 characters for Vaidation"}
        )
    
    # Standard Validation
    return standard_invoice_number_validation(
        extracted_value=extracted_value,
        sap_value=sap_row.invoice_number
    )