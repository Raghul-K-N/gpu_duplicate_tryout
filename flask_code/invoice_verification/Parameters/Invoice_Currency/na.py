from invoice_verification.constant_field_names import INVOICE_CURRENCY
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.logger.logger import log_message
from typing import Dict, List

def   validate_naa_invoice_currency(
            sap_po_currency: List, 
            invoice_extracted_invoice_currency: List,
            voucher_extracted_invoice_currency: List
            ) -> Dict:
    """
    Validate the Invoice currency based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message("Global invoice currency validation for Parameter Invoice Currency")
    all_extracted_values = []
    
    if invoice_extracted_invoice_currency:
        all_extracted_values.extend([str(val).strip().upper() for val in invoice_extracted_invoice_currency if val])
    
    if voucher_extracted_invoice_currency:
        all_extracted_values.extend([str(val).strip().upper() for val in voucher_extracted_invoice_currency if val])
    
    sap_po_currency = [po_currency.strip().upper() for po_currency in sap_po_currency if po_currency]
    
    # Check if all extracted values match SAP value
    anomaly_flag = None
    if all_extracted_values:
        # Anomaly is True if ANY extracted value doesn't match SAP
        for val in all_extracted_values:
            if val in sap_po_currency:
                anomaly_flag = False
                break
            else:
                anomaly_flag = True
                break
    
    log_message(f"Anomaly Flag for Invoice Currency Validation: {anomaly_flag} for Parameter Invoice Currency")
    
    log_message(f"Final Extracted Value for Invoice Currency: {all_extracted_values} for Parameter Invoice Currency")
    
    return build_validation_result(extracted_value={INVOICE_CURRENCY: all_extracted_values},
                                   is_anomaly=anomaly_flag,
                                   edit_operation=False,
                                   highlight=False,
                                   method="Automated",
                                   supporting_details={"Summary":"Standard Validation"})