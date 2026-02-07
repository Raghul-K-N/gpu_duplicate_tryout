from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_AMOUNT
from typing import Dict

def validate_naa_invoice_amount(sap_invoice_amount: str,
                                invoice_extracted_invoice_amount: str|None,
								voucher_extracted_invoice_amount: str|None
                                ) -> Dict:
    """NAA-specific invoice amount extraction logic"""
    log_message(f"Inside NAA based Exceptions for Parameter Invoice Amount")
    
    extracted_invoice_amount_processed = None
    if invoice_extracted_invoice_amount is not None:
        extracted_invoice_amount_processed = str(round(float(invoice_extracted_invoice_amount))).strip()
    elif voucher_extracted_invoice_amount is not None:
        extracted_invoice_amount_processed = str(round(float(voucher_extracted_invoice_amount))).strip()
    sap_invoice_amount_processed = str(round(float(sap_invoice_amount))).strip() if sap_invoice_amount else None

    log_message(f"SAP Invoice Amount: {sap_invoice_amount_processed} and Extracted Invoice Amount: {extracted_invoice_amount_processed}")

    if extracted_invoice_amount_processed is None:
        log_message(f"No Extracted Invoice Amount is available")
        return build_validation_result(
                    extracted_value={INVOICE_AMOUNT:extracted_invoice_amount_processed},
                    is_anomaly=True,
                    method="Combined",
                    highlight=False,
                    edit_operation=False,
                    supporting_details={"Summary":"Invoice Amount is not Available"}
        )

    anomaly_flag = extracted_invoice_amount_processed != sap_invoice_amount_processed
    log_message(f"After comparing extracted invoice amount and SAP Invoice Amount : {anomaly_flag}")

    return build_validation_result(
                    extracted_value={INVOICE_AMOUNT:extracted_invoice_amount_processed},
                    is_anomaly=anomaly_flag,
                    method="Automated",
                    highlight=False,
                    edit_operation=False,
                    supporting_details={"Summary":"Standard Validation"}
        )