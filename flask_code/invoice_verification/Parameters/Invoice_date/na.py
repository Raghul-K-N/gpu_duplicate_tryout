from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_DATE
from invoice_verification.logger.logger import log_message
from .utils import pandas_date_parser
from typing import Dict

def   validate_naa_invoice_date(sap_invoice_date: str,
                                invoice_extracted_invoice_date: str|None,
                                voucher_extracted_invoice_date: str|None
                                ) -> Dict:
    """
    Validate the Invoice date based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    # parse both extracted and SAP dates using pandas for robustness
    extracted_date = None
    invoice_extracted_processed_date = pandas_date_parser(invoice_extracted_invoice_date) if invoice_extracted_invoice_date else None
    voucher_extracted_processed_date = pandas_date_parser(voucher_extracted_invoice_date) if voucher_extracted_invoice_date else None
    log_message(f"Processed Invoice Extracted Date: {invoice_extracted_processed_date}, Processed Voucher Extracted Date: {voucher_extracted_processed_date} for Parameter Invoice Date")
    if invoice_extracted_processed_date:
        extracted_date = invoice_extracted_processed_date
    elif voucher_extracted_processed_date:
        extracted_date = voucher_extracted_processed_date

    if extracted_date is None:
        log_message(f"Extracted Invoice date is None, So Anomaly-True")
        return build_validation_result(extracted_value={INVOICE_DATE: None},
                                    is_anomaly=True,
                                    edit_operation=False,
                                    highlight=False,
                                    method="Automated",
                                    supporting_details={"Summary":"Standard Validation."})

    processed_sap = pandas_date_parser(sap_invoice_date)
    sap_date = processed_sap if processed_sap else None
    log_message(f"Invoice date: [Extracted Value: {extracted_date}, SAP Value: {sap_date} ] for Parameter Invoice Date")

    # Validate
    anomaly_flag = extracted_date != sap_date if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Date")

    extracted_invoice_date = {INVOICE_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_invoice_date,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Standard Validation."})