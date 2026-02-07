from typing import Dict
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.logger.logger import log_message
from invoice_verification.constant_field_names import VAT_TAX_AMOUNT

def validate_naa_vat_tax_code(sap_tax_amount: str,
                              extracted_tax_amount: str|None
                              ) -> Dict:
    """NAA-specific Tax amount extraction logic"""
    log_message(f"Inside NAA based Exceptions for Parameter Tax Amount")
    
    extracted_tax_amount_processed = str(round(float(extracted_tax_amount))).strip() if extracted_tax_amount else None

    sap_tax_amount_processed = str(round(float(sap_tax_amount))).strip() if sap_tax_amount else None

    log_message(f"SAP Tax Amount: {sap_tax_amount_processed} and Extracted Tax Amount: {extracted_tax_amount_processed}")

    if extracted_tax_amount_processed is None:
        log_message(f"No Extracted Tax Amount is available")
        return build_validation_result(
                    extracted_value={VAT_TAX_AMOUNT:extracted_tax_amount_processed},
                    is_anomaly=True,
                    method="Combined",
                    highlight=False,
                    edit_operation=False,
                    supporting_details={"Summary":"Tax Amount is not Available"}
        )

    anomaly_flag = extracted_tax_amount_processed != sap_tax_amount_processed
    log_message(f"After comparing extracted Tax amount and SAP Tax Amount : {anomaly_flag}")

    return build_validation_result(
                    extracted_value={VAT_TAX_AMOUNT:extracted_tax_amount_processed},
                    is_anomaly=anomaly_flag,
                    method="Automated",
                    highlight=False,
                    edit_operation=False,
                    supporting_details={"Summary":"Standard Validation"}
        )