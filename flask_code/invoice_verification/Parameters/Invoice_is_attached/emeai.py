from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow

def validate_emeai_invoice_is_attached(sap_row: SAPRow
                                        ) -> Dict:
    """
    Region EMEAI based Exceptions
    """

    doc_type = str(sap_row.doc_type).strip().upper()

    if doc_type == "KH":
        # setattr(sap_row, "payment_certificate_flag", True)
        log_message(f"KH Doc type based exception handling started")
        anomaly_flag = False if sap_row.payment_certificate_flag else True
        log_message(f"Payment Certificate Flag is {sap_row.payment_certificate_flag} - SO anomaly is {anomaly_flag} for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={},
                                        is_anomaly=anomaly_flag,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"PO Number": sap_row.po_number,
                                                            "Summary": "KH Doc type - Payment Certificate based exception handling."})
    
    # No EMEAI Exceptions were met
    log_message("No EMEAI Exceptions were met for Parameter Invoice is Attached")
    from .global_logic import validate_global_invoice_is_attached
    return validate_global_invoice_is_attached(sap_row = sap_row)
