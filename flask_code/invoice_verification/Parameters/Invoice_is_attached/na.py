from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_IS_ATTACHED
from invoice_verification.Schemas.sap_row import SAPRow

def validate_naa_invoice_is_attached(sap_row: SAPRow
                                    ) -> Dict:
    """
    Validate NAA region based Invoice is attached exceptions
    """

    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()

    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message(f"Doc type KE based Exception handling started for Invoice is Attached Parameter")
        if sap_row.dummy_generic_invoice_flag and sap_row.eml_file_flag and sap_row.excel_file_flag:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=True,
                                            method="Combined",
                                            supporting_details={"Summary": "DOCTYPE-KE with Dummy Generic Invoice, Email and Excel file."})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=True,
                                            method="Combined",
                                            supporting_details={"Summary": "DOCTYPE-KE - Not all files present(Dummy/Invoice, Email, Excel)."})
        
    if doc_type in ["KC","KA"]:
        log_message(f"for {doc_type} Doc type Exception we do no process or no Documents needed for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary": f"DOCTYPE-{doc_type} requires no documents."})
    
    if doc_type == "KH":
        # setattr(sap_row, "payment_certificate_flag", True)
        log_message(f"for KH doc type we will check the PO number for 4xx or 5xx series for validation for Invoice is Attached Parameter")
        anomaly_flag = False if sap_row.payment_certificate_flag else True
        log_message(f"Payment Certificate Flag is {sap_row.payment_certificate_flag} - SO anomaly is {anomaly_flag} for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={},
                                        is_anomaly=anomaly_flag,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"PO Number": sap_row.po_number,
                                                            "Summary": "KH Doc type based exception handling."})
    
    if doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"]:
        log_message(f"for {doc_type} Doc type and DP Doc type {dp_doc_type} Exception Handling Started for Invoice is Attached Parameter")
        if sap_row.dummy_generic_invoice_flag or sap_row.invoice_copy_flag:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"DOCTYPE-KS Dummy/Invoice Mandate"})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"DOCTYPE-KS Dummy/Invoice Mandate"})
    # No NAA Exceptions were met
    log_message("No NAA Exceptions were met for Parameter Invoice is Attached")
    from .global_logic import validate_global_invoice_is_attached
    return validate_global_invoice_is_attached(sap_row = sap_row)