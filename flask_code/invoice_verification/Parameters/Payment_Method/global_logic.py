from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.constant_field_names import PAYMENT_METHOD

def validate_global_payment_method(sap_row: SAPRow,
                                    invoice_extracted_payment_method: str|None,
                                    voucher_extracted_payment_method: str|None
                                    ) -> Dict:
    """
    Validate the Payment Method based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message(" Global Level Exception andling Started for Parameter Payment Method")

    sap_payment_method: str = str(sap_row.payment_method).strip().upper()
    invoice_extracted_payment_method_processed: str|None = str(invoice_extracted_payment_method).strip().upper() if invoice_extracted_payment_method else None
    voucher_extracted_payment_method_processed: str|None = str(voucher_extracted_payment_method).strip().upper() if voucher_extracted_payment_method else None
    sap_payment_method_description: str = str(sap_row.payment_method_description).strip().upper()
    sap_payment_method_supplement: str = str(sap_row.payment_method_supplement).strip().upper()
    vim_comment_lines: list = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []
    dp_doc_type: str = str(sap_row.dp_doc_type).strip().upper()
    doc_type: str = str(sap_row.doc_type).strip().upper()

    if (sap_payment_method == "E"):
        log_message("Payment Method: E based Manual Process for Parameter Payment Method")
        return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                            is_anomaly=None,
                                            highlight=True,
                                            edit_operation=False,
                                            method="Manual",
                                            supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                                "SAP payment_method_description":str(sap_row.payment_method_description),
                                                                "SAP Payment method Code": str(sap_row.payment_method),
                                                                "Summary":"-E- payment method identified - Manual process"})
    
    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter PAYMENT METHOD")
        return build_validation_result(extracted_value={PAYMENT_METHOD: None,},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":"DOCTYPE-KE requires Combined verification."})
    
    if dp_doc_type in ["PO_EC_GLB","NPO_EC_GLB"] and str(sap_row.region).strip().upper() not in ['EMEAI','EMEA'] and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DP DOCTYPE-PO_EC_GLB in EMEAI region started for Parameter PAYMENT METHOD")
        return check_parameter_in_vim_comments(parameter=PAYMENT_METHOD,
                                                parameter_value=sap_payment_method,
                                                sap_vim_comment_lines=vim_comment_lines)

    if (doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter PAYMENT METHOD")
        return check_parameter_in_vim_comments(parameter=PAYMENT_METHOD,
                                                parameter_value=sap_payment_method,
                                                sap_vim_comment_lines=vim_comment_lines)
        
    if invoice_extracted_payment_method_processed is None and voucher_extracted_payment_method_processed is None:
        log_message("Invoice extracted Payment method value is None")
        return build_validation_result(extracted_value={PAYMENT_METHOD: None},
                                    is_anomaly=None,
                                    highlight=True,
                                    edit_operation=True,
                                    method="Combined",
                                    supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                        "SAP payment_method_description":str(sap_row.payment_method_description),
                                                        "SAP Payment method Code": str(sap_row.payment_method),
                                                        "Summary":"No payment method available in Document."})

    log_message("Standard validation process started for Parameter Payment Method")
    result = standard_validation(sap_payment_method_description=sap_payment_method_description,
                                 sap_payment_method_supplement=sap_payment_method_supplement,
                                invoice_extracted_payment_method=invoice_extracted_payment_method,
                                voucher_extracted_payment_method=voucher_extracted_payment_method)
    return result


def standard_validation(sap_payment_method_description: str,
                        sap_payment_method_supplement: str,
                        invoice_extracted_payment_method: str|None,
                        voucher_extracted_payment_method: str|None
                        ) -> Dict:
    """
    This Function Does standard Validation based on 
    SAP to Invoice Extracted payment method comparison.
    """
    log_message(f"SAP payment method: {sap_payment_method_description} and Invoice Extracted payment method: {invoice_extracted_payment_method} \
                and Voucher Extracted payment method: {voucher_extracted_payment_method}")
    
    invoice_extracted_payment_method_processed: str|None = str(invoice_extracted_payment_method).strip().upper() if invoice_extracted_payment_method else None
    voucher_extracted_payment_method_processed: str|None = str(voucher_extracted_payment_method).strip().upper() if voucher_extracted_payment_method else None

    if (sap_payment_method_description == invoice_extracted_payment_method_processed):
        return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                    is_anomaly=False,
                                    highlight=False,
                                    edit_operation=False,
                                    method="Automated",
                                    supporting_details={"payment_method_supplement":str(sap_payment_method_supplement),
                                                        "payment_method_description":str(sap_payment_method_description),
                                                        "Summary":"Automated process - Standard validation"})
    elif (sap_payment_method_description == voucher_extracted_payment_method_processed):
        return build_validation_result(extracted_value={PAYMENT_METHOD:voucher_extracted_payment_method},
                                    is_anomaly=False,
                                    highlight=False,
                                    edit_operation=False,
                                    method="Automated",
                                    supporting_details={"payment_method_supplement":str(sap_payment_method_supplement),
                                                        "payment_method_description":str(sap_payment_method_description),
                                                        "Summary":"Automated process - Standard validation"})
    else:
        return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method if invoice_extracted_payment_method is not None else voucher_extracted_payment_method},
                                    is_anomaly=True,
                                    highlight=False,
                                    edit_operation=False,
                                    method="Automated",
                                    supporting_details={"payment_method_supplement":str(sap_payment_method_supplement),
                                                        "payment_method_description":str(sap_payment_method_description),
                                                        "Summary":"Automated process - Standard validation"})