from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, kc_ka_doctype_common_handling, check_parameter_in_vim_comments
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.constant_field_names import PAYMENT_METHOD

def validate_naa_payment_method(sap_row: SAPRow,
                                invoice_extracted_invoice_currency: str|None,
                                invoice_extracted_payment_method: str|None,
                                voucher_extracted_invoice_currency: str|None,
                                voucher_extracted_payment_method: str|None
                                ) -> Dict:
    """
    Validate NAA based Exceptions
    """
    log_message("NAA region based Exception Handling started for Parameter Payment method")

    tenant_code = str(sap_row.tenant_code).strip().upper()
    company_code = str(sap_row.company_code).strip().upper()
    if not isinstance(sap_row.po_type, list):
        sap_row.po_type = [sap_row.po_type]
    po_types = [str(sap_row.po_type).strip().upper() for po_type in sap_row.po_type] if sap_row.po_type else []
    item_categories = [str(sap_row.item_category).strip().upper() for item_category in sap_row.item_category] if sap_row.item_category else []
    naa_currencies = ["BRL","CLP","CNY","INR","KRW","MXN","MYR","PHP","QAR","RUB","THB","TWD"]
    invoice_extracted_invoice_currency = str(invoice_extracted_invoice_currency).strip().upper() if invoice_extracted_invoice_currency else None
    sap_payment_method = str(sap_row.payment_method).strip().upper()
    sap_payment_method_supplement = str(sap_row.payment_method_supplement).strip().upper()
    doc_type: str = str(sap_row.doc_type).strip().upper()
    dp_doc_type: str = str(sap_row.dp_doc_type).strip().upper()
    vim_comment_lines: list = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    if "663" in tenant_code:
        log_message(f"Tenant: {tenant_code} based Exception Handling Started for Parameter Payment Method")
        if sap_payment_method in ["P","M"]:
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                                "SAP payment_method_description":str(sap_row.payment_method_description),
                                                                "SAP Payment method Code": str(sap_row.payment_method),
                                                                "Summary":f"Automated process for Client: {tenant_code}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                                "SAP payment_method_description":str(sap_row.payment_method_description),
                                                                "SAP Payment method Code": str(sap_row.payment_method),
                                                                "Summary":f"Anomaly detected for Client: {tenant_code}"})
    
    if invoice_extracted_invoice_currency and (invoice_extracted_invoice_currency in naa_currencies):
        log_message(f"Currency - {invoice_extracted_invoice_currency} based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Automated process for NAA Currency: {invoice_extracted_invoice_currency}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Anomaly detected for NAA Currency: {invoice_extracted_invoice_currency}"})
    
    if (invoice_extracted_invoice_currency == "CNY") and any(item_category == "D" for item_category in item_categories) and any(po_type in ["ZMRV","ZMRO","ZLGS"] for po_type in po_types):
        log_message(f"Chinese Service invoice based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary": "Automated process for Chinese Service Invoice"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary": "Anomaly detected for Chinese Service Invoice"})
        
    if company_code in ["1102","1386"]:
        log_message(f"Company Code - {company_code} based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Automated process for Company Codes: {company_code}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD: invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Anomaly detected for Company Codes: {company_code}"})
        
    if doc_type in ["KA","KC"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KA and KC started for Parameter PAYMENT METHOD")
        return kc_ka_doctype_common_handling(parameter=PAYMENT_METHOD)

    elif (doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter PAYMENT METHOD")
        return check_parameter_in_vim_comments(parameter=PAYMENT_METHOD,
                                                parameter_value=sap_payment_method,
                                                sap_vim_comment_lines=vim_comment_lines)

    from .global_logic import validate_global_payment_method
    return validate_global_payment_method(
        sap_row=sap_row,
        invoice_extracted_payment_method=invoice_extracted_payment_method,
        voucher_extracted_payment_method=voucher_extracted_payment_method
    )