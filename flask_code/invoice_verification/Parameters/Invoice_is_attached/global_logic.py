from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_IS_ATTACHED
from invoice_verification.Parameters.constants import CITI_BANK_VENDOR_CODES
from invoice_verification.Schemas.sap_row import SAPRow


def validate_global_invoice_is_attached(sap_row: SAPRow
                                        ) -> Dict:
    """
    Global Exceptions and Standard Validation
    """

    region = str(sap_row.region).strip().upper()
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    transaction_code = str(sap_row.transaction_code).strip().upper()
    sap_assignment = str(sap_row.assignment).strip().upper()
    vendor_code = str(sap_row.vendor_code).strip().upper()
    voucher_no_invoice_checkbox = sap_row.voucher_no_invoice_checkbox
    email_present_flag = sap_row.eml_file_flag if sap_row.eml_file_flag else False
    excel_present_flag = sap_row.excel_file_flag if sap_row.excel_file_flag else False
    invoice_present_flag = sap_row.invoice_copy_flag if sap_row.invoice_copy_flag else False
    dummy_generic_present_flag = sap_row.dummy_generic_invoice_flag if sap_row.dummy_generic_invoice_flag else False
    voucher_present_flag = sap_row.voucher_copy_flag if sap_row.voucher_copy_flag else False
    bank_statement_flag = sap_row.bank_statement_flag if sap_row.bank_statement_flag else False
    rental_agreement_flag = sap_row.rental_aggrement_flag if sap_row.rental_aggrement_flag else False
    supporting_documents_present_flag = sap_row.supporting_documents_present_flag if sap_row.supporting_documents_present_flag else False
    confidential_documents = sap_row.confidential_documents if sap_row.confidential_documents else False

    if doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"]:
        log_message(f"KS doc type and [NPO_EC_GLB, PO_EC_GLB] dp_doc_type Exception Handling Started for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KS with DP_DOCTYPE- [PO_EC_GLB, NPO_EC_GLB]."})
    
    elif doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]:
        log_message(f"KS doc type and [NPO_RP_GLB, PO_RP_GLB] dp_doc_type Exception Handling Started for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KS with DP_DOCTYPE- [PO_RP_GLB, NPO_RP_GLB]."})
    
    elif doc_type == "KE":
        log_message(f"KE doc type based Exception process started for Invoice is Attached Parameter")
        if vendor_code in CITI_BANK_VENDOR_CODES:
            anomaly_flag = False if bank_statement_flag or invoice_present_flag else True
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=anomaly_flag,
                                        highlight=False,
                                        edit_operation=True,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KE with CITI_BANK_VENDOR_CODES."})
        elif all((any((dummy_generic_present_flag,invoice_present_flag)),email_present_flag,excel_present_flag)):
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=True,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KE with all required documents present."})
        # elif not(all((excel_present_flag,dummy_generic_present_flag))) and email_present_flag:
        #     return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
        #                                 is_anomaly=anomaly_flag,
        #                                 highlight=False,
        #                                 edit_operation=False,
        #                                 method="Automated",
        #                                 supporting_details={})
        elif email_present_flag and excel_present_flag:
            anomaly_flag = any(s.lower().endswith(".pdf") for s in sap_row.email_attachments) if sap_row.email_attachments else False
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=anomaly_flag,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KE with email and excel present, checking for PDF in Email attachments."})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"DOCTYPE-KE missing required documents(Email, Excel, Dummy Generic or Invoice)."})
    
    elif doc_type == "KY":
        log_message("KY doc type based Exceptions Process: if Voucher then tickbox decides anomaly else standard validation for Invoice is Attached Parameter")
        return ky_voucher_validation(invoice_present_flag=invoice_present_flag,
                                        voucher_present_flag=voucher_present_flag,
                                        voucher_no_invoice_checkbox=voucher_no_invoice_checkbox)
    
    elif transaction_code in ["MR8M", "FB08"]:
        log_message(f"transaction code {transaction_code} based Exception Handling for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=None,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":f"T-code {transaction_code}."})
    
    elif doc_type == "KA":
        log_message(f"Down Payment based Exception Handling for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Down Payment based Manual process."})
    
    elif transaction_code == "ME23N":
        log_message(f"transaction code {transaction_code} based Exception Handling for Invoice is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Transaction code ME23N based Manual process."})
    
    elif "RENTAL" in sap_assignment:
        log_message("Rental Statement based Exception Handling Started for Invoice is attached parameter")
        if rental_agreement_flag:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Combined",
                                        supporting_details={"Summary":"Rental Statement based Combined Process."})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Combined",
                                        supporting_details={"Summary":"Rental Statement based Combined Process."})
    
    elif dp_doc_type in ["PO_CS_GLB", "NPO_CS_GLB"]:
        log_message("for CASS - Excel File based Exceptions Handling Started for Invoice is Attached Parameter")
        if excel_present_flag:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"CASS - Excel File based Exceptions Handling."})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":"CASS - Excel File based Exceptions Handling."})

    elif (region in ["NAA","EMEAI","APAC"] and dp_doc_type == "PO_IU_GLB") or (region == "LAA" and dp_doc_type in ["PO_IU_GLB", "PO_IU_BRA"]):
        log_message("Region and DP doc type based Script Invoice Exception Handling for Inoie is Attached Parameter")
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=None,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"Summary":f"{region} and {dp_doc_type} based Script Invoice Exception Handling."})
    
    else:
        if supporting_documents_present_flag or invoice_present_flag:
            log_message("Supporting Documents based Exceptions Handling Started for Invoice is Attached Parameter")
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Supporting Documents based Manual process."})
        elif confidential_documents:
            log_message("Confidential Documents based Exceptions Handling Started for Invoice is Attached Parameter")
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=False,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Confidential Documents based Manual process."})
        else:
            log_message("Supporting/Confidential Documents based Exceptions Handling Started for Invoice is Attached Parameter")
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                        is_anomaly=True,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"No Documents Attached."})

def ky_voucher_validation(invoice_present_flag: bool,
                            voucher_present_flag: bool,
                            voucher_no_invoice_checkbox: Optional[bool]=None,
                            ) -> Dict:
    """
    Ky Doc type based exception Handling
    If Voucher is present then check the invoice tickbox
    else check the invoice copy based validation.
    """
    if voucher_present_flag:
        invoice_tickbox = voucher_no_invoice_checkbox
        log_message(f"Tickbox returned from Vouchercopy: {invoice_tickbox} for Invoice is Attached Parameter")
        if invoice_tickbox and invoice_present_flag:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Voucher and Invoice tickbox validation."})
        else:
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Voucher and Invoice tickbox validation."})
    elif invoice_present_flag:
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Invoice present"})
    else:
        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Invoice and Voucher copy not present"})