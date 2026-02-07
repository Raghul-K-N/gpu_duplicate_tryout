from typing import Dict, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import GL_ACCOUNT_NUMBER
from invoice_verification.Parameters.utils import check_parameter_in_vim_comments

def validate_global_gl_account_number(sap_row: SAPRow,
            invoice_extracted_gl_account_number: List,
            voucher_extracted_gl_account_number: List
                                    ) -> Dict:
    """
    Validate the GL Account Number based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message("Started Global level Exceptions handling for Parameter GL Accounts")
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_gl_account_numbers = [str(gl_account_number).strip() for gl_account_number in sap_row.gl_account_number]
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    if (doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]) and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter INVOICE DATE")
        for sap_gl_account_number in sap_gl_account_numbers:
            return check_parameter_in_vim_comments(
                parameter_value=sap_gl_account_number,
                sap_vim_comment_lines=vim_comment_lines,
                parameter=GL_ACCOUNT_NUMBER
            )
        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                                    is_anomaly=False,
                                    highlight=False,
                                    edit_operation=False,
                                    method="Automated",
                                    supporting_details={"Summary":f"GL Account not found in SAP Data"})

    # KE Doc Type and Non-PO invoice Exception
    if doc_type=="KE" and dp_doc_type.startswith("NPO"):
        log_message(f"Doc-Type KE and Non-PO invoices Exception handling Started for Parameter GL Accounts")
        result = ke_non_po(sap_gl_account_number=sap_gl_account_numbers,
                            voucher_extracted_gl_account_number=voucher_extracted_gl_account_number,
                            invoice_extracted_gl_account_number=invoice_extracted_gl_account_number,
                            sap_vim_comment_lines=sap_row.vim_comment_lines)
        return result
    
    # DP_DOC_TYPE is [NPO_EC_GLB,PO_EC_GLB]
    if dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message(f"Started DP-Doc-Type based Exception Handling for Parameter GL Accounts")
        for sap_gl_account_number in sap_gl_account_numbers:
            return check_parameter_in_vim_comments(
                parameter_value=sap_gl_account_number,
                sap_vim_comment_lines=vim_comment_lines,
                parameter=GL_ACCOUNT_NUMBER
            )

        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                                    is_anomaly=False,
                                    highlight=False,
                                    edit_operation=False,
                                    method="Automated",
                                    supporting_details={"Summary":f"GL Account not found in SAP Data"})
    
    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter GL Accounts")
        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None,},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details={"Summary":f"KE Doc type - No Invoice or Voucher"})

    if (invoice_extracted_gl_account_number is None) and (voucher_extracted_gl_account_number is None):
        log_message("Invoice extracted Value is none for parameter GL Account Number")
        # No extracted invoice amount; return an explicit empty result with low confidence
        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                            is_anomaly=None,
                            highlight=True,
                            edit_operation=True,
                            method="Combined",
                            supporting_details={"Summary":f"No Invoice or Voucher attached"})
    
    # Standard validate
    log_message("Started Standard Validation Process for Parameter GL Accounts")
    result = standard_validation(sap_gl_account_number=sap_row.gl_account_number,
                                invoice_extracted_gl_account_number=invoice_extracted_gl_account_number,
                                voucher_extracted_gl_account_number=voucher_extracted_gl_account_number)
    return result


def ke_non_po(sap_gl_account_number: List, 
            voucher_extracted_gl_account_number: List|None,
            invoice_extracted_gl_account_number: List|None,
            sap_vim_comment_lines: List
            ) -> Dict:
    """
    Validate and extract GL account number for KE doctype non-PO invoices.
    - If sap_row.voucher_gl_account_number is not None:
        - Compare the GL account number list obtained from OCR (voucher_ocr.gl_account_number)
          to the GL account number list recorded in SAP (sap_row.gl_account_number).
        - Returns a dictionary
    - If sap_row.voucher_gl_account_number is None:
        - Delegate validation to check_gl_account_in_vim_comments(sap_row) and return its result.
    """
    if voucher_extracted_gl_account_number or invoice_extracted_gl_account_number:
        # Normalize SAP GL account numbers
        sap_gl_account_list = [str(gl).strip().upper() for gl in sap_gl_account_number]
        # Normalize voucher extracted GL account numbers
        # voucher_gl_account_list = [str(gl).strip().upper() for gl in voucher_extracted_gl_account_number]
        if invoice_extracted_gl_account_number:
            extracted_gl_account_list = [str(gl).strip().upper() for gl in invoice_extracted_gl_account_number]
            # extracted_gl_account_list = [str(invoice_extracted_gl_account_number).strip().upper()]
        elif voucher_extracted_gl_account_number:
            extracted_gl_account_list = [str(gl).strip().upper() for gl in voucher_extracted_gl_account_number]
            # extracted_gl_account_list = [str(voucher_extracted_gl_account_number).strip().upper()]
        else:
            extracted_gl_account_list = []
        
        # Check if both lists match completely (same elements, same order/count)
        is_match = sorted(sap_gl_account_list) == sorted(extracted_gl_account_list)
        
        if is_match:
            return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: extracted_gl_account_list},
                                is_anomaly=False,
                                highlight=False,
                                edit_operation=False,
                                method="Combined",
                                supporting_details={"Summary":f"KE Doc type - Non PO Document"})
        else:
            return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: extracted_gl_account_list},
                                is_anomaly=True,
                                highlight=False,
                                edit_operation=False,
                                method="Combined",
                                supporting_details={"Summary":f"KE Doc Type - Non PO Document"})
    else:
        # Check each SAP GL account in VIM comments
        for sap_gl_account in sap_gl_account_number:
            return check_parameter_in_vim_comments(sap_vim_comment_lines=sap_vim_comment_lines,
                                                    parameter=GL_ACCOUNT_NUMBER,
                                                    parameter_value=sap_gl_account)
        
        # If no anomalies found in any GL account
        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                            is_anomaly=False,
                            highlight=False,
                            edit_operation=False,
                            method="Combined",
                            supporting_details={"Summary":f"KE Doc type - Non PO Document"})


def standard_validation(sap_gl_account_number: List, 
                        invoice_extracted_gl_account_number: List|None,
                        voucher_extracted_gl_account_number: List|None
                        ) -> Dict:
    """
    Usual SAP to Invoice Validation
    """
    # Determine which extracted value to use (invoice or voucher)
    if invoice_extracted_gl_account_number:
        extracted_gl_account_list = [str(gl).strip().upper() for gl in invoice_extracted_gl_account_number]
        # extracted_gl_account_list = [str(invoice_extracted_gl_account_number).strip().upper()]
    elif voucher_extracted_gl_account_number:
        extracted_gl_account_list = [str(gl).strip().upper() for gl in voucher_extracted_gl_account_number]
        # extracted_gl_account_list = [str(voucher_extracted_gl_account_number).strip().upper()]
    else:
        extracted_gl_account_list = []

    # Normalize SAP GL account numbers
    sap_gl_account_list = [str(gl).strip().upper() for gl in sap_gl_account_number]

    log_message(f"Gl-Account-Value: Extracted value: {extracted_gl_account_list} and SAP value: {sap_gl_account_list} for Parameter GL Accounts")

    # Validate - check if both lists match completely
    if extracted_gl_account_list:
        anomaly_flag = sorted(sap_gl_account_list) != sorted(extracted_gl_account_list)
    else:
        anomaly_flag = False

    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter GL Accounts")

    # Use the original extracted value (invoice or voucher) for the result
    if invoice_extracted_gl_account_number:
        extracted_value = invoice_extracted_gl_account_number
    elif voucher_extracted_gl_account_number:
        extracted_value = voucher_extracted_gl_account_number
    else:
        extracted_value = None

    extracted_gl_account_number = {GL_ACCOUNT_NUMBER: extracted_value}

    return build_validation_result(extracted_value=extracted_gl_account_number,
                            is_anomaly=anomaly_flag,
                            highlight=False,
                            edit_operation=False,
                            method="Automated",
                            supporting_details={"Summary":f"Standard Validation"})