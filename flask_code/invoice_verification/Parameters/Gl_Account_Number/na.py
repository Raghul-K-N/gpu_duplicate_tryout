from typing import Dict, List, Any
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, kc_ka_doctype_common_handling, check_parameter_in_vim_comments
from invoice_verification.constant_field_names import GL_ACCOUNT_NUMBER
from invoice_verification.Schemas.sap_row import SAPRow

def validate_naa_gl_account_number(sap_row: SAPRow, 
                                invoice_extracted_gl_account_number: List,
                                voucher_extracted_gl_account_number: List
                                ) -> Dict:
    """
    NAA-specific GL Accounts extraction logic
    """
    log_message(f"NAA based exceptions process Started for Parameter GL Accounts")

    dp_doc_type: str = str(sap_row.dp_doc_type).strip().upper()
    sap_gl_account_numbers: List = [str(gl_account_number).strip() for gl_account_number in sap_row.gl_account_number]
    doc_type: str = str(sap_row.doc_type).strip().upper()
    vim_comment_lines: List[Any] = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    # Invoice is Attached Special-case Handling
    if doc_type in ["KA","KC"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KA and KC started for Parameter GL_ACCOUNT_NUMBER")
        return kc_ka_doctype_common_handling(parameter=GL_ACCOUNT_NUMBER)

    elif not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag) and \
        (doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter GL_ACCOUNT_NUMBER")
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
                                    supporting_details={"Summary": "No GL accounts listed in SAP"})
    
    if dp_doc_type in ["PO_FR_GLB","NPO_FR_GLB","FR_EI_AUT"]: # NOTE: then it is a Freight Invoice
        log_message(f"Freight invoice based Exception Handling Started for Parameter GL Accounts")
        result = freight_no_voucher(sap_gl_account_numbers=sap_gl_account_numbers, 
                                    invoice_extracted_gl_account_number=invoice_extracted_gl_account_number,
                                    voucher_extracted_gl_account_number = voucher_extracted_gl_account_number)
        return result
    
    log_message("No NAA Region level Exceptions were met for parameter GL accounts")
    from .global_logic import validate_global_gl_account_number
    return validate_global_gl_account_number(
        sap_row=sap_row,
        invoice_extracted_gl_account_number=invoice_extracted_gl_account_number,
        voucher_extracted_gl_account_number=voucher_extracted_gl_account_number
    )


def freight_no_voucher(sap_gl_account_numbers: List, 
                    invoice_extracted_gl_account_number: List,
                    voucher_extracted_gl_account_number: List
                    ) -> Dict:
    """
        When no voucher copy is present and the SAP and Invoice Copy GL account numbers
        differ, returns the result of build_validation_result(...) with the following fields:
            - extracted_value: {'gl_account_number': <extracted_list>}
            - is_anomaly: False only if all SAP GL accounts match with invoice/voucher, True otherwise
            - highlight: False
            - edit_operation: False
            - method: 'Automated'
    """
    sap_gl_account_numbers_processed = [str(gl).strip().upper() for gl in sap_gl_account_numbers]
    log_message(f"freight Invoice Handling: SAP values-{sap_gl_account_numbers}, invoice values-{invoice_extracted_gl_account_number}, voucher values-{voucher_extracted_gl_account_number}")
    
    # If both invoice and voucher are None or empty
    if (not invoice_extracted_gl_account_number or invoice_extracted_gl_account_number == [None]) and \
       (not voucher_extracted_gl_account_number or voucher_extracted_gl_account_number == [None]): 
        # Check if all SAP GL accounts are freight accounts
        all_freight = any(str(gl).strip() in ["514008", "514009"] for gl in sap_gl_account_numbers_processed)
        return build_validation_result(
            extracted_value={GL_ACCOUNT_NUMBER: invoice_extracted_gl_account_number},
            is_anomaly=not all_freight,
            highlight=False,
            edit_operation=False,
            method="Automated",
            supporting_details={"Summary":f"No data found in Invoice or Voucher copy"}
        )

    # Determine which extracted list to use
    if voucher_extracted_gl_account_number and voucher_extracted_gl_account_number != [None]:
        extracted_gl_account_numbers = voucher_extracted_gl_account_number
        extracted_gl_processed = [str(gl).strip().upper() for gl in voucher_extracted_gl_account_number if gl is not None]
    else:
        extracted_gl_account_numbers = invoice_extracted_gl_account_number
        extracted_gl_processed = [str(gl).strip().upper() for gl in invoice_extracted_gl_account_number if gl is not None]

    # Convert to sets for comparison
    sap_set = set(sap_gl_account_numbers_processed)
    extracted_set = set(extracted_gl_processed)
    
    # Check if all SAP GL accounts are present in extracted list
    all_match = sap_set.issubset(extracted_set) and len(sap_set) == len(extracted_set)
    
    return build_validation_result(
        extracted_value={GL_ACCOUNT_NUMBER: extracted_gl_account_numbers},
        is_anomaly=not all_match,
        highlight=False,
        edit_operation=False,
        method="Automated",
        supporting_details={"Summary":f"Frieght-No Voucher Exception"}
    )