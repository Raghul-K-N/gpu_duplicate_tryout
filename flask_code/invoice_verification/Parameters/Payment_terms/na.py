from typing import Dict, Any, List
from invoice_verification.logger.logger import log_message
from invoice_verification.constant_field_names import PAYMENT_TERMS
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import kc_ka_doctype_common_handling, check_parameter_in_vim_comments
from invoice_verification.Parameters.Payment_terms.global_logic import validate_global_payment_terms
import pandas as pd

def  validate_naa_payment_terms(sap_row: SAPRow, 
                                invoice_extracted_payment_terms: str|None,
                                voucher_extracted_payment_terms: str|None,
                                vendors_df: pd.DataFrame|None
                                ) -> Dict:
    """
    Validate the Payment Terms based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message("Global payment terms validation for Parameter Payment Terms started.")

    doc_type: str = str(sap_row.doc_type).strip().upper()
    dp_doc_type: str = str(sap_row.dp_doc_type).strip().upper()
    sap_payment_terms: str = str(sap_row.payment_terms).strip().upper()
    vim_comment_lines: List[Any] = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    # Invoice is Attached Special-case Handling
    if doc_type in ["KA","KC"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KA and KC started for Parameter PAYMENT TERMS")
        return kc_ka_doctype_common_handling(parameter=PAYMENT_TERMS)

    elif (doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter PAYMENT TERMS")
        return check_parameter_in_vim_comments(parameter=PAYMENT_TERMS,
                                                parameter_value=sap_payment_terms,
                                                sap_vim_comment_lines=vim_comment_lines)

    return validate_global_payment_terms(sap_row=sap_row,
                                        invoice_extracted_payment_terms=invoice_extracted_payment_terms,
                                        voucher_extracted_payment_terms=voucher_extracted_payment_terms,
                                        vendors_df=vendors_df
                                        )