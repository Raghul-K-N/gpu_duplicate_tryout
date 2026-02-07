"""
Global fallback validation for Payment Terms
"""
from typing import Dict,Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.constant_field_names import PAYMENT_TERMS
import pandas as pd

def validate_global_payment_terms(
         sap_row: SAPRow,
         invoice_extracted_payment_terms: str|None,
         voucher_extracted_payment_terms: str|None,
         vendors_df: Optional[pd.DataFrame]=None
    ) -> Dict:
    """
    Global validation for Payment Terms.

    Logic summary:
      - If SAP payment term code is "A845" or "A846": return valid (not an anomaly).
      - If the invoice-extracted payment terms are None: return a neutral/undetermined result.
      - If the extracted payment terms (trimmed, case-insensitive) do not match the SAP payment term description: mark as anomaly.
      - If they match, then:
          * If vendors_df is provided and the vendor's allowed payment_term_code list contains the SAP payment term code valid.
          * Else if the SAP payment term reason code is one of ["R10","R22","R23","R24"] valid (with highlight).
          * Otherwise anomaly.

    Implementation notes:
      - Vendor lookup compares uppercased, trimmed strings and logs any dataframe errors.
      - Returns a dict created by build_validation_result with keys like extracted_value, is_anomaly, highlight, etc.
    """

    log_message("Running Global Payment Terms Validation")

    sap_payment_terms = str(sap_row.payment_terms).strip().upper()
    sap_payment_term_reason_code = str(sap_row.payment_term_reason_code).strip().upper()
    sap_payment_term_description = str(sap_row.payment_term_description).strip().upper()
    sap_vendor_code = str(sap_row.vendor_code).strip().upper()
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    doc_type = str(sap_row.doc_type).strip().upper()

    log_message(
        f"Received values — SAP Payment Terms: {sap_payment_terms}, "
        f"SAP Payment Term Description: {sap_payment_term_description}, "
        f"SAP Reason Code: {sap_payment_term_reason_code}, "
        f"SAP Vendor Code: {sap_vendor_code}, "
        f"Extracted Payment Terms: {invoice_extracted_payment_terms}"
    )

    if sap_payment_terms in ["A845", "A846"]:
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
            is_anomaly=True,
            edit_operation=False,
            highlight=True,
            method="Automated",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":f"SAP Payment Terms code: {sap_payment_terms} Exception handling."}
        )
    
    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter PAYMENT TERMS")
        return build_validation_result(extracted_value={PAYMENT_TERMS: None,},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                                            "SAP Payment Term Description": sap_payment_term_description,
                                                            "Summary":"DOCTYPE-KE requires Combined verification."})
    
    if dp_doc_type in ["PO_EC_GLB","NPO_EC_GLB"] and str(sap_row.region).strip().upper() not in ['EMEAI','EMEA'] and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DP DOCTYPE-PO_EC_GLB/NPO_EC_GLB in EMEAI region started for Parameter PAYMENT TERMS")
        return check_parameter_in_vim_comments(parameter=PAYMENT_TERMS,
                                                parameter_value=sap_payment_terms,
                                                sap_vim_comment_lines=vim_comment_lines)

    if (doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter PAYMENT TERMS")
        return check_parameter_in_vim_comments(parameter=PAYMENT_TERMS,
                                                parameter_value=sap_payment_terms,
                                                sap_vim_comment_lines=vim_comment_lines)

    if invoice_extracted_payment_terms is None and voucher_extracted_payment_terms is None:
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: None},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method="Combined",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":"No extracted Payment Terms available in Document."}
        )
    
    if invoice_extracted_payment_terms:
        invoice_extracted_payment_terms = str(invoice_extracted_payment_terms).strip()
    elif voucher_extracted_payment_terms:
        invoice_extracted_payment_terms = str(voucher_extracted_payment_terms).strip()
    else:
        invoice_extracted_payment_terms = ""

    if invoice_extracted_payment_terms.strip().lower() != sap_payment_term_description.strip().lower():
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":"Standard Validation."}
        )

    if vendors_df is None:
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":"Vendor data not provided for validation."}
        )

    vendor_payment_term_code_list = []
    try:
        vendor_payment_term_code_list = (
            vendors_df.loc[vendors_df["vendor_code"].astype(str).str.strip().str.upper() == sap_vendor_code.strip().upper(), "payment_terms"]
            .dropna()
            .unique()
            .astype(str)
            .tolist()
        )
        vendor_payment_term_code_list = [str(code).strip().upper() for code in vendor_payment_term_code_list]

        log_message(
            f"Vendor {sap_vendor_code} has payment term codes: {vendor_payment_term_code_list} for Parameter Payment Terms"
        )
    except Exception as e:
        log_message(f"Vendor dataframe validation error: {str(e)} for Parameter Payment Terms", error_logger=True)

    if sap_payment_terms in vendor_payment_term_code_list:
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
            is_anomaly=False,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":f"Vendor payment terms validation for vendor code: {sap_vendor_code}."}
        )

    if sap_payment_term_reason_code in ["R10", "R22", "R23", "R24"]:
        return build_validation_result(
            extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
            is_anomaly=False,
            edit_operation=False,
            highlight=True,
            method="Automated",
            supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                                "SAP Payment Term Description": sap_payment_term_description,
                                "Summary":f"SAP Payment Terms reason code: {sap_payment_term_reason_code} Exception handling."}
        )

    return build_validation_result(
        extracted_value={PAYMENT_TERMS: invoice_extracted_payment_terms},
        is_anomaly=True,
        edit_operation=False,
        highlight=False,
        method="Automated",
        supporting_details={"SAP Payment Terms Code": sap_payment_terms,
                            "SAP Payment Term Description": sap_payment_term_description,
                            "Summary":"Standard Validation."}
    )