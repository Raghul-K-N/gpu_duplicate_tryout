from typing import Dict, List
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Parameters.utils import build_validation_result, kc_ka_doctype_common_handling
from invoice_verification.logger.logger import log_message
from .utils import ks_po_an_glb, get_list_values
from invoice_verification.constant_field_names import BANK_NAME, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, PARTNER_BANK_TYPE

def validate_naa_vendor_banking_details(sap_row: SAPRow,
                                        extracted_bank_infos: Dict,
                                        vendor_bank_info: Dict,
                                        control_list: List
                                        ) -> Dict:
    """
    Validate all NAA region based Exceptions
    """
    partner_bank_type = str(sap_row.partner_bank_type).strip().upper()
    payment_method = str(sap_row.payment_method).strip().upper()
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()

    if payment_method == "F":
        log_message("Handled Payment method - F Exception: No process and No Validadtion - Combined for Paramter Vendor Banking Details")
        return build_validation_result(extracted_value={
                                                        # BANK_NAME: None,
                                                        BANK_ACCOUNT_NUMBER: None,
                                                        BANK_ACCOUNT_HOLDER_NAME: None},
                        is_anomaly=None,
                        edit_operation=False,
                        highlight=True,
                        method="Combined",
                        supporting_details={"Summary":"Payment Method 'F'(Supply Chain Financing (SCF)) - No validation performed."})
    
    # Invoice is Attached Special-case Handling
    if doc_type in ["KA","KC"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KA and KC started for Parameter INVOICE CURRENCY")
        return kc_ka_doctype_common_handling(parameter=[BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME])

    elif (doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter INVOICE CURRENCY")
        return build_validation_result(extracted_value={
                                                        # BANK_NAME: None,
                                                        BANK_ACCOUNT_NUMBER: None,
                                                        BANK_ACCOUNT_HOLDER_NAME: None},
                        is_anomaly=None,
                        highlight=True,
                        edit_operation=True,
                        method="Manual",
                        supporting_details={"Summary":"Bank details not found in VIM comments - Manual review"})
    
    if (doc_type == "KS") and (dp_doc_type == "PO_AN_GLB"):
        log_message(" Started Handling Exception based on KS doc_type and PO_AN_GLB dp_doc_type for Paramter Vendor Banking Details")
        result = ks_po_an_glb(extracted_bank_infos=extracted_bank_infos,
                              vendor_banking_dict=vendor_bank_info,
                              control_list=control_list,
                              partner_bank_type=partner_bank_type)
        return result
        
    # No NAA Exception Conditions were met
    log_message("No NAA Exception Conditions were met for Parameter Vendor banking Details")
    from .global_logic import validate_global_vendor_banking_details
    return validate_global_vendor_banking_details(
        sap_row=sap_row,
        extracted_bank_infos=extracted_bank_infos,
        vendor_bank_info=vendor_bank_info,
        control_list=control_list
    )