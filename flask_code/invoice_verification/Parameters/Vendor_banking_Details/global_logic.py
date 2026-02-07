from typing import List, Dict
from invoice_verification.logger.logger import log_message
from .utils import validate_bank_info_with_vendor_dict
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import BANK_NAME, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME

def validate_global_vendor_banking_details(sap_row: SAPRow, 
                                           extracted_bank_infos: Dict,
                                           vendor_bank_info: Dict,
                                           control_list: List
                                           ) -> Dict:
    """
    Validate the Payment Method based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message("Started Standard Validation for Paramter Vendor Banking Details")
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()

    if dp_doc_type in ["PO_EC_GLB","NPO_EC_GLB"] and str(sap_row.region).strip().upper() != "EMEAI" and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DP DOCTYPE-PO_EC_GLB started for Parameter Vendor Banking Details")
        return build_validation_result(extracted_value={
                                                        # BANK_NAME: None,
                                                        BANK_ACCOUNT_NUMBER: None,
                                                        BANK_ACCOUNT_HOLDER_NAME: None},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details=vendor_bank_info | {"Summary":"Legal requirement Exception Handling."})

    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter Vendor Banking Details")
        return build_validation_result(extracted_value={
                                                        # BANK_NAME: None,
                                                        BANK_ACCOUNT_NUMBER: None,
                                                        BANK_ACCOUNT_HOLDER_NAME: None},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details=vendor_bank_info | {"Summary":"KE-DOCTYPE and No Invoice/Voucher copy."})

    # Initialize VendorBankingDetails validator
    return validate_bank_info_with_vendor_dict(
        extracted_bank_infos=extracted_bank_infos,
        vendor_banking_dict=vendor_bank_info,
        control_list=control_list,
        partner_bank_type=str(sap_row.partner_bank_type).strip().upper()
    )