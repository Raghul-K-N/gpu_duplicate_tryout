# Parameters/Vendor name & Remmit to Addr/global_logic.py
"""
Global fallback vendor name validation logic
"""
from invoice_verification.constant_field_names import VENDOR_NAME, VENDOR_ADDRESS, VENDOR_NAME_AND_ADDRESS
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Vendor_name_and_Remit_to_Addr.utils import standard_vendor_validation
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional

def validate_global_vendor_info(extracted_vendor_name: Optional[str], extracted_vendor_address: Optional[str],
                              sap_row: SAPRow, validate_address: bool=True) -> dict:
    """
    Global fallback validation - validate both vendor name and address
    Args:
        extracted_vendor_name: Vendor name from invoice OCR
        extracted_vendor_address: Vendor address from invoice OCR
        sap_row: SAPRow object with all SAP data
        validate_address: Whether to validate address (default True)
    Returns:
        Dict with standardized output format
    """
    log_message("Global vendor validation")
    
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_vendor_name = sap_row.vendor_name
    sap_vendor_address = sap_row.vendor_address
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    # Global Invoice is Attached: KS with EC_GLB (except NAA) or RP_GLB (all regions)
    if doc_type == "KS" and ((dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.region not in ['NAA','NA']) or (dp_doc_type in ["PO_RP_GLB", "NPO_RP_GLB"])) and \
        not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message(f"Global: KS {dp_doc_type} - using VIM comments for Vendor Name and Address validation")
        return check_parameter_in_vim_comments(parameter=VENDOR_NAME_AND_ADDRESS,
                                        parameter_value={VENDOR_NAME: sap_vendor_name, VENDOR_ADDRESS: sap_vendor_address},
                                        sap_vim_comment_lines=vim_comment_lines)
    
    if dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.region not in ['EMEAI','EMEA'] and \
        not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message(f"Global: PO_EC_GLB/NPO_EC_GLB (non-EMEAI) - using VIM comments for Vendor Name and Address validation")
        return check_parameter_in_vim_comments(parameter=VENDOR_NAME_AND_ADDRESS,
                                        parameter_value={VENDOR_NAME: sap_vendor_name, VENDOR_ADDRESS: sap_vendor_address},
                                        sap_vim_comment_lines=vim_comment_lines)
    
    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter Vendor Name and Address")
        return build_validation_result(extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
                                        is_anomaly=None,
                                        edit_operation=True,
                                        highlight=True,
                                        method="Combined",
                                        supporting_details={"Summary":"DOCTYPE-KE requires Combined verification."})
    
    # Standard validation
    log_message("Global: Standard validation for vendor name and address")
    
    return standard_vendor_validation(
        extracted_vendor_name=extracted_vendor_name,
        sap_vendor_name=sap_vendor_name,
        extracted_vendor_address=extracted_vendor_address,
        sap_vendor_address=sap_vendor_address,
        validate_address=validate_address,
        address_threshold=75.0
    )


def should_use_global_vim_check(doc_type: str, dp_doc_type: str, region: str, invoice_copy_flag, voucher_copy_flag) -> bool:
    """
    Determine if global VIM comment check should be applied based on doc type and region
    """
    # Condition 1: KS with EC_GLB (except NAA) or RP_GLB (all regions)
    if doc_type == "KS":
        if dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and region not in ['NAA', 'NA']:
            return True
        if dp_doc_type in ["PO_RP_GLB", "NPO_RP_GLB"]:
            return True

    # Condition 2: PO_EC_GLB (except EMEAI)
    if dp_doc_type in ["PO_EC_GLB"] and region not in ['EMEAI', 'EMEA']:
        return True
    if doc_type == "KE" and not(invoice_copy_flag or voucher_copy_flag):
        return True
    return False
