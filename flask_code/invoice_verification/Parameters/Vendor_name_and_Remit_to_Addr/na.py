# Parameters/Vendor name & Remmit to Addr/na.py
"""
NAA region-specific vendor name and remit to address validation logic
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Vendor_name_and_Remit_to_Addr.utils import (normalize_vendor_name, normalize_address, fuzzy_match_vendor, 
                   get_payee_address_from_vendor_master)
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value, kc_ka_doctype_common_handling, check_parameter_in_vim_comments
from invoice_verification.constant_field_names import VENDOR_NAME, VENDOR_ADDRESS, VENDOR_NAME_AND_ADDRESS
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional
import pandas as pd


def validate_naa_vendor_info(extracted_vendor_name: Optional[str], extracted_vendor_address: Optional[str], 
                             sap_row: SAPRow, vendors_df: Optional[pd.DataFrame]) -> dict:
    """
    NAA region vendor validation with payment-method-based address rules
    Rules:
    1. Always validate vendor name
    2. If payment method is 'C' (check), validate remit to address
    3. If payment method is 'Z', use payee address from vendor master for validation
    Args:
        extracted_vendor_name: Vendor name from invoice OCR
        extracted_vendor_address: Remit to address from invoice OCR
        sap_row: SAPRow object with all SAP data
        vendors_df: Vendor master DataFrame
    Returns:
        Dict with standardized output format
    """
    log_message(f"NAA Vendor validation - Doc type: {sap_row.doc_type} - Payment method: {sap_row.payment_method}")
    
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_vendor_name = sap_row.vendor_name
    sap_vendor_address = sap_row.vendor_address
    payment_method = sap_row.payment_method
    vendor_code = sap_row.vendor_code
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []
    
    # Invoice is Attached Condition 1: KC document type - Skip validation
    if doc_type in ["KC", "KA"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("NAA: KC/KA document type - skipping Vendor Name and Address validation (no invoice attached)")
        return kc_ka_doctype_common_handling(parameter=[VENDOR_NAME, VENDOR_ADDRESS])
        
    # Invoice is Attached Condition 3: KS (NPO_EC_GLB/PO_EC_GLB) with generic PDF - Use VIM comments
    if doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag and \
        not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("NAA: KS with generic PDF - using VIM comments for Vendor Name and Address validation")
        check_parameter_in_vim_comments(parameter=VENDOR_NAME_AND_ADDRESS,
                                        parameter_value={VENDOR_NAME: sap_vendor_name, VENDOR_ADDRESS: sap_vendor_address},
                                        sap_vim_comment_lines=vim_comment_lines)
        
    from .global_logic import validate_global_vendor_info, should_use_global_vim_check
    
    if should_use_global_vim_check(doc_type=doc_type, dp_doc_type=dp_doc_type, region=sap_row.region, invoice_copy_flag=sap_row.invoice_copy_flag, voucher_copy_flag=sap_row.voucher_copy_flag):
        log_message("NAA: Global VIM check condition met - calling global logic")
    
        return validate_global_vendor_info(
            extracted_vendor_name=extracted_vendor_name,
            extracted_vendor_address=extracted_vendor_address,
            sap_row=sap_row
            )
    
    # Check for empty/None vendor name first
    if is_empty_value(extracted_vendor_name):
        log_message("NAA: Extracted vendor name is Empty or None")
        return build_validation_result(
            extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"Vendor name is missing in Invoice/Voucher copy."}
        )
    
    # Normalize vendor name
    norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
    norm_sap_name = normalize_vendor_name(sap_vendor_name)

    #1. Always validate vendor name
    name_match = fuzzy_match_vendor(str1= norm_extracted_name,str2= norm_sap_name)
    
    address_match = False  #address anomaly flag
    payee_address = None
    supporting_details = {}

    # Normalize addresses
    norm_extracted_addr = normalize_address(extracted_vendor_address)
    norm_sap_addr = normalize_address(sap_vendor_address)

    # Determine address validation based on payment method
    if payment_method == 'C':
        # Rule 2: Check payment - validate remit to address
        log_message("NAA: Payment method C (check) - validating remit to address")
        address_match = fuzzy_match_vendor(norm_extracted_addr, norm_sap_addr, threshold=75.0)
        supporting_details = {"Summary":"Payment method Check - remit to address validated."}
    elif payment_method == 'Z':
        # First try matching with remit to address
        remit_address_match = fuzzy_match_vendor(norm_extracted_addr, norm_sap_addr, threshold=75.0)
        
        if not remit_address_match:
            # If remit address doesn't match, try payee address
            # payee_address = get_payee_address_from_vendor_master(vendor_code=vendor_code, vendors_df=vendors_df)
            payee_address = sap_row.payee_address
            if payee_address:
                norm_payee_addr = normalize_address(payee_address)
                address_match = fuzzy_match_vendor(norm_payee_addr, norm_sap_addr, threshold=75.0)
                log_message("NAA: Payment method Z - checking payee address")
                supporting_details = {'payee_address': payee_address} | {"Summary":"Payment method Z - used payee address for remit to address validation."}
            else:
                # Payee address not found - mark as anomaly
                log_message("NAA: Payment method Z but payee address not found")
                return build_validation_result(
                    extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
                    is_anomaly=True,
                    edit_operation=False,
                    highlight=False,
                    method='Automated',
                    supporting_details={'payee_address': None,
                                        "Summary":"Payee address not found in vendor master."}
                )
        else:
            address_match = True
            supporting_details = {"Summary":"Payment method Z - remit to address matched."}
            log_message("NAA: Payment method Z - remit address matched")
    
    else:
        # For all other payment methods in NAA region, skip address validation
        address_match = True
        supporting_details = {"Summary":"Remit to address validation skipped - other payment method"}
        log_message("NAA: Other payment method - skipping address validation")


    if name_match and address_match:
        log_message("NAA: Vendor name and address matched")
        return build_validation_result(
            extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
            is_anomaly=False,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details=supporting_details | {"Summary":"Standard Validation."}
        )
    else:
        log_message("NAA: Vendor name or address did not match")
        return build_validation_result(
            extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details=supporting_details | {"Summary":"Standard Validation."}
        )

