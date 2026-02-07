# Parameters/Bill to LE and Addr/global_logic.py
"""
Global bill to legal entity validation logic
NO REGION-SPECIFIC CONDITIONS HERE
"""
from invoice_verification.constant_field_names import LEGAL_ENTITY_NAME, LEGAL_ENTITY_ADDRESS, LEGAL_ENTITY_NAME_AND_ADDRESS
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Bill_to_LE_and_Addr.utils import (normalize_legal_entity_name,
                    normalize_legal_entity_address, partial_match_legal_entity)
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments, is_empty_value
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional

def validate_global_legal_entity(extracted_name: Optional[str], extracted_address: Optional[str],
                                 sap_row: SAPRow,
                                 extracted_remit_to_name: Optional[str],extracted_remit_to_address: Optional[str]) -> dict:
    """
    Global legal entity validation
    This should contain only truly global exceptions, no region-specific logic
    Global Exceptions:
    1. Statement (Bank/Rental): Use remit to information from OCR data (already extracted),
       partial match with SAP legal entity
    
    Falls through to standard validation
    Args:
        extracted_name: Legal entity name from OCR (or remit to name if statement)
        extracted_address: Legal entity address from OCR (or remit to address if statement)
        sap_row: SAPRow object with SAP data
        extracted_remit_to_name: Remit to name from voucher OCR (if applicable)
        extracted_remit_to_address: Remit to address from voucher OCR (if applicable)
    Returns:
        Dict with standardized output format
    """
    log_message("Global Legal Entity validation")
    
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_legal_entity_name = sap_row.legal_entity_name
    sap_legal_entity_address = sap_row.legal_entity_address
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []
    sap_remit_to_name = sap_row.vendor_name
    sap_remit_to_address = sap_row.vendor_address

    # Global Invoice is Attached: KS with EC_GLB (except NAA) or RP_GLB (all regions)
    if not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag) and \
        doc_type == "KS" and ((dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.region not in ['NAA','NA']) or (dp_doc_type in ["PO_RP_GLB", "NPO_RP_GLB"])):
        log_message(f"Global: KS {dp_doc_type} - using VIM comments for Bill to LE validation")
        return check_parameter_in_vim_comments(parameter=LEGAL_ENTITY_NAME_AND_ADDRESS,
                                        parameter_value={LEGAL_ENTITY_NAME: sap_legal_entity_name, LEGAL_ENTITY_ADDRESS: sap_legal_entity_address},
                                        sap_vim_comment_lines=vim_comment_lines)
    
    if dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.region not in ['EMEAI','EMEA'] and \
        not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message(f"Global: PO_EC_GLB (non-EMEAI) - using VIM comments for Bill to LE validation")
        return check_parameter_in_vim_comments(parameter=LEGAL_ENTITY_NAME_AND_ADDRESS,
                                        parameter_value={LEGAL_ENTITY_NAME: sap_legal_entity_name, LEGAL_ENTITY_ADDRESS: sap_legal_entity_address},
                                        sap_vim_comment_lines=vim_comment_lines)
    
    
    # Global Exception 1: Bank/Rental Statement - Use remit to information
    if sap_row.bank_statement_flag:
        log_message(f"Global: Statement detected - Using remit to information for validation")
        
        # For statements, OCRData already contains remit_to_name and remit_to_address
        remit_name = extracted_remit_to_name
        remit_address = extracted_remit_to_address
        
        # # If remit to fields are not explicitly present, use the regular extracted values
        # # (since for statements, legal_entity_name might actually be remit_to_name)
        # if not remit_name:
        #     remit_name = extracted_name
        # if not remit_address:
        #     remit_address = extracted_address
        
        log_message(f"Global: Using remit to from statement - Name: {remit_name}")
        
        # Check if we have remit to data
        if is_empty_value(remit_name) or is_empty_value(remit_address):
            log_message("Global: No remit to information found in statement")
            return build_validation_result(
                extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=True,
                method='Combined',
                supporting_details={"Summary":"Missing remit to information in statement."}
            )
        
        # Normalize for partial match
        norm_remit_name = normalize_legal_entity_name(remit_name) if remit_name else ""
        norm_le_sap_name = normalize_legal_entity_name(sap_legal_entity_name)
        norm_remit_addr = normalize_legal_entity_address(remit_address) if remit_address else ""
        norm_le_sap_addr = normalize_legal_entity_address(sap_legal_entity_address) if sap_legal_entity_address else ""
        norm_remit_sap_name = normalize_legal_entity_name(sap_remit_to_name)
        norm_sap_remit_addr = normalize_legal_entity_address(sap_remit_to_address)

        # Partial match validation
        name_match = partial_match_legal_entity(norm_remit_name, norm_le_sap_name) if norm_remit_name else False
        address_match = partial_match_legal_entity(norm_remit_addr, norm_le_sap_addr) if norm_remit_addr else False
        remit_name_match = partial_match_legal_entity(norm_remit_name, norm_remit_sap_name) if norm_remit_name else False
        remit_address_match = partial_match_legal_entity(norm_remit_addr, norm_sap_remit_addr) if norm_remit_addr else False
        # For statements, partial match is acceptable - not flagged as anomaly if matched
        is_anomaly = not (name_match and address_match)
        
        log_message(f"Global: Statement validation - Name match: {name_match}, Address match: {address_match}")
        
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: remit_name, LEGAL_ENTITY_ADDRESS: remit_address},
            is_anomaly=is_anomaly,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Statement validation with partial match for remit to name and address."}
        )
    
    # Check for empty extracted name (non-statement case)
    if is_empty_value(extracted_name) or is_empty_value(extracted_address):
        return build_validation_result(
                extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
                is_anomaly=None,
                edit_operation=True,
                highlight=True,
                method='Combined',
                supporting_details={"Summary":"Extracted legal entity name or address is missing."}
            )
    
    # Standard validation: Partial match for both name and address
    log_message("Global: Standard partial match validation for name and address")
    
    norm_extracted_name = normalize_legal_entity_name(extracted_name)
    norm_sap_name = normalize_legal_entity_name(sap_legal_entity_name)
    norm_extracted_addr = normalize_legal_entity_address(extracted_address) if extracted_address else ""
    norm_sap_addr = normalize_legal_entity_address(sap_legal_entity_address) if sap_legal_entity_address else ""
    
    name_match = partial_match_legal_entity(norm_extracted_name, norm_sap_name)
    address_match = partial_match_legal_entity(norm_extracted_addr, norm_sap_addr)
    
    is_anomaly = not (name_match and address_match)
    
    return build_validation_result(
        extracted_value={LEGAL_ENTITY_NAME: extracted_name, LEGAL_ENTITY_ADDRESS: extracted_address},
        is_anomaly=is_anomaly,
        edit_operation=False,
        highlight=False,
        method='Automated',
        supporting_details={"Summary":"Standard validation."}
    )