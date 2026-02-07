"""NAA region-specific bill to legal entity validation logic"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Bill_to_LE_and_Addr.utils import (normalize_legal_entity_name, normalize_legal_entity_address,
                   partial_match_legal_entity, check_naa_vip_veson_emails, CANADA_APPROVED_VARIATIONS,
                   )
from invoice_verification.Parameters.utils import (build_validation_result, check_parameter_in_vim_comments, kc_ka_doctype_common_handling, 
                                    parse_supplier_remit_details_ke, is_empty_value)
from invoice_verification.constant_field_names import LEGAL_ENTITY_NAME, LEGAL_ENTITY_ADDRESS, LEGAL_ENTITY_NAME_AND_ADDRESS
from invoice_verification.Parameters.constants import CANADA_COMPANY_CODES, UNITED_STATES_COMPANY_CODES
from invoice_verification.Schemas.sap_row import SAPRow
from typing import Optional, List


def validate_naa_legal_entity(extracted_name: Optional[str], extracted_address: Optional[str],
                              sap_row: SAPRow,
                              extracted_remit_to_name: Optional[str],
                              extracted_remit_to_address: Optional[str],
                              eml_lines: List[str]) -> dict:
    """
    NAA region legal entity validation with country-specific rules
    
    Flow:
    1. Check NAA-specific exceptions
    2. If no exceptions apply, call global logic
    
    NAA Exceptions:
    - Rule 1: Canada - Exact match or approved variations for name, partial match for address
    - Rule 2: US - Partial match for both name and address  
    - Rule 3: Doc type KE - Extract from Excel Remit To tab
    - Rule 4: Doc type KE + VIP VESON email (fusappr@dow.com) - Combined review
    """
    log_message(f"NAA Legal Entity - company_code: {sap_row.company_code}, Doc type: {sap_row.doc_type}")

    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_legal_entity_name = sap_row.legal_entity_name
    sap_legal_entity_address = sap_row.legal_entity_address
    vim_comment_lines = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []
    has_invoice_or_voucher = sap_row.invoice_copy_flag or sap_row.voucher_copy_flag
    # Invoice is Attached Condition 1: KC document type - Skip validation
    if doc_type in ["KC", "KA"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("NAA: KC document type - skipping Bill to LE validation (no invoice attached)")
        return kc_ka_doctype_common_handling(parameter=[LEGAL_ENTITY_NAME, LEGAL_ENTITY_ADDRESS])
        
    # Invoice is Attached Condition 3: KS (NPO_EC_GLB/PO_EC_GLB) with generic PDF - Use VIM comments
    if doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB", "PO_EC_GLB"] and sap_row.dummy_generic_invoice_flag and not has_invoice_or_voucher:
        log_message("NAA: KS with generic PDF - using VIM comments for Bill to LE validation")
        return check_parameter_in_vim_comments(parameter=LEGAL_ENTITY_NAME_AND_ADDRESS,
                                        parameter_value={LEGAL_ENTITY_NAME: sap_legal_entity_name, LEGAL_ENTITY_ADDRESS: sap_legal_entity_address},
                                        sap_vim_comment_lines=vim_comment_lines)
      
    # Rule 3: Doc type KE - Extract from Excel Remit To tab
    if sap_row.doc_type == 'KE' and sap_row.excel_paths and not has_invoice_or_voucher:
        remit_to_data = parse_supplier_remit_details_ke(sap_row.excel_paths)
        extracted_name = remit_to_data['supplier_name']
        extracted_address = remit_to_data['supplier_physical_address']
        log_message(f"NAA: Using Excel Supplier Remit Details data for KE doc type - Name: {extracted_name}, Address: {extracted_address}")
    

    # Rule 4: Doc type KE + VIP VESON email check (NAA specific)
    if sap_row.doc_type == 'KE' and check_naa_vip_veson_emails(eml_lines):
        log_message("NAA: KE doc type with VIP VESON email (fusappr@dow.com)")
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: extracted_name, LEGAL_ENTITY_ADDRESS: extracted_address},
            is_anomaly=False,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"DOCTYPE-KE with VIP VESON email requires Combined review."}
        )
    
    # Check for empty extracted name
    if is_empty_value(extracted_name) or is_empty_value(extracted_address):
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"Extracted legal entity name or address is missing."}
        )
    
    # Rule 1: Canada - Exact match or approved variations for name
    if sap_row.company_code in CANADA_COMPANY_CODES:
        log_message("NAA: Canada - validation with approved variations")
        
        # Validate name with approved variations
        name_is_valid = validate_canada_legal_entity_name(
            extracted_name, sap_legal_entity_name, sap_row.company_code
        )
        # Address validation - use partial match (more lenient than exact)
        norm_extracted_addr = normalize_legal_entity_address(extracted_address) if extracted_address else ""
        norm_sap_addr = normalize_legal_entity_address(sap_legal_entity_address) if sap_legal_entity_address else ""
        address_match = partial_match_legal_entity(norm_extracted_addr, norm_sap_addr) if extracted_address else True
        
        is_anomaly = not (name_is_valid and address_match)
        
        log_message(f"Canada validation - Name valid: {name_is_valid}, Address match: {address_match}")
        
        return build_validation_result(
            extracted_value={LEGAL_ENTITY_NAME: extracted_name, LEGAL_ENTITY_ADDRESS: extracted_address},
            is_anomaly=is_anomaly,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Canada validation with approved variations for legal entity name."}
        )
    
    # Rule 2: US - Partial match for both name and address
    if sap_row.company_code in UNITED_STATES_COMPANY_CODES:
        log_message("NAA: US - partial match validation")
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
            supporting_details={"Summary":"US validation with partial match for legal entity name and address."}
        )
    
    # No NAA-specific exceptions apply - call global logic
    log_message("NAA: No region-specific exceptions, calling global logic")
    from .global_logic import validate_global_legal_entity
    return validate_global_legal_entity(
        extracted_name=extracted_name,
        extracted_address=extracted_address,
        sap_row=sap_row,
        extracted_remit_to_name=extracted_remit_to_name,
        extracted_remit_to_address=extracted_remit_to_address
    )


def validate_canada_legal_entity_name(extracted_name, sap_name: str, company_code: str) -> bool:
    """
    Validate Canadian legal entity name with approved variations
    Returns:
        tuple: (is_valid, reason)
    """
    
    norm_extracted = normalize_legal_entity_name(extracted_name)
    norm_sap = normalize_legal_entity_name(sap_name)
    
    # Check exact match first
    if norm_extracted == norm_sap:
        return True
    
    # Check approved variations for sp  ecific company codes
    if company_code in CANADA_APPROVED_VARIATIONS:
        approved_list = CANADA_APPROVED_VARIATIONS[company_code]
        
        # Check if extracted name matches any approved variation
        if norm_extracted in approved_list:
            log_message(f"Canada: Approved variation found for company code {company_code}")
            return True
        
    return False