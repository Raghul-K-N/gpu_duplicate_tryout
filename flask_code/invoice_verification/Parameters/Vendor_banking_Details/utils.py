from typing import Tuple, Optional, List, Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.constant_field_names import BANK_NAME, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, PARTNER_BANK_TYPE, IBAN
from invoice_verification.Parameters.utils import build_validation_result

def get_list_values(d: Dict, key: str) -> List:
    """Get list values from dict, ensuring it's always a list"""
    val = d.get(key, [])
    if val is None:
        return []
    if isinstance(val, list):
        return [v for v in val if v is not None and str(v).strip() != ""]
    if str(val).strip() != "":
        return [val]
    return []

def get_bank_info_from_vim_comments(sap_vim_comments: List,
                                    vendor_banking_dict: Dict,
                                    control_list: List
                                    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract bank account number, bank name, and account holder name
    from VIM comments by searching for vendor banking values.
    Returns a 3-tuple of strings or None if not found.
    """
    
    # Filter vendor_banking_dict to only include fields in control_list
    filtered_vendor_dict = {
        k: v for k, v in vendor_banking_dict.items() 
        if k in control_list
    }
    
    vendor_accounts = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_NUMBER)
    # vendor_names = get_list_values(filtered_vendor_dict, BANK_NAME)
    vendor_holders = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_HOLDER_NAME)
    
    extracted_bank_account_number = None
    # extracted_bank_name = None
    extracted_bank_account_holder_name = None
    
    for comment in sap_vim_comments:
        comment_upper = str(comment).upper()
        for acc in vendor_accounts:
            if acc and str(acc).upper() in comment_upper:
                extracted_bank_account_number = acc
                break
        # for name in vendor_names:
        #     if name and str(name).upper() in comment_upper:
        #         extracted_bank_name = name
        #         break
        for holder in vendor_holders:
            if holder and str(holder).upper() in comment_upper:
                extracted_bank_account_holder_name = holder
                break
    return extracted_bank_account_number, None, extracted_bank_account_holder_name


def get_bank_info_from_vendor_account_history(vendor_account_history: str|None,
                                              vendor_banking_dict: Dict,
                                              control_list: List,
                                                ) -> Tuple[str|None, str|None, str|None]:
    """
    Get bank info from vendor account history by searching for vendor banking values.
    """
    
    # Filter vendor_banking_dict to only include fields in control_list
    filtered_vendor_dict = {
        k: v for k, v in vendor_banking_dict.items() 
        if k in control_list
    }
    
    vendor_accounts = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_NUMBER)
    # vendor_names = get_list_values(filtered_vendor_dict, BANK_NAME)
    vendor_holders = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_HOLDER_NAME)
    
    extracted_bank_account_number = None
    # extracted_bank_name = None
    extracted_bank_account_holder_name = None
    
    for line in str(vendor_account_history).splitlines():
        line_lower = line.lower()
        for acc in vendor_accounts:
            if acc and str(acc).lower() in line_lower:
                extracted_bank_account_number = acc
                break
        # for name in vendor_names:
        #     if name and str(name).lower() in line_lower:
        #         extracted_bank_name = name
        #         break
        for holder in vendor_holders:
            if holder and str(holder).lower() in line_lower:
                extracted_bank_account_holder_name = holder
                break
    return extracted_bank_account_number, None, extracted_bank_account_holder_name   


def get_bank_info_from_payee_column(sap_payee_column: str,
                                    vendor_banking_dict: Dict,
                                    control_list: List,
                                    ) -> Tuple[str|None, str|None, str|None]:
    """
    Logic for getting Bank Info from SAP Payee by searching for vendor banking values.
    """
    
    # Filter vendor_banking_dict to only include fields in control_list
    filtered_vendor_dict = {
        k: v for k, v in vendor_banking_dict.items() 
        if k in control_list
    }
    
    vendor_accounts = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_NUMBER)
    # vendor_names = get_list_values(filtered_vendor_dict, BANK_NAME)
    vendor_holders = get_list_values(filtered_vendor_dict, BANK_ACCOUNT_HOLDER_NAME)
    
    extracted_bank_account_number = None
    # extracted_bank_name = None
    extracted_bank_account_holder_name = None
    
    for line in str(sap_payee_column).splitlines():
        line_upper = line.upper()
        for acc in vendor_accounts:
            if acc and str(acc).upper() in line_upper:
                extracted_bank_account_number = acc
                break
        # for name in vendor_names:
        #     if name and str(name).upper() in line_upper:
        #         extracted_bank_name = name
        #         break
        for holder in vendor_holders:
            if holder and str(holder).upper() in line_upper:
                extracted_bank_account_holder_name = holder
                break
    return extracted_bank_account_number, None, extracted_bank_account_holder_name


def validate_bank_info_with_vendor_dict(
    extracted_bank_infos: Dict,
    vendor_banking_dict: Dict,
    control_list: List,
    partner_bank_type: str
) -> Dict:
    """
    Validate extracted bank info against vendor banking dict based on rules:
    
    Input format:
    extracted_bank_infos = {
        BANK_ACCOUNT_NUMBER: ["1111111111", "2222222222"],
        BANK_ACCOUNT_HOLDER_NAME: ["First Corp", "Second Corp"],
        BANK_NAME: ["Bank A", "Bank B"]
    }
    
    Rules:
    1. Single extracted + single vendor: partner bank type match AND string compare
    2. Single extracted + multiple vendor: partner bank type match AND extracted matches type-matched vendor
    3. Multiple extracted + single vendor: partner bank type match AND vendor matches any extracted
    4. Multiple extracted + multiple vendor: highlight all, no anomaly validation
    5. No extracted bank info: no anomaly validation, highlight=True
    
    Note: If BANK_ACCOUNT_NUMBER doesn't match, try matching with IBAN from vendor_dict
    """
    
    def normalize_value(val):
        """Convert value to lowercase stripped string"""
        if val is None:
            return ""
        return str(val).strip().lower()
    
    def compare_with_iban_fallback(ext_acc, vendor_accs, vendor_ibans, control_list):
        """
        Compare extracted account number with vendor accounts, fallback to IBAN if no match.
        Returns: (match_found, matched_index, matched_with_iban)
        """
        ext_acc_norm = normalize_value(ext_acc)
        if not ext_acc_norm:
            return True, None, False  # Empty extracted, consider as match
        
        # First try matching with BANK_ACCOUNT_NUMBER
        for idx, vendor_acc in enumerate(vendor_accs):
            vendor_acc_norm = normalize_value(vendor_acc)
            if vendor_acc_norm and ext_acc_norm == vendor_acc_norm:
                return True, idx, False
        
        # If no match with account number, try IBAN
        for idx, vendor_iban in enumerate(vendor_ibans):
            vendor_iban_norm = normalize_value(vendor_iban)
            if vendor_iban_norm and ext_acc_norm == vendor_iban_norm:
                log_message(f"  Account matched with IBAN at index {idx}")
                return True, idx, True
        
        return False, None, False
    
    # Get extracted values as lists
    extracted_accounts = get_list_values(extracted_bank_infos, BANK_ACCOUNT_NUMBER) if extracted_bank_infos and BANK_ACCOUNT_NUMBER in control_list else []
    extracted_holders = get_list_values(extracted_bank_infos, BANK_ACCOUNT_HOLDER_NAME) if extracted_bank_infos and BANK_ACCOUNT_HOLDER_NAME in control_list else []
    extracted_names = get_list_values(extracted_bank_infos, BANK_NAME) if extracted_bank_infos and BANK_NAME in control_list else []
    
    # Count extracted entries
    extracted_count = max(len(extracted_accounts), len(extracted_holders), len(extracted_names)) if extracted_bank_infos else 0
    
    # Check if extracted bank info is empty
    has_extracted = extracted_count > 0
    
    # Filter vendor_banking_dict to only include fields in control_list + partner_bank_type + IBAN
    vendor_banking_dict = {
        k: v for k, v in vendor_banking_dict.items() 
        if k in control_list or k == PARTNER_BANK_TYPE or k == IBAN
    }
    
    # Get vendor values (always include IBAN)
    vendor_accounts = get_list_values(vendor_banking_dict, BANK_ACCOUNT_NUMBER)
    vendor_holders = get_list_values(vendor_banking_dict, BANK_ACCOUNT_HOLDER_NAME)
    vendor_names = get_list_values(vendor_banking_dict, BANK_NAME)
    vendor_ibans = get_list_values(vendor_banking_dict, IBAN)  # Always get IBAN from vendor
    vendor_partner_types = get_list_values(vendor_banking_dict, PARTNER_BANK_TYPE)
    
    # Count vendor entries
    vendor_count = max(len(vendor_accounts), len(vendor_holders), len(vendor_partner_types), len(vendor_ibans))
    
    # Find partner_bank_type matched index in vendor
    partner_bank_type_matched_index = None
    partner_bank_type_normalized = str(partner_bank_type).strip().upper() if partner_bank_type else ""
    
    for i, vpt in enumerate(vendor_partner_types):
        if str(vpt).strip().upper() == partner_bank_type_normalized:
            partner_bank_type_matched_index = i
            break
    
    log_message(f"Extracted count: {extracted_count}, Vendor count: {vendor_count}")
    log_message(f"Partner bank type: {partner_bank_type}, Matched index: {partner_bank_type_matched_index}")
    
    # =========================================================================
    # Rule 5: No bank info in extracted
    # =========================================================================
    if not has_extracted:
        log_message("Rule 5: No bank info in extracted - no anomaly validation, highlight=True")
        return build_validation_result(
            extracted_value={
                BANK_ACCOUNT_NUMBER: None, 
                BANK_ACCOUNT_HOLDER_NAME: None
            },
            is_anomaly=True,
            edit_operation=True,
            highlight=True,
            method="Combined",
            supporting_details=vendor_banking_dict | {"Summary":"No extracted bank info available."}
        )
    
    # =========================================================================
    # Rule 4: Multiple extracted + multiple vendor
    # =========================================================================
    if extracted_count > 1 and vendor_count > 1:
        log_message("Rule 4: Multiple extracted + multiple vendor - no anomaly validation, highlight all")
        return build_validation_result(
            extracted_value=extracted_bank_infos,
            is_anomaly=True,
            edit_operation=True,
            highlight=True,
            method="Combined",
            supporting_details=vendor_banking_dict | {"Summary":"Multiple extracted and multiple vendor entries - highlight all."}
        )
    
    # =========================================================================
    # Rule 1: Single extracted + single vendor
    # =========================================================================
    if extracted_count == 1 and vendor_count <= 1:
        log_message("Rule 1: Single extracted + single vendor - partner bank type match AND string compare")
        
        # Check partner_bank_type match first
        partner_type_match = partner_bank_type_matched_index is not None
        
        # String comparison
        string_match = True
        matched_with_iban = False
        
        # Account number comparison with IBAN fallback
        if BANK_ACCOUNT_NUMBER in control_list:
            ext_acc = extracted_accounts[0] if extracted_accounts else None
            if ext_acc:
                acc_match, _, used_iban = compare_with_iban_fallback(
                    ext_acc, vendor_accounts, vendor_ibans, control_list
                )
                if not acc_match:
                    string_match = False
                matched_with_iban = used_iban
        
        if BANK_ACCOUNT_HOLDER_NAME in control_list:
            ext_holder = normalize_value(extracted_holders[0]) if extracted_holders else ""
            vendor_holder = normalize_value(vendor_holders[0]) if vendor_holders else ""
            if ext_holder and vendor_holder and ext_holder != vendor_holder:
                string_match = False

        if BANK_NAME in control_list:
            ext_name = normalize_value(extracted_names[0]) if extracted_names else ""
            vendor_name = normalize_value(vendor_names[0]) if vendor_names else ""
            if ext_name and vendor_name and ext_name != vendor_name:
                string_match = False
        
        # Both conditions must be met for anomaly=False
        anomaly_flag = not (partner_type_match and string_match)
        
        log_message(f"  Partner type match: {partner_type_match}, String match: {string_match}, Anomaly: {anomaly_flag}")
        
        extracted_value = {
            BANK_ACCOUNT_NUMBER: extracted_accounts[0] if extracted_accounts else None,
            BANK_ACCOUNT_HOLDER_NAME: extracted_holders[0] if extracted_holders else None,
            BANK_NAME: extracted_names[0] if extracted_names else None
        }
        
        summary = "Single extracted bank info and single vendor bank info validation."
        if matched_with_iban:
            summary += " (Matched using IBAN)"
        
        return build_validation_result(
            extracted_value=extracted_value,
            is_anomaly=anomaly_flag,
            edit_operation=False,
            highlight=True,
            method="Automated",
            supporting_details=vendor_banking_dict | {"Summary": summary}
        )
    
    # =========================================================================
    # Rule 2: Single extracted + multiple vendor
    # =========================================================================
    if extracted_count == 1 and vendor_count > 1:
        log_message("Rule 2: Single extracted + multiple vendor - partner type match AND value match")
        
        # If no partner_bank_type match found, anomaly=True
        if partner_bank_type_matched_index is None:
            log_message("  No partner bank type match found - Anomaly=True")
            extracted_value = {
                BANK_ACCOUNT_NUMBER: extracted_accounts[0] if extracted_accounts else None,
                BANK_ACCOUNT_HOLDER_NAME: extracted_holders[0] if extracted_holders else None,
                BANK_NAME: extracted_names[0] if extracted_names else None
            }
            return build_validation_result(
                extracted_value=extracted_value,
                is_anomaly=True,
                edit_operation=False,
                highlight=True,
                method="Automated",
                supporting_details=vendor_banking_dict | {"Summary":"No partner bank type match found - So Anomaly."}
            )
        
        # Get vendor values at the matched partner_bank_type index
        idx = partner_bank_type_matched_index
        
        match_found = True
        matched_with_iban = False
        
        # Account number comparison with IBAN fallback
        if BANK_ACCOUNT_NUMBER in control_list:
            ext_acc = extracted_accounts[0] if extracted_accounts else None
            if ext_acc:
                vendor_acc_at_idx = normalize_value(vendor_accounts[idx]) if idx < len(vendor_accounts) else ""
                vendor_iban_at_idx = normalize_value(vendor_ibans[idx]) if idx < len(vendor_ibans) else ""
                ext_acc_norm = normalize_value(ext_acc)
                
                # Try account number first
                acc_match = (ext_acc_norm == vendor_acc_at_idx) if (ext_acc_norm and vendor_acc_at_idx) else False
                
                # If no match, try IBAN
                if not acc_match and vendor_iban_at_idx:
                    acc_match = (ext_acc_norm == vendor_iban_at_idx)
                    if acc_match:
                        matched_with_iban = True
                        log_message(f"  Account matched with IBAN at index {idx}")
                
                if not acc_match:
                    match_found = False
        
        if BANK_ACCOUNT_HOLDER_NAME in control_list:
            ext_holder = normalize_value(extracted_holders[0]) if extracted_holders else ""
            vendor_holder_at_idx = normalize_value(vendor_holders[idx]) if idx < len(vendor_holders) else ""
            if ext_holder and vendor_holder_at_idx and ext_holder != vendor_holder_at_idx:
                match_found = False

        if BANK_NAME in control_list:
            ext_name = normalize_value(extracted_names[0]) if extracted_names else ""
            vendor_name_at_idx = normalize_value(vendor_names[idx]) if idx < len(vendor_names) else ""
            if ext_name and vendor_name_at_idx and ext_name != vendor_name_at_idx:
                match_found = False
        
        anomaly_flag = not match_found
        
        log_message(f"  Partner type matched at index {idx}, Value match: {match_found}, Anomaly: {anomaly_flag}")
        
        extracted_value = {
            BANK_ACCOUNT_NUMBER: extracted_accounts[0] if extracted_accounts else None,
            BANK_ACCOUNT_HOLDER_NAME: extracted_holders[0] if extracted_holders else None,
            BANK_NAME: extracted_names[0] if extracted_names else None
        }
        
        summary = "Standard validation"
        if matched_with_iban:
            summary += " (Matched using IBAN)"
        
        return build_validation_result(
            extracted_value=extracted_value,
            is_anomaly=anomaly_flag,
            edit_operation=False,
            highlight=True,
            method="Automated",
            supporting_details=vendor_banking_dict | {"Summary": summary}
        )
    
    # =========================================================================
    # Rule 3: Multiple extracted + single vendor
    # =========================================================================
    if extracted_count > 1 and vendor_count <= 1:
        log_message("Rule 3: Multiple extracted + single vendor - partner type match AND vendor match any extracted")
        
        # If no partner_bank_type match found, anomaly=True
        if partner_bank_type_matched_index is None:
            log_message("  No partner bank type match found - Anomaly=True")
            return build_validation_result(
                extracted_value=extracted_bank_infos,
                is_anomaly=True,
                edit_operation=False,
                highlight=True,
                method="Automated",
                supporting_details=vendor_banking_dict | {"Summary":"No partner bank type match found - So Anomaly."}
            )
        
        vendor_acc = normalize_value(vendor_accounts[0]) if vendor_accounts else ""
        vendor_iban = normalize_value(vendor_ibans[0]) if vendor_ibans else ""
        vendor_holder = normalize_value(vendor_holders[0]) if vendor_holders else ""
        
        match_found = False
        matched_index = None
        matched_with_iban = False
        
        # Iterate through each extracted entry
        for i in range(extracted_count):
            ext_acc = normalize_value(extracted_accounts[i]) if i < len(extracted_accounts) else ""
            ext_holder = normalize_value(extracted_holders[i]) if i < len(extracted_holders) else ""
            
            acc_match = True
            holder_match = True
            used_iban = False
            
            # Account number comparison with IBAN fallback
            if BANK_ACCOUNT_NUMBER in control_list:
                if ext_acc and (vendor_acc or vendor_iban):
                    # Try account number first
                    acc_match = (ext_acc == vendor_acc) if vendor_acc else False
                    
                    # If no match, try IBAN
                    if not acc_match and vendor_iban:
                        acc_match = (ext_acc == vendor_iban)
                        if acc_match:
                            used_iban = True
                            log_message(f"  Account at index {i} matched with IBAN")
            
            if BANK_ACCOUNT_HOLDER_NAME in control_list:
                holder_match = (ext_holder == vendor_holder) if (ext_holder and vendor_holder) else True

            if BANK_NAME in control_list:
                ext_name = normalize_value(extracted_names[i]) if i < len(extracted_names) else ""
                vendor_name = normalize_value(vendor_names[0]) if vendor_names else ""
                name_match = (ext_name == vendor_name) if (ext_name and vendor_name) else True
                acc_match = acc_match and name_match
            
            if acc_match and holder_match:
                match_found = True
                matched_index = i
                matched_with_iban = used_iban
                break
        
        if match_found and matched_index is not None:
            log_message(f"  Match found at extracted index {matched_index} - Anomaly=False")
            matched_value = {
                BANK_ACCOUNT_NUMBER: extracted_accounts[matched_index] if matched_index < len(extracted_accounts) else None,
                BANK_ACCOUNT_HOLDER_NAME: extracted_holders[matched_index] if matched_index < len(extracted_holders) else None,
                BANK_NAME: extracted_names[matched_index] if matched_index < len(extracted_names) else None
            }
            
            summary = "Standard validation"
            if matched_with_iban:
                summary += " (Matched using IBAN)"
            
            return build_validation_result(
                extracted_value=matched_value,
                is_anomaly=False,
                edit_operation=False,
                highlight=True,
                method="Automated",
                supporting_details=vendor_banking_dict | {"Summary": summary}
            )
        else:
            log_message("  No match found - Anomaly=True")
            return build_validation_result(
                extracted_value=extracted_bank_infos,
                is_anomaly=True,
                edit_operation=False,
                highlight=True,
                method="Automated",
                supporting_details=vendor_banking_dict | {"Summary":"No match found - Anomaly."}
            )
    
    # =========================================================================
    # Default fallback
    # =========================================================================
    log_message("Default fallback")
    return build_validation_result(
        extracted_value={
            BANK_ACCOUNT_NUMBER: None, 
            BANK_ACCOUNT_HOLDER_NAME: None
        },
        is_anomaly=True,
        edit_operation=False,
        highlight=True,
        method="Combined",
        supporting_details=vendor_banking_dict | {"Summary":"No extracted bank info available."}
    )

def ks_po_an_glb(extracted_bank_infos: Dict,
                vendor_banking_dict: Dict,
                control_list: List,
                partner_bank_type: str
                ) -> Dict:
    """
    Exception based on KS doctype and 
    DP DOC Type PO_AN_GLB is processed using
    Normal validation but if no bank info 
    is present in invoice copy then manual highlight.
    """
    # Check if extracted bank info is empty
    has_extracted = bool(extracted_bank_infos) and any(
        isinstance(v, list) and any(item is not None and str(item).strip() != "" for item in v)
        for v in extracted_bank_infos.values()
    )
    if not has_extracted:
        log_message("No bank info in extracted - no anomaly validation, highlight=True")
        return build_validation_result(
            extracted_value={BANK_NAME: None, BANK_ACCOUNT_NUMBER: None, BANK_ACCOUNT_HOLDER_NAME: None},
            is_anomaly=True,
            edit_operation=False,
            highlight=True,
            method="Combined",
            supporting_details={"Summary":"No bank info extracted from invoice - ARIBA IDOC Error - Combined process."}
        )
    
    return validate_bank_info_with_vendor_dict(
        extracted_bank_infos=extracted_bank_infos,
        vendor_banking_dict=vendor_banking_dict,
        control_list=control_list,
        partner_bank_type=partner_bank_type
    )