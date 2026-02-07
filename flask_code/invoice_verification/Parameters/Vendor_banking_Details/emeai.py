from typing import Dict, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from .utils import get_bank_info_from_vendor_account_history, validate_bank_info_with_vendor_dict, get_list_values, ks_po_an_glb
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import BANK_NAME, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, ESR_NUMBER, IBAN
from invoice_verification.Parameters.constants import SWITZERLAND_COMPANY_CODES

def validate_emeai_vendor_banking_details(sap_row: SAPRow,
                                        extracted_bank_infos: Dict,
                                        vendor_bank_info: Dict,
                                        control_list: List
                                    ) -> Dict:
    """EMEAI-specific invoice receipt_date extraction logic"""
    log_message(f"Validate Emeai based Exceptions for Paramter Vendor Banking Details")

    # Initialize Variables
    company_code = str(sap_row.company_code).strip().upper()
    vendor_code = str(sap_row.vendor_code).strip().upper()
    vendor_account_history = str(sap_row.vendor_account_history).strip() if sap_row.vendor_account_history else ""
    partner_bank_type = str(sap_row.partner_bank_type).strip().upper()
    payment_method = str(sap_row.payment_method).strip().upper() if sap_row.payment_method else ""
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper() if sap_row.dp_doc_type else ""

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
    
    if (doc_type == "KS") and (dp_doc_type == "PO_AN_GLB"):
        log_message(" Started Handling Exception based on KS doc_type and PO_AN_GLB dp_doc_type for Paramter Vendor Banking Details")
        result = ks_po_an_glb(extracted_bank_infos=extracted_bank_infos,
                              vendor_banking_dict=vendor_bank_info,
                              control_list=control_list,
                              partner_bank_type=partner_bank_type)
        return result

    if (company_code in [str(code).strip().upper() for code in SWITZERLAND_COMPANY_CODES]) and (company_code == "1097"):
        log_message("Started Switzerland, company_code-1097 Exception Handling for Paramter Vendor Banking Details")
        result = switzerland_1097_validatin(vendor_bank_info=vendor_bank_info,
                                            extracted_bank_infos=extracted_bank_infos,
                                            sap_esr_number=sap_row.esr_number,
                                            invoice_extracted_esr_number=extracted_bank_infos.get(ESR_NUMBER) if extracted_bank_infos else None,
                                            control_list=control_list,
                                            partner_bank_type=partner_bank_type
                                            )
        return result
    
    if (company_code == "921") and (vendor_code=="1590807"):
        log_message("Started India based Vendor code exception handling for Paramter Vendor Banking Details")
        result = india_vendcode_1590807_validation(vendor_account_history=vendor_account_history,
                                                   vendor_bank_info=vendor_bank_info,
                                                   extracted_bank_infos=extracted_bank_infos,
                                                   control_list=control_list,
                                                   partner_bank_type=partner_bank_type
                                                   )
        return result
        
    # No EMEAI Exception Conditions were met
    log_message("No EMEAI Exception Conditions were met for Parameter Vendor banking Details")
    from .global_logic import validate_global_vendor_banking_details
    return validate_global_vendor_banking_details(
        sap_row=sap_row,
        extracted_bank_infos=extracted_bank_infos,
        vendor_bank_info=vendor_bank_info,
        control_list=control_list
    )

def switzerland_1097_validatin(sap_esr_number: str,
                               vendor_bank_info: Dict,
                               extracted_bank_infos: Dict,
                               invoice_extracted_esr_number: str|None,
                               control_list: List,
                               partner_bank_type: str
                                ) -> Dict:
    """
    This function handles exception for
    Country Switzerland and 1097-Company Code.
    """
    sap_iban = str(vendor_bank_info.get(IBAN)).strip().lower() if vendor_bank_info.get(IBAN) else None

    # Get extracted values as lists
    extracted_accounts = get_list_values(extracted_bank_infos, BANK_ACCOUNT_NUMBER) if extracted_bank_infos and BANK_ACCOUNT_NUMBER in control_list else []
    extracted_holders = get_list_values(extracted_bank_infos, BANK_ACCOUNT_HOLDER_NAME) if extracted_bank_infos and BANK_ACCOUNT_HOLDER_NAME in control_list else []
    extracted_names = get_list_values(extracted_bank_infos, BANK_NAME) if extracted_bank_infos and BANK_NAME in control_list else []
    extracted_ibans = get_list_values(extracted_bank_infos, IBAN) if extracted_bank_infos and IBAN in control_list else []
    
    log_message(f"Vendor IBAN Number: {sap_iban} and Invoice Copy IBAN: {extracted_ibans} for Paramter Vendor Banking Details")

    if not(sap_iban) or not(extracted_ibans) or (sap_iban != extracted_ibans[0].strip().lower() if extracted_ibans else None):
        return build_validation_result(extracted_value={BANK_NAME: extracted_names,
                                                        BANK_ACCOUNT_NUMBER: extracted_accounts,
                                                        BANK_ACCOUNT_HOLDER_NAME: extracted_holders},
                                        is_anomaly=True,
                                        edit_operation=False,
                                        highlight=True,
                                        method="Automated",
                                        supporting_details=vendor_bank_info | {ESR_NUMBER: invoice_extracted_esr_number} | {IBAN: extracted_ibans} | {"Summary":"IBAN mismatch or missing - Anomaly detected"})

    # Anomaly based on ESR mismatch or ESR value is None
    control_list.pop(control_list.index(BANK_ACCOUNT_NUMBER)) if BANK_ACCOUNT_NUMBER in control_list else None

    log_message("ESR number match so validate other paramters for Anomaly_flag for Paramter Vendor Banking Details")
    result = validate_bank_info_with_vendor_dict(
        extracted_bank_infos=extracted_bank_infos,
        vendor_banking_dict=vendor_bank_info,
        control_list=control_list,
        partner_bank_type=partner_bank_type
    )
    return build_validation_result(
        extracted_value={BANK_NAME: extracted_names,
                         BANK_ACCOUNT_NUMBER: extracted_accounts,
                         BANK_ACCOUNT_HOLDER_NAME: extracted_holders},
        is_anomaly=result.get("is_anomaly"),
        edit_operation=False,
        highlight=True,
        method="Automated",
        supporting_details=vendor_bank_info | {ESR_NUMBER: invoice_extracted_esr_number} | {IBAN: extracted_ibans} | {"Summary":"IBAN match - Validated other bank details"}
    )

def india_vendcode_1590807_validation(vendor_account_history: str,
                                      vendor_bank_info: Dict,
                                      extracted_bank_infos: Dict,
                                      control_list: List,
                                      partner_bank_type: str
                                      ) -> Dict:
    """
    for India and vendor code-1590807,
    if no bank info then do validation based on
    vendor account history
    """
    # Get extracted values as lists
    extracted_accounts = get_list_values(extracted_bank_infos, BANK_ACCOUNT_NUMBER) if extracted_bank_infos and BANK_ACCOUNT_NUMBER in control_list else []
    extracted_holders = get_list_values(extracted_bank_infos, BANK_ACCOUNT_HOLDER_NAME) if extracted_bank_infos and BANK_ACCOUNT_HOLDER_NAME in control_list else []
    extracted_names = get_list_values(extracted_bank_infos, BANK_NAME) if extracted_bank_infos and BANK_NAME in control_list else []
    
    # Count extracted entries (max of all list lengths)
    extracted_count = max(len(extracted_accounts), len(extracted_holders), len(extracted_names)) if extracted_bank_infos else 0
    
    # Check if extracted bank info is empty
    has_extracted = extracted_count > 0

    if not has_extracted:
        log_message("Invoice Copy Doesn't have any Bank Info for Paramter Vendor Banking Details")
        if (vendor_account_history != "") or (vendor_account_history is not None):
            bank_name, bank_account_number, account_holder_name = get_bank_info_from_vendor_account_history(
                vendor_account_history=vendor_account_history,
                vendor_banking_dict=vendor_bank_info,
                control_list=control_list
            )
            
            # Check if any bank info was found from vendor account history
            has_bank_info = any([bank_name, bank_account_number, account_holder_name])
            
            if has_bank_info:
                log_message("Bank info found from vendor account history for Parameter Vendor Banking Details")
                return build_validation_result(
                    extracted_value={
                        BANK_NAME: bank_name,
                        BANK_ACCOUNT_NUMBER: bank_account_number,
                        BANK_ACCOUNT_HOLDER_NAME: account_holder_name
                    },
                    is_anomaly=False,
                    edit_operation=False,
                    highlight=True,
                    method="Automated",
                    supporting_details=vendor_bank_info | {"Summary":"Bank info retrieved from vendor account history - No anomaly detected"}
                )
            else:
                log_message("No bank info found from vendor account history for Parameter Vendor Banking Details")
                return build_validation_result(
                    extracted_value={
                        # BANK_NAME: None,
                        BANK_ACCOUNT_NUMBER: None,
                        BANK_ACCOUNT_HOLDER_NAME: None
                    },
                    is_anomaly=True,
                    edit_operation=False,
                    highlight=True,
                    method="Automated",
                    supporting_details=vendor_bank_info | {"Summary":"No bank info found in vendor account history - Anomaly detected"}
                )
            

    return validate_bank_info_with_vendor_dict(
        extracted_bank_infos=extracted_bank_infos,
        vendor_banking_dict=vendor_bank_info,
        control_list=control_list,
        partner_bank_type=partner_bank_type
    )