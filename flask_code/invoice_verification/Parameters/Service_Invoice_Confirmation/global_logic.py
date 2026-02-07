"""
Global fallback validation for Service Invoice Confirmation
"""
from typing import Dict,List
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Parameters.Service_Invoice_Confirmation.utils import get_servie_approval
from invoice_verification.constant_field_names import SERVICE_INVOICE_CONFIRMATION

def validate_global_service_invoice_confirmation(
        po_types: List,
        item_categories: List,
        line_item_amount_values: List,
        vim_comment_lines: List,
        eml_lines: List,
        sap_invoice_number: str
    ) -> Dict:
    """
    Global validation logic for Service Invoice Confirmation.

    This validation applies only to:
            - PO types : ZSRV, ZMRO, ZLGS
            - Item category: D 
            - Line item amount > 5,000 USD

    The check looks for specific patterns in either:
            1. VIM comment lines
            2. Email lines

    Expected Format:
            Uxxxxx-datetypestring 

    Logic Flow:
        1. If PO type, item category, and amount criteria match:
            a. Search for service confirmation pattern in VIM comments.
            b. If not found, search in email lines.
        2. If found anomaly_flag = False
        3. If not found anomaly_flag = True
        4. If PO type/item category not applicable auto pass (no anomaly)
    """

    log_message("Running Global Service Invoice Confirmation Validation for Parameter Service Invoice Confirmation")

    try:
        amount_values = [float(line_item_amount) for line_item_amount in line_item_amount_values if line_item_amount is not None]
    except (ValueError, TypeError):
        amount_values = []

    applicable_po_types = ["ZSRV", "ZMRO", "ZLGS"]

    amount_condition = any(amount > 5000 for amount in amount_values)
    
    approval = get_servie_approval(eml_lines=eml_lines,
                                   vim_comment_lines=vim_comment_lines,
                                   sap_invoice_number=sap_invoice_number)

    if any((str(po_type).strip().upper() in applicable_po_types for po_type in po_types)) and \
        any((str(item_category).strip().upper() == "D" for item_category in item_categories)) and amount_condition:

        log_message(f"Service Invoice Confirmation Validation applicable for the transaction | PO: {po_types}, Item: {item_categories}, Amount: {amount_values} for Parameter Service Invoice Confirmation")
        
        if approval == True:
            log_message(f"Approval is True for Parameter Service Invoice Confirmation")
            return build_validation_result(
                extracted_value={SERVICE_INVOICE_CONFIRMATION:None},
                is_anomaly=False,
                edit_operation=False,
                highlight=False,
                method="Automated",
                supporting_details={"Summary":"Service Invoice Confirmation found in VIM comments or email"}
            )

        log_message("No matching service confirmation format found in VIM comments or email lines for Parameter Service Invoice Confirmation")
        return build_validation_result(
            extracted_value={SERVICE_INVOICE_CONFIRMATION:None},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"Summary":"No matching service confirmation format found in VIM comments or email"}
        )
    
    log_message("Validation not applicable for this PO type or amount threshold for Parameter Service Invoice Confirmation")
    return build_validation_result(
        extracted_value={SERVICE_INVOICE_CONFIRMATION:None},
        is_anomaly=False,
        edit_operation=False,
        highlight=False,
        method="Automated",
        supporting_details={"Summary":"Validation not applicable for this PO type or amount threshold."}
    )