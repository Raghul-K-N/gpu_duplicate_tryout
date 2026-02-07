# Parameters/UDC/global_logic.py
# ============================================================================
"""
Global UDC validation logic (common for all regions)
Line-item level processing: Each line item has its own condition type and amount.
"""
from invoice_verification.constant_field_names import UDC
from invoice_verification.logger.logger import log_message
from .utils import (
    get_regional_threshold, is_credit_memo,
    extract_user_id_from_voucher,
    check_approval_status, lookup_diamond_data, extract_user_id_from_eml,
    get_doa_type_from_gl_accounts, load_diamond_data,
    get_doa_validation_type_from_condition
)
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.constants import CITI_BANK_VENDOR_CODES
from typing import Optional, List

def validate_global_udc(sap_row: SAPRow, voucher_text: str,
                       gl_accounts_list: Optional[List[str]] = None) -> dict:
    """
    Global UDC validation logic common to all regions
    Args:
        sap_row: SAPRow object with all SAP data
        voucher_text: Voucher OCR text
        gl_accounts_list: Combined list of GL accounts (priority: Voucher OCR -> Invoice OCR -> SAP)
    Returns:
        Dict with standardized output format
    """
    log_message("Global UDC validation")
    flat_list = [item for sublist in sap_row.eml_lines for item in sublist]
    eml_lines = " \n ".join(flat_list)
    method = 'Automated'
    highlight = True
    # Skip conditions
    if str(sap_row.vendor_code).strip().upper() in CITI_BANK_VENDOR_CODES and str(sap_row.doc_type).strip().upper() == 'KE':
        log_message("UDC: Citibank - skipping validation")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "Citibank vendor - UDC validation skipped"}
        )
    
    if sap_row.rental_aggrement_flag:
        log_message("UDC: Rental agreement flag set - skipping validation")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "Rental agreement - UDC validation skipped"}
        )
    
    
    if is_credit_memo(str(sap_row.transaction_type)):
        log_message("UDC: Credit memo - skipping validation")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "Credit memo - UDC validation skipped"}
        )
    
    # Determine method and highlight based on expense type
    if sap_row.doc_type== 'KE' and str(sap_row.dp_doc_type).startswith('NPO'):
        if sap_row.expense_type == 'EX':
            method = 'Combined'
            highlight = True
            log_message("UDC: KE document type with NPO - Combined method with highlight")
        else:
            method = 'Automated'
            highlight = True
            log_message("UDC: Standard document type - Automated method")
    
    # Initial threshold check
    threshold = get_regional_threshold(region=sap_row.region) if sap_row.voucher_copy_flag==False else 0.0
    log_message(f"UDC: Initial threshold check - Amount: {sap_row.invoice_amount}, Threshold: {threshold}")
    
    if float(sap_row.invoice_amount_usd) <= threshold:
        log_message("UDC: Invoice amount below threshold - no UDC processing needed")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "Invoice amount below threshold - no UDC processing needed"}
        )
    
    # Use pre-built GL accounts list (priority: Voucher OCR -> Invoice OCR -> SAP)
    gl_accounts_for_lookup = gl_accounts_list or []
    
    # Get DOA type from PRO_VR_DOA table using tenant_code and GL accounts (values like '001', '9CD')
    # This is used for Diamond data lookup
    doa_type = get_doa_type_from_gl_accounts(
        tenant_code=sap_row.tenant_code,
        gl_accounts=gl_accounts_for_lookup
    )

    # Get line item data (both are lists now)
    udc_amounts = sap_row.udc_amount_usd or []
    udc_condition_types = sap_row.udc_condition_type or []
    
    # Ensure lists are same length
    max_len = max(len(udc_amounts), len(udc_condition_types)) if udc_amounts or udc_condition_types else 0
    if len(udc_amounts) < max_len:
        udc_amounts = udc_amounts + [0.0] * (max_len - len(udc_amounts))
    if len(udc_condition_types) < max_len:
        udc_condition_types = udc_condition_types + [None] * (max_len - len(udc_condition_types))
    
    # Check if we have any line items to process (either amounts or condition types)
    if not udc_amounts and not udc_condition_types:
        log_message("UDC: No line items to process")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "No line items to process"}
        )
    
    # Pre-load diamond data for efficiency
    diamond_df = load_diamond_data()
    
    # Process each line item - only UDC validation types
    summary_lines = []
    is_anomaly = False
    has_udc_items = False
    
    for idx, (amount, condition_type) in enumerate(zip(udc_amounts, udc_condition_types)):
        line_num = idx + 1
        amount = float(amount) if amount else 0.0
        
        # If voucher is present, treat all line items as UDC
        if sap_row.voucher_copy_flag:
            doa_validation_type = 'UDC'
        else:
            doa_validation_type = get_doa_validation_type_from_condition(condition_type) if condition_type else None
        
        # Skip non-UDC line items (they are handled by DOA module)
        if doa_validation_type != 'UDC':
            log_message(f"UDC: Line item {line_num} - Skipped: validation type is '{doa_validation_type}', not 'UDC'")
            continue
        
        has_udc_items = True
        
        # Validate this UDC line item
        passed, reason = _validate_udc_line_item(
            sap_row=sap_row,
            amount=amount,
            threshold=threshold,
            voucher_text=voucher_text,
            eml_lines=eml_lines,
            doa_type=doa_type,
            diamond_df=diamond_df
        )
        
        # Handle result: None = skipped, True = passed, False = failed
        if passed is None:
            summary_lines.append(f"Line item {line_num} UDC Skipped, {reason}")
            log_message(f"UDC: Line item {line_num} - Skipped: {reason}")
        elif passed:
            summary_lines.append(f"Line item {line_num} UDC Passed")
            log_message(f"UDC: Line item {line_num} - Passed: {reason}")
        else:
            summary_lines.append(f"Line item {line_num} UDC Failed, {reason}")
            is_anomaly = True
            log_message(f"UDC: Line item {line_num} - Failed: {reason}")
    
    # If no UDC items found, skip validation
    if not has_udc_items:
        log_message("UDC: No UDC line items found")
        return build_validation_result(
            extracted_value={UDC: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details={"Summary": "No UDC line items found"}
        )
    
    # Build summary
    summary_text = "\n".join(summary_lines)
    
    return build_validation_result(
        extracted_value={UDC: None},
        is_anomaly=is_anomaly,
        edit_operation=False,
        highlight=highlight,
        method=method,
        supporting_details={"Summary": summary_text}
    )


def _validate_udc_line_item(sap_row: SAPRow, amount: float, threshold: float, voucher_text: str,
                            eml_lines: str, doa_type: str | None, diamond_df) -> tuple:
    """UDC validation for a single line item. Returns (passed: bool | None, reason: str)
    None = skipped (no UDC processing needed), True = passed, False = failed"""
    log_message(f"UDC: UDC validation for amount {amount}")
    
    # Amount threshold check - skip UDC processing if below threshold
    if amount <= threshold:
        return None, f"amount below regional threshold {threshold}"
    
    # Determine source and extract user ID
    user_id = None
    source = None
    
    if voucher_text:
        user_id = extract_user_id_from_voucher(voucher_text=voucher_text)
        source = 'voucher'

    # If voucher didn't yield a user_id, check VIM comments (use OpenText user id)
    if not user_id and sap_row.vim_comments:
        user_id = sap_row.opentext_user_id
        source = 'vim'

    # If still no user_id, fall back to EML lines
    if not user_id and sap_row.eml_lines:
        # user_id = extract_user_id_from_eml(eml_lines=sap_row.eml_lines)
        user_id = sap_row.opentext_user_id  # Use OpenText user id as fallback
        source = 'eml'
    
    if not user_id or not user_id.startswith('U'):
        return False, f"invalid user ID ({user_id})"
    
    # Check approval
    approved = check_approval_status(
        opentext_user_id=sap_row.opentext_user_id,
        app_action=sap_row.app_action,
        source=source,
        vim_comments=sap_row.vim_comments,
        eml_lines=eml_lines,
        sap_invoice_number=sap_row.invoice_number
    )
    
    if not approved:
        return False, "approval not present"
    
    # Check if DOA type from GL accounts is available for Diamond lookup
    if not doa_type:
        return False, "no DOA type for Diamond lookup"
    
    # Diamond data lookup using DOA type
    diamond_threshold = lookup_diamond_data(
        user_id=user_id,
        doa_type=doa_type,
        diamond_df=diamond_df
    )
    
    if diamond_threshold is None:
        return False, f"no Diamond threshold for DOA type {doa_type}"
    
    # Threshold validation
    if amount <= diamond_threshold:
        return True, f"within Diamond threshold {diamond_threshold}"
    else:
        return False, f"exceeds Diamond threshold {diamond_threshold}"
