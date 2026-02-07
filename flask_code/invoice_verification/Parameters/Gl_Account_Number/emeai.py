from typing import Dict, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow

def validate_emeai_gl_account_number(sap_row: SAPRow, 
                                invoice_extracted_gl_account_number: List,
                                voucher_extracted_gl_account_number: List
                                ) -> Dict:
    """
    EMEAI-specific GL Account Validation logic
    """    
    log_message("No EMEAI Region level Exceptions were met for Parameter GL account")
    from .global_logic import validate_global_gl_account_number
    return validate_global_gl_account_number(
        sap_row=sap_row,
        invoice_extracted_gl_account_number=invoice_extracted_gl_account_number,
        voucher_extracted_gl_account_number=voucher_extracted_gl_account_number
    )