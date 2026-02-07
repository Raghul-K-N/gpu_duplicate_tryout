from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import TEXT_INFO

class TextInfo:
    """
    Text Info parameter validator
    Handles extraction value determination and anomaly validation
    """
    
    def __init__(self, invoice_ocr: OCRData, 
                voucher_ocr: VoucherOCRData, 
                sap_row: SAPRow, 
                vendors_df:Optional[pd.DataFrame]=None, 
                vat_df:Optional[pd.DataFrame]=None, 
                gl_accounts_df:Optional[pd.DataFrame]=None) -> None:
        """
        Initialize validator with all required data
        
        Args:
            invoice_ocr: InvoiceOCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object with extracted voucher data
            sap_row: SAPRow object with SAP data and file lines
            vendors_df: Vendor master DataFrame (not used for Text Info, kept for consistency)
            vat_df: VAT master DataFrame (not used for Text Info, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for Text Info, kept for consistency)
        """
        # Store input data
        self.text_field = str(sap_row.text_field)
        self.transaction_id = str(sap_row.transaction_id)
    
    def main(self) -> Dict:
        """
        Main method to calculate anomaly and determine value for IV Processing table
        
        Returns:
            Dict with standardized output format:
            {
                'extracted_value': Any,           # Value(s) to store in invoice_processing table
                'is_anomaly': bool,              # True=anomaly detected, False=no anomaly
                'edit_operation': bool,          # True=editable, False=automatic only
                'highlight': bool,               # True=highlight in UI
                'method': str,                   # 'Automated', 'Manual', 'Combined'
                'supporting_details': dict
            }
        """
        try:
            log_message(f"Processing Text Info for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_text_info()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Text Info validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={TEXT_INFO: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_text_info(self) -> Dict:
        """
        Validate extracted Text Info against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        from .global_logic import validate_global_text_info
        return validate_global_text_info(
            text_field = self.text_field
        )