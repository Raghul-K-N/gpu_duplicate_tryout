from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import GL_ACCOUNT_NUMBER

class GLAccountNumber:
    """
    Base class for region-specific handlers.
    """
    def __init__(self, invoice_ocr: OCRData, 
                voucher_ocr: VoucherOCRData, 
                sap_row: SAPRow, 
                vendors_df:Optional[pd.DataFrame] =None, 
                vat_df:Optional[pd.DataFrame] =None, 
                gl_accounts_df:Optional[pd.DataFrame] =None
                ) -> None:
        """
        Initialize validator with all required data
        
        Args:
            invoice_ocr: InvoiceOCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object with extracted voucher data
            sap_row: SAPRow object with SAP data and file lines
            vendors_df: DataFrame of vendor master data (if needed)
            vat_df: DataFrame of VAT tax codes (if needed)
            gl_accounts_df: DataFrame of GL accounts (if needed)
        """        
        # Extract key context fields
        self.sap_row = sap_row
        self.region = str(sap_row.region)
        self.transaction_id = str(sap_row.transaction_id).strip()

        log_message(f"GLAccountNumber init - Region: {self.region},  Transaction ID: {self.transaction_id}")
        
        # Ensure gl_account_number is always a list
        invoice_gl = getattr(invoice_ocr, "gl_account_number", None)
        if invoice_gl is None:
            self.invoice_extracted_gl_account_number = []
        elif isinstance(invoice_gl, list):
            self.invoice_extracted_gl_account_number = invoice_gl
        else:
            self.invoice_extracted_gl_account_number = [invoice_gl]
        
        voucher_gl = getattr(voucher_ocr, "gl_account_number", None)
        if voucher_gl is None:
            self.voucher_extracted_gl_account_number = []
        elif isinstance(voucher_gl, list):
            self.voucher_extracted_gl_account_number = voucher_gl
        else:
            self.voucher_extracted_gl_account_number = [voucher_gl]

        log_message(f"Extracted GL Account Numbers - Invoice OCR: {self.invoice_extracted_gl_account_number}, Voucher OCR: {self.voucher_extracted_gl_account_number}")
    
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
            log_message(f"Processing gl_account for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_gl_account()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in gl_account validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_gl_account(self) -> Dict:
        """
        Validate extracted gl_account against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        
        if self.region in ["NAA","NA"]:
            from .na import validate_naa_gl_account_number
            result = validate_naa_gl_account_number(
                    sap_row=self.sap_row,
                    invoice_extracted_gl_account_number=self.invoice_extracted_gl_account_number,
                    voucher_extracted_gl_account_number=self.voucher_extracted_gl_account_number
                )
            return result
        
        if self.region in ["EMEA","EMEAI"]:
            from .emeai import validate_emeai_gl_account_number
            result = validate_emeai_gl_account_number(
                    sap_row=self.sap_row,
                    invoice_extracted_gl_account_number=self.invoice_extracted_gl_account_number,
                    voucher_extracted_gl_account_number=self.voucher_extracted_gl_account_number
                )
            return result

        return build_validation_result(extracted_value={GL_ACCOUNT_NUMBER: None},
                                        is_anomaly=None,
                                        highlight=None,
                                        edit_operation=None,
                                        method=None,
                                        supporting_details=None)

