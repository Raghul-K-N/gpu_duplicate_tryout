from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import TRANSACTION_TYPE

class TransactionType:
    """
    Transaction Type parameter validator
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
            vendors_df: Vendor master DataFrame (not used for Transaction Type, kept for consistency)
            vat_df: VAT master DataFrame (not used for Transaction Type, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for Transaction Type, kept for consistency)
        """
        # Extract key context fields
        self.region = (sap_row.region or "").upper()
        self.transaction_id = str(sap_row.transaction_id)
        self.sap_transaction_type = str(sap_row.transaction_type)
        self.invoice_extracted_transaction_type = getattr(invoice_ocr, TRANSACTION_TYPE, None)
        self.voucher_extracted_transaction_type = getattr(sap_row,"voucher_transaction_type")
        self.manual_transaction_type = getattr(sap_row,"manual_transaction_type", None)

        log_message(f"Initialized TransactionType for transaction: {self.transaction_id}, region: {self.region},\
                     sap_transaction_type: {self.sap_transaction_type}, \
                        invoice_extracted_transaction_type: {self.invoice_extracted_transaction_type}, \
                        voucher_extracted_transaction_type: {self.voucher_extracted_transaction_type}, \
                        manual_transaction_type: {self.manual_transaction_type}")
    
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
            log_message(f"Processing Transaction Type for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_transaction_type()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Transaction Type validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={TRANSACTION_TYPE: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_transaction_type(self) -> Dict:
        """
        Validate extracted Transaction Type against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        from .global_logic import validate_global_transaction_type
        return validate_global_transaction_type(sap_transaction_type=self.sap_transaction_type,
                                                manual_transaction_type=self.manual_transaction_type,
                                                invoice_extracted_transaction_type=self.invoice_extracted_transaction_type,
                                                voucher_extracted_transaction_type=self.voucher_extracted_transaction_type)