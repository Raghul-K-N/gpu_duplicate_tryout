from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.constant_field_names import INVOICE_AMOUNT, INVOICE_CURRENCY
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result

class InvoiceAmount:
    """
    Base class for region-specific handlers.
    """
    def __init__(self, invoice_ocr: OCRData, 
                voucher_ocr: VoucherOCRData, 
                sap_row: SAPRow, 
                vendors_df: Optional[pd.DataFrame]=None, 
                vat_df: Optional[pd.DataFrame]=None, 
                gl_accounts_df: Optional[pd.DataFrame]=None) -> None:
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
        self.region = str(sap_row.region).upper()
        self.transaction_id = sap_row.transaction_id
        self.sap_invoice_amount = str(sap_row.invoice_amount)
        self.invoice_extracted_invoice_amount = str(getattr(invoice_ocr, INVOICE_AMOUNT)) if getattr(invoice_ocr, INVOICE_AMOUNT, None) else None
        self.voucher_extracted_invoice_amount = str(getattr(voucher_ocr, INVOICE_AMOUNT)) if getattr(voucher_ocr, INVOICE_AMOUNT, None) else None
        self.invoice_extracted_invoice_currency = str(getattr(invoice_ocr, INVOICE_CURRENCY)) if getattr(invoice_ocr, INVOICE_CURRENCY, None) else None

        log_message(f"InvoiceAmount init - Region: {self.region},  Transaction ID: {self.transaction_id},\
                    SAP Invoice Amount: {self.sap_invoice_amount}, Invoice OCR Amount: {self.invoice_extracted_invoice_amount},\
                     Voucher OCR Amount: {self.voucher_extracted_invoice_amount},\
                    Invoice OCR Currency: {self.invoice_extracted_invoice_currency}")
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
            log_message(f"Processing Invoice amount for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will upamount self.extracted_value based on conditions
            result = self._validate_invoice_amount()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice amount validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={INVOICE_AMOUNT:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_invoice_amount(self) -> Dict:
        """
        Validate extracted invoice amount against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        
        from .na import validate_naa_invoice_amount
        result = validate_naa_invoice_amount(
                sap_invoice_amount=self.sap_invoice_amount,
                invoice_extracted_invoice_amount=self.invoice_extracted_invoice_amount,
                voucher_extracted_invoice_amount=self.voucher_extracted_invoice_amount
        )
        return result