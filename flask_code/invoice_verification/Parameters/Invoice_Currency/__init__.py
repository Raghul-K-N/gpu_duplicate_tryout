# Parameters/Invoice Currency/__init__.py
"""
Invoice Currency Validator 
Regions: Global
"""
from typing import Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_CURRENCY

class InvoiceCurrency:
    """
    Invoice Currency parameter validator
    Handles extraction value determination and anomaly validation
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
            vendors_df: Vendor master DataFrame (not used for invoice number, kept for consistency)
            vat_df: VAT master DataFrame (not used for invoice number, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for invoice number, kept for consistency)
        """
        # SAP value for comparison
        self.sap_po_currency = [str(po_currency).strip() for po_currency in sap_row.po_currency if po_currency]
        self.transaction_id = str(sap_row.transaction_id)
        
        # Initialize extracted value from OCR (can be None for manual/combined cases)
        # Handle invoice_currency from invoice OCR - can be list or string
        invoice_currency_raw = getattr(invoice_ocr, INVOICE_CURRENCY, [])
        self.invoice_extracted_invoice_currency = (
            invoice_currency_raw if isinstance(invoice_currency_raw, list) 
            else [invoice_currency_raw] if invoice_currency_raw else []
        )
        
        # Handle invoice_currency from voucher OCR - can be list or string
        voucher_currency_raw = getattr(voucher_ocr, INVOICE_CURRENCY, [])
        self.voucher_extracted_invoice_currency = (
            voucher_currency_raw if isinstance(voucher_currency_raw, list)
            else [voucher_currency_raw] if voucher_currency_raw else []
        )
        log_message(f"InvoiceCurrency init -  Transaction ID: {self.transaction_id}")
        log_message(f"Invoice currency extracted from invoice OCR: {self.invoice_extracted_invoice_currency}")
        log_message(f"Invoice currency extracted from voucher OCR: {self.voucher_extracted_invoice_currency}")
        
    
    def main(self):
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
                'supporting_details': dict       # Additional data (e.g., stamp_number)
            }
        """
        try:
            log_message(f"Processing Invoice Currency for transaction: {self.transaction_id}")
            
            result = self._validate_invoice_currency()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice Currency validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={INVOICE_CURRENCY:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_invoice_currency(self):
        """
        Validate extracted invoice currency against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        from .na import validate_naa_invoice_currency
        result = validate_naa_invoice_currency(
                sap_po_currency=self.sap_po_currency,
                invoice_extracted_invoice_currency=self.invoice_extracted_invoice_currency,
                voucher_extracted_invoice_currency=self.voucher_extracted_invoice_currency
        )
        return result