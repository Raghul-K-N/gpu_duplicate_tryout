from typing import Dict, Optional, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import VAT_TAX_CODE, VAT_TAX_AMOUNT

class VatTaxCode:
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
        self.sap_vat_amount = sap_row.vat_amount
        
        # OCR extracted values with fallback logic (invoice primary, voucher fallback)
        invoice_vat_amount = getattr(invoice_ocr, VAT_TAX_AMOUNT, None)
        invoice_vat_tax_code = getattr(invoice_ocr, VAT_TAX_CODE, None)
        voucher_vat_amount = getattr(voucher_ocr, VAT_TAX_AMOUNT, None)
        voucher_vat_tax_code = getattr(voucher_ocr, VAT_TAX_CODE, None)
        
        # Use invoice values if present, otherwise fall back to voucher values
        self.extracted_vat_amount = invoice_vat_amount if not is_empty_value(invoice_vat_amount) else voucher_vat_amount
        self.extracted_vat_tax_code = invoice_vat_tax_code if not is_empty_value(invoice_vat_tax_code) else voucher_vat_tax_code
        
        # All text lines for regex-based detection (e.g., China invoice type)
        invoice_text_lines = getattr(invoice_ocr, 'all_text_lines', []) or []
        voucher_text_lines = getattr(voucher_ocr, 'all_text_lines', []) or []
        self.all_text_lines: List[str] = invoice_text_lines if invoice_text_lines else voucher_text_lines
        
        log_message(f"VAT extraction - Using: {'Invoice' if not is_empty_value(invoice_vat_tax_code) else 'Voucher'} OCR")

        log_message(f"Initialized VatTaxCode for transaction: {self.transaction_id}, region: {self.region},\
                     invoice_vat_amount: {invoice_vat_amount}, \
                        invoice_vat_tax_code: {invoice_vat_tax_code}, \
                        voucher_vat_amount: {voucher_vat_amount}, \
                        voucher_vat_tax_code: {voucher_vat_tax_code}")
        

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
            log_message(f"Processing Vat Tax Code for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_vat_tax_code()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Vat Tax Code validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={VAT_TAX_CODE:None,
                                                            VAT_TAX_AMOUNT:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)

    def _validate_vat_tax_code(self) -> Dict:
        """
        Validate extracted Vat Tax Code against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        
        from .na import validate_naa_vat_tax_code
        result = validate_naa_vat_tax_code(sap_tax_amount=self.sap_vat_amount,
                                           extracted_tax_amount=self.extracted_vat_amount)
        return result