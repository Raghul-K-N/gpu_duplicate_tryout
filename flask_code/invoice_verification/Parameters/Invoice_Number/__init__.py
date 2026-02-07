# Parameters/Invoice Number/__init__.py
"""
Invoice Number Field Validator - Phase 1 Implementation
Regions: NAA, EMEAI, Global
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import INVOICE_NUMBER, STAMP_NUMBER


class InvoiceNumber:
    """
    Invoice Number parameter validator
    Handles extraction value determination and anomaly validation
    """
    
    def __init__(self, invoice_ocr:OCRData, voucher_ocr:VoucherOCRData, sap_row:SAPRow, vendors_df=None, vat_df=None, gl_accounts_df=None):
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
        # Store input data
        self.sap_row = sap_row
        # SAP value for comparison
        self.sap_invoice_number = sap_row.invoice_number
        
        # OCR extracted values with fallback logic
        # Primary: Invoice OCR, Fallback: Voucher OCR
        invoice_extracted_invoice_number = getattr(invoice_ocr, INVOICE_NUMBER, None)
        coversheet_extracted_invoice_number = getattr(voucher_ocr, INVOICE_NUMBER, None)
        
        # Use invoice value if present, otherwise fall back to voucher value
        self.extracted_invoice_number = invoice_extracted_invoice_number if not is_empty_value(invoice_extracted_invoice_number) else coversheet_extracted_invoice_number

        log_message(f"Initialized InvoiceNumber validator for transaction_id: {sap_row.transaction_id}, \
                    SAP Invoice Number: {self.sap_invoice_number},\
                    invoice_extracted_invoice_number: {invoice_extracted_invoice_number}, coversheet_extracted_invoice_number: {coversheet_extracted_invoice_number}")
        
        log_message(f"Invoice Number extraction - Using: {'Invoice' if not is_empty_value(invoice_extracted_invoice_number) else 'Cover sheet'} OCR")


    def main(self) -> dict:
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
            log_message(f"Processing Invoice Number for transaction: {self.sap_row.transaction_id}")
            # Route to region-specific validation
            result = self._validate_invoice_number()
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice Number validation: {str(e)}", error_logger=True)
            return build_validation_result(
                    extracted_value={INVOICE_NUMBER: None},
                    is_anomaly=None,
                    edit_operation=None,
                    highlight=None,
                    method=None,
                    supporting_details={}
                )
    
    def _validate_invoice_number(self):
        """
        Validate extracted invoice number against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        # Route to region-specific validation
        # if self.region in ['NAA', 'NA']:
        from .na import validate_naa_invoice_number
        return validate_naa_invoice_number(
            extracted_value=self.extracted_invoice_number,
            sap_row=self.sap_row
        )

