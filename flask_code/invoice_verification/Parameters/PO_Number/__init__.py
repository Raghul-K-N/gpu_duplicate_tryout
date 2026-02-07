# Parameters/PO number/__init__.py
"""
PO number Field Validator - Phase 1 Implementation
Regions: NAA, EMEAI, Global
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import PO_NUMBER


class PONumber:
    """
    PO number parameter validator
    Handles extraction value determination and anomaly validation
    """
    
    def __init__(self, invoice_ocr:OCRData, voucher_ocr:VoucherOCRData, sap_row:SAPRow, vendors_df=None, vat_df=None, gl_accounts_df=None):
        """
        Initialize validator with all required data
        Args:
            invoice_ocr: InvoiceOCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object with extracted voucher data
            sap_row: SAPRow object with SAP data and file lines
            vendors_df: Vendor master DataFrame (not used for PO number, kept for consistency)
            vat_df: VAT master DataFrame (not used for PO number, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for PO number, kept for consistency)
        """
        self.transaction_id = sap_row.transaction_id
        
        # SAP value for comparison
        self.sap_po_number = sap_row.po_number
        self.po_accounting_group = sap_row.po_accounting_group
        
        # OCR extracted values with fallback logic
        # Primary: Invoice OCR, Fallback: Voucher OCR
        invoice_po_number = getattr(invoice_ocr, PO_NUMBER) if isinstance(getattr(invoice_ocr, PO_NUMBER),list) else []
        voucher_po_number = getattr(voucher_ocr, PO_NUMBER) if getattr(voucher_ocr, PO_NUMBER) else []
        
        # Use invoice value if present, otherwise fall back to voucher value
        self.extracted_po_number = invoice_po_number if invoice_po_number!=[] else voucher_po_number

        log_message(f"Initialized PO Number validator for transaction_id: {sap_row.transaction_id}, \
                    SAP PO Number: {self.sap_po_number},\
                    invoice_extracted_po_number: {invoice_po_number}, voucher_extracted_po_number: {voucher_po_number}")


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
            log_message(f"Processing PO number for transaction: {self.transaction_id}")
            # Route to region-specific validation
            result = self._validate_po_number()
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in PO number validation: {str(e)}", error_logger=True)
            return build_validation_result(
                    extracted_value={PO_NUMBER: None},
                    is_anomaly=None,
                    edit_operation=None,
                    highlight=None,
                    method=None,
                    supporting_details={}
                )
    
    def _validate_po_number(self):
        """
        Validate extracted PO number against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        from .na import validate_naa_po_number
        return validate_naa_po_number(
            extracted_value=self.extracted_po_number,
            sap_value=self.sap_po_number,
            po_accounting_group=self.po_accounting_group
        )