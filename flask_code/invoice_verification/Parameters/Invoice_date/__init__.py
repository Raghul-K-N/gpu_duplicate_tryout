from typing import Dict, Optional
from invoice_verification.Parameters.Invoice_date.na import validate_naa_invoice_date
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_DATE


class InvoiceDate:
    """
    Invoice Date parameter validator
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
            vendors_df: Vendor master DataFrame (not used for invoice Date, kept for consistency)
            vat_df: VAT master DataFrame (not used for invoice Date, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for invoice Date, kept for consistency)
        """
        # Extract key context fields
        self.transaction_id = sap_row.transaction_id
        self.region = (sap_row.region or "").upper()
        self.sap_invoice_date = sap_row.invoice_date
        self.invoice_extracted_invoice_date = str(getattr(invoice_ocr, INVOICE_DATE, None)) if getattr(invoice_ocr, INVOICE_DATE, None) else None
        self.voucher_extracted_invoice_date = str(getattr(voucher_ocr, INVOICE_DATE, None)) if getattr(voucher_ocr, INVOICE_DATE, None) else None

        log_message(f"Invoice Date Init - : {self.transaction_id}, region: {self.region},\
                    invoice_extracted_invoice_date: {self.invoice_extracted_invoice_date},\
                    voucher_extracted_invoice_date: {self.voucher_extracted_invoice_date}")
    
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
            log_message(f"Processing Invoice Date for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_invoice_date()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice Date validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={INVOICE_DATE:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_invoice_date(self) -> Dict:
        """
        Validate extracted invoice Date against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        from .na import validate_naa_invoice_date
        result = validate_naa_invoice_date(
                sap_invoice_date=self.sap_invoice_date,
                invoice_extracted_invoice_date=self.invoice_extracted_invoice_date,
                voucher_extracted_invoice_date=self.voucher_extracted_invoice_date
        )
        return result