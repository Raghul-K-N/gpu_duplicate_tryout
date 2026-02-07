from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import INVOICE_DATE, INVOICE_RECEIPT_DATE
from invoice_verification.Parameters.Invoice_Reciept_date.utils import get_payment_certificate_date

class InvoiceReceiptDate:
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
        self.region = str(sap_row.region).strip().upper()
        self.transaction_id = sap_row.transaction_id

        self.payment_certificate_receipt_date = get_payment_certificate_date(sap_row.payment_certificate_lines) if self.region in ["NAA", "NA"] else None
        self.invoice_extracted_receipt_date = getattr(invoice_ocr, INVOICE_RECEIPT_DATE, None)
        self.invoice_extracted_invoice_date = getattr(invoice_ocr, INVOICE_DATE, None)
        self.voucher_extracted_receipt_date = getattr(voucher_ocr, INVOICE_RECEIPT_DATE, None)

        log_message(f"Initialized InvoiceReceiptDate for transaction: {self.transaction_id}, region: {self.region},\
                     payment_certificate_receipt_date: {self.payment_certificate_receipt_date}, \
                     Invoice_extracted_invoice_date: {self.invoice_extracted_invoice_date}, \
                     invoice_extracted_receipt_date: {self.invoice_extracted_receipt_date}, \
                        voucher_extracted_receipt_date: {self.voucher_extracted_receipt_date}")        

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
            log_message(f"Processing Invoice receipt_date for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_invoice_receipt_date()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice receipt_date validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_invoice_receipt_date(self) -> Dict:
        """
        Validate extracted invoice receipt_date against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        
        if self.region in ["NAA","NA"]:
            from .na import validate_naa_invoice_receipt_date
            result = validate_naa_invoice_receipt_date(
                    sap_row=self.sap_row,
                    payment_certificate_receipt_date=self.payment_certificate_receipt_date,
                    invoice_extracted_invoice_date=self.invoice_extracted_invoice_date,
                    invoice_extracted_receipt_date=self.invoice_extracted_receipt_date,
                    voucher_extracted_receipt_date=self.voucher_extracted_receipt_date)
            return result

        elif self.region in ["EMEAI","EMEA"]:
            from .emeai import validate_emeai_invoice_receipt_date
            result = validate_emeai_invoice_receipt_date(
                    sap_row=self.sap_row,
                    payment_certificate_receipt_date=self.payment_certificate_receipt_date,
                    invoice_extracted_invoice_date=self.invoice_extracted_invoice_date,
                    invoice_extracted_receipt_date=self.invoice_extracted_receipt_date,
                    voucher_extracted_receipt_date=self.voucher_extracted_receipt_date)
            return result

        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)