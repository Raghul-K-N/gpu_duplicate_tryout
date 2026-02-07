from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import PAYMENT_METHOD, INVOICE_CURRENCY

class PaymentMethod:
    """
    Payment Method parameter validator
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
            vendors_df: Vendor master DataFrame (not used for Payment Method, kept for consistency)
            vat_df: VAT master DataFrame (not used for Payment Method, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for Payment Method, kept for consistency)
        """
        self.sap_row = sap_row

        # Extract key context fields
        self.region = (sap_row.region or "").upper()
        self.transaction_id = str(sap_row.transaction_id)
        self.sap_payment_method = str(sap_row.payment_method)
        self.company_code = str(sap_row.company_code)
        self.tenant_code = str(sap_row.tenant_code)
        self.invoice_extracted_payment_method = str(getattr(invoice_ocr, PAYMENT_METHOD, None)) if getattr(invoice_ocr, PAYMENT_METHOD, None) else None
        self.invoice_extracted_invoice_currency = str(getattr(invoice_ocr, INVOICE_CURRENCY, None)) if getattr(invoice_ocr, INVOICE_CURRENCY, None) else None
        # self.voucher_extracted_payment_method = str(sap_row.voucher_payment_method).strip()
        self.voucher_extracted_payment_method = str(getattr(sap_row, PAYMENT_METHOD, None)).strip() if getattr(sap_row, PAYMENT_METHOD, None) else None
        self.voucher_extracted_invoice_currency = str(getattr(voucher_ocr, INVOICE_CURRENCY, None)) if getattr(voucher_ocr, INVOICE_CURRENCY, None) else None

        log_message(f"Initialized PaymentMethod for transaction: {self.transaction_id}, region: {self.region},\
                     sap_payment_method: {self.sap_payment_method}, \
                     company_code: {self.company_code}, tenant_code: {self.tenant_code}, \
                        invoice_extracted_payment_method: {self.invoice_extracted_payment_method}, \
                        invoice_extracted_invoice_currency: {self.invoice_extracted_invoice_currency}, \
                        voucher_extracted_payment_method: {self.voucher_extracted_payment_method}, \
                        voucher_extracted_invoice_currency: {self.voucher_extracted_invoice_currency}")
        if self.invoice_extracted_payment_method is None and self.voucher_extracted_payment_method is not None:
            self.invoice_extracted_payment_method = self.voucher_extracted_payment_method

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
            log_message(f"Processing Payment Method for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_payment_method()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Payment Method validation: {str(e)}", error_logger=True)
            import traceback
            print(traceback.format_exc())
            return build_validation_result(extracted_value={PAYMENT_METHOD: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_payment_method(self) -> Dict:
        """
        Validate extracted Payment Method against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        if str(self.region).strip().upper() in ["NAA", "NA"]:
            from .na import validate_naa_payment_method
            result = validate_naa_payment_method(sap_row=self.sap_row,
                                                invoice_extracted_invoice_currency=self.invoice_extracted_invoice_currency,
                                                invoice_extracted_payment_method=self.invoice_extracted_payment_method,
                                                voucher_extracted_invoice_currency=self.voucher_extracted_invoice_currency,
                                                voucher_extracted_payment_method=self.voucher_extracted_payment_method)
            return result
        
        if str(self.region).strip().upper() in ["EMEAI", "EMEA"]:
            from .emeai import validate_emeai_payment_method
            result = validate_emeai_payment_method(sap_row=self.sap_row,
                                                    invoice_extracted_payment_method=self.invoice_extracted_payment_method,
                                                    voucher_extracted_payment_method=self.voucher_extracted_payment_method)
            
            return result
        
        return build_validation_result(extracted_value={PAYMENT_METHOD: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)