# Parameters/Payment Terms/__init__.py
"""
Payment Terms Validator 
Regions: Global
"""
from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.constant_field_names import PAYMENT_TERMS
import pandas as pd

class PaymentTerms:
    """
    Payment Terms parameter validator
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

        # SAP values for checks
        self.sap_row = sap_row
        self.region = sap_row.region if sap_row else None
        self.sap_transaction_id = sap_row.transaction_id if sap_row else None

        # Extracted Value
        self.invoice_extracted_payment_terms = str(getattr(invoice_ocr, PAYMENT_TERMS, None)) if getattr(invoice_ocr, PAYMENT_TERMS, None) else None
        self.voucher_extracted_payment_terms = str(getattr(voucher_ocr, PAYMENT_TERMS, None)) if getattr(voucher_ocr, PAYMENT_TERMS, None) else None

        # Vendor Dataframe
        self.vendors_df = vendors_df if (vendors_df is not None and not vendors_df.empty) else None

        log_message(f"Initialized PaymentTerms for transaction: {self.sap_transaction_id}, region: {self.region},\
                     invoice_extracted_payment_terms: {self.invoice_extracted_payment_terms}, \
                        voucher_extracted_payment_terms: {self.voucher_extracted_payment_terms}")
        

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
            log_message(f"Processing Payment Terms for transaction: {self.sap_transaction_id}")
            
            result = self._validate_payment_terms()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Payment Terms validation: {str(e)}", error_logger=True)
            return build_validation_result(
                extracted_value= {PAYMENT_TERMS: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=None,
                method=None,
                supporting_details=None
            )
    
    def _validate_payment_terms(self):
        """
        Validate Payment Terms 
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        if self.region and self.region.upper() == 'NAA':
            from .na import validate_naa_payment_terms
            return validate_naa_payment_terms(
                sap_row=self.sap_row,
                invoice_extracted_payment_terms = self.invoice_extracted_payment_terms,
                voucher_extracted_payment_terms = self.voucher_extracted_payment_terms,
                vendors_df=self.vendors_df
            )
        else:
            from .global_logic import validate_global_payment_terms
            return validate_global_payment_terms(
                sap_row=self.sap_row,
                invoice_extracted_payment_terms = self.invoice_extracted_payment_terms,
                voucher_extracted_payment_terms = self.voucher_extracted_payment_terms,
                vendors_df=self.vendors_df
            )
        return build_validation_result(
            extracted_value= {PAYMENT_TERMS: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=None,
            method=None,
            supporting_details=None
        )