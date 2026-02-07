# Parameters/Service Invoice Confirmation/__init__.py
"""
Service Invoice Confirmation Validator 
Regions: Global
"""
from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.constant_field_names import SERVICE_INVOICE_CONFIRMATION
import pandas as pd

class ServiceInvoiceConfirmation:
    """
    Service Invoice Confirmation parameter validator
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
        self.transaction_id = str(sap_row.transaction_id)
        self.sap_item_category = sap_row.item_category
        self.sap_po_type = sap_row.po_type
        self.line_item_amount_values = sap_row.line_item_amount_usd
        self.vim_comment_lines = sap_row.vim_comment_lines
        self.eml_lines = sap_row.eml_lines
        self.sap_invoice_number = str(sap_row.invoice_number).strip()

        if not isinstance(self.sap_po_type, list):
            self.sap_po_type = [self.sap_po_type]

        log_message(f"Initialized ServiceInvoiceConfirmation for transaction: {self.transaction_id}, \
                     sap_po_type: {self.sap_po_type}, \
                        sap_item_category: {self.sap_item_category}, \
                        line_item_amount_values: {self.line_item_amount_values}")
            
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
                'supporting_details': dict       # Additional data (e.g., stamp_number)
            }
        """
        try:
            log_message(f"Processing Service Invoice Confirmation for transaction: {self.transaction_id}")
            
            result = self._validate_service_invoice_confirmation()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Service Invoice Confirmation validation: {str(e)}", error_logger=True)
            import traceback
            print(traceback.format_exc())
            return build_validation_result(
                extracted_value= {SERVICE_INVOICE_CONFIRMATION:None},
                is_anomaly=None,
                edit_operation=None,
                highlight=None,
                method=None,
                supporting_details=None
            )
    
    def _validate_service_invoice_confirmation(self) -> Dict:
        """
        Validate Service Invoice Confirmation 
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        from .global_logic import validate_global_service_invoice_confirmation
        return validate_global_service_invoice_confirmation(
                po_types = self.sap_po_type,
                item_categories = self.sap_item_category,
                line_item_amount_values = self.line_item_amount_values,
                vim_comment_lines = self.vim_comment_lines,
                eml_lines = self.eml_lines,
                sap_invoice_number=self.sap_invoice_number
        )
