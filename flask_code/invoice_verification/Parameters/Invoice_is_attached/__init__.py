from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.constant_field_names import INVOICE_IS_ATTACHED
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result

class InvoiceIsAttached:
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
        self.sap_row = sap_row

        # Extract key context fields
        self.region = str(sap_row.region).upper()
        self.transaction_id = sap_row.transaction_id

        log_message(f"Invoice Is Attached Init - : {self.transaction_id}, region: {self.region}")

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
            log_message(f"Processing Invoice is attached for transaction: {self.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_invoice_is_attached()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Invoice is Attached validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_invoice_is_attached(self) -> Dict:
        """
        Validate extracted invoice is attached against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """
        
        if self.region in ["NAA","NA"]:
            from .na import validate_naa_invoice_is_attached
            result = validate_naa_invoice_is_attached(sap_row = self.sap_row)
            return result

        elif self.region in ["EMEAI","EMEA"]:
            from .emeai import validate_emeai_invoice_is_attached
            result = validate_emeai_invoice_is_attached(sap_row = self.sap_row)
            return result

        return build_validation_result(extracted_value={INVOICE_IS_ATTACHED:None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)