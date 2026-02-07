# Parameters/DOA/__init__.py
"""
DOA (Delegation of Authority) Field Validator - Phase 1 Implementation
Regions: NAA, APAC, EMEAI, LATAM
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import DOA, GL_ACCOUNT_NUMBER
from typing import List

class Doa:
    """
    DOA parameter validator
    Validates delegation of authority based on approval type and thresholds
    No extraction - only anomaly detection
    """
    
    def __init__(self, invoice_ocr, voucher_ocr, sap_row: SAPRow, vendors_df=None, vat_df=None, gl_accounts_df=None):
        """
        Initialize validator with all required data
        
        Args:
            invoice_ocr: InvoiceOCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object with extracted voucher data
            sap_row: SAPRow object with SAP data and file lines
            vendors_df: Vendor master DataFrame (not used, kept for consistency)
            vat_df: VAT master DataFrame (not used, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used, kept for consistency)
        """
        # Store input data
        self.invoice_ocr = invoice_ocr
        self.voucher_ocr = voucher_ocr
        self.sap_row = sap_row
        self.vendors_df = vendors_df
        self.vat_df = vat_df
        self.gl_accounts_df = gl_accounts_df
        
        voucher_gls = getattr(voucher_ocr, GL_ACCOUNT_NUMBER, None)
        invoice_gls = getattr(invoice_ocr, GL_ACCOUNT_NUMBER, None)
        sap_gls = [str(gl).strip().upper() for gl in (sap_row.gl_account_number or []) if gl]
        
        # Exclusive priority: Voucher OCR first, else Invoice OCR, else SAP
        if voucher_gls:
            self.gl_accounts_list = voucher_gls
            self.gl_account_source = 'voucher_ocr'
        elif invoice_gls:
            self.gl_accounts_list = invoice_gls
            self.gl_account_source = 'invoice_ocr'
        else:
            self.gl_accounts_list = sap_gls
            self.gl_account_source = 'sap'
        
        log_message(f"DOA GL Accounts List: {self.gl_accounts_list} (Source: {self.gl_account_source})")
        
        self.region = sap_row.region.upper()
        # Get voucher text
        self.voucher_text = "\n".join(getattr(sap_row, "voucher_lines", []) or [])
    

    def main(self) -> dict:
        """
        Main method to validate DOA
        Returns:
            Dict with standardized output format
        """
        try:
            log_message(f"Processing DOA for transaction: {self.sap_row.transaction_id}")
            # Route to region-specific validation
            result = self._validate_doa()
            
            return result
            
        except Exception as e:
            log_message(f"Error in DOA validation: {str(e)}", error_logger=True)
            return build_validation_result(
                extracted_value={DOA: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=None,
                method=None,
                supporting_details={}
            )
            
    def _validate_doa(self) -> dict:
        """Route to region-specific validation logic"""
    
        if self.region in ['NAA', 'NA']:
            from .na import validate_naa_doa
            return validate_naa_doa(
                sap_row=self.sap_row,
                voucher_text=self.voucher_text,
                gl_accounts_list=self.gl_accounts_list
            )
        
        elif self.region in ['EMEAI', 'EMEA']:
            from .emeai import validate_emeai_doa
            return validate_emeai_doa(
                sap_row=self.sap_row,
                voucher_text=self.voucher_text,
                gl_accounts_list=self.gl_accounts_list
            )
        
        elif self.region in ['APAC']:
            from .apac import validate_apac_doa
            return validate_apac_doa(
                sap_row=self.sap_row,
                voucher_text=self.voucher_text,
                gl_accounts_list=self.gl_accounts_list
            )
        
        elif self.region in ['LATAM', 'LA', 'LAA']:
            from .la import validate_latam_doa
            return validate_latam_doa(
                sap_row=self.sap_row,
                voucher_text=self.voucher_text,
                gl_accounts_list=self.gl_accounts_list
            )
    
        else:
            raise ValueError(f"Unsupported region for DOA validation: {self.region}")