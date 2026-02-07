# Parameters/Vendor name & Remmit to Addr/__init__.py
"""
Vendor Name and Remit To Address Field Validator - Phase 1 Implementation
Regions: NAA, APAC, EMEAI, LATAM (partial), Global
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.constant_field_names import VENDOR_NAME, VENDOR_ADDRESS

class VendorNameAndRemitToAddress:
    """
    Vendor Name and Remit To Address parameter validator
    Handles both vendor name and address validation based on region-specific rules
    """
    
    def __init__(self, invoice_ocr: OCRData, voucher_ocr: VoucherOCRData, sap_row: SAPRow, 
                                            vendors_df=None, vat_df=None, gl_accounts_df=None):
        """
        Initialize validator with all required data
        Args:
            invoice_ocr: InvoiceOCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object with extracted voucher data
            sap_row: SAPRow object with SAP data and file lines
            vendors_df: Vendor master DataFrame (required for payee address lookup)
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
        
        # Extract key context fields
        self.region = sap_row.region.upper()
        self.company_code = sap_row.company_code
        self.vendor_code = sap_row.vendor_code
        self.payment_method = sap_row.payment_method
        self.vendor_tax_id = sap_row.vendor_tax_id
        self.legal_entity_address = sap_row.legal_entity_address
        
        # SAP values for comparison
        self.sap_vendor_name = sap_row.vendor_name
        self.sap_vendor_address = sap_row.vendor_address
        
        # OCR extracted values with fallback logic
        # Primary: Invoice OCR, Fallback: Voucher OCR
        invoice_vendor_name = getattr(invoice_ocr, VENDOR_NAME, None)
        invoice_vendor_address = getattr(invoice_ocr, VENDOR_ADDRESS, None)
        voucher_vendor_name = getattr(voucher_ocr, VENDOR_NAME, None)
        voucher_vendor_address = getattr(voucher_ocr, VENDOR_ADDRESS, None)
        
        # Use invoice values if present, otherwise fall back to voucher values
        self.extracted_vendor_name = invoice_vendor_name if not is_empty_value(invoice_vendor_name) else voucher_vendor_name
        self.extracted_vendor_address = invoice_vendor_address if not is_empty_value(invoice_vendor_address) else voucher_vendor_address
        

         
        # Bank account details from invoice OCR
        self.bank_account_number = getattr(invoice_ocr, 'bank_account_number', None)
        self.bank_account_holder_name = getattr(invoice_ocr, 'bank_account_holder_name', None)

        log_message(f"Initialized VendorNameAndRemitToAddress for transaction: {self.sap_row.transaction_id}, region: {self.region},\
                     company_code: {self.company_code}, vendor_code: {self.vendor_code}, \
                        payment_method: {self.payment_method}, vendor_tax_id: {self.vendor_tax_id}, \
                        legal_entity_address: {self.legal_entity_address}, \
                        sap_vendor_name: {self.sap_vendor_name}, sap_vendor_address: {self.sap_vendor_address}, \
                        extracted_vendor_name: {self.extracted_vendor_name}, extracted_vendor_address: {self.extracted_vendor_address}, \
                        bank_account_number: {self.bank_account_number}, bank_account_holder_name: {self.bank_account_holder_name}")
        
        
    def main(self):
        """
        Main method to validate vendor name and remit to address
        """
        try:
            log_message(f"Processing Vendor Name and Address for transaction: {self.sap_row.transaction_id}")
            
            # Route to region-specific validation
            result = self._validate_vendor_info()
            
            return result
            
        except Exception as e:
            log_message(f"Error in Vendor Name and Address validation: {str(e)}", error_logger=True)
            return build_validation_result(
                extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
                is_anomaly= None,
                edit_operation= None,
                highlight= None,
                method=None,
                supporting_details= {}
            )
    
    def _validate_vendor_info(self):
        """Route to region-specific vendor validation logic"""
        log_message(f"Routing to region-specific validation for region: {self.region}")
        
        if self.region in ['NAA', 'NA']:
            from .na import validate_naa_vendor_info
            return validate_naa_vendor_info(
                extracted_vendor_name=self.extracted_vendor_name,
                extracted_vendor_address=self.extracted_vendor_address,
                sap_row=self.sap_row,
                vendors_df=self.vendors_df
            )
        
        elif self.region in ['EMEAI', 'EMEA']:
            from .emeai import validate_emeai_vendor_info
            return validate_emeai_vendor_info(
                extracted_vendor_name=self.extracted_vendor_name,
                extracted_vendor_address=self.extracted_vendor_address,
                sap_row=self.sap_row
            )
        
        # elif self.region in ['APAC']:
        #     from .apac import validate_apac_vendor_info
        #     return validate_apac_vendor_info(
        #         extracted_vendor_name=self.extracted_vendor_name,
        #         extracted_vendor_address=self.extracted_vendor_address,
        #         sap_vendor_name=self.sap_vendor_name,
        #         sap_vendor_address=self.sap_vendor_address,
        #         payment_method=self.payment_method,
        #         vendor_code=self.vendor_code,
        #         country=self.country,
        #         bank_account_number=self.bank_account_number,
        #         bank_account_holder_name=self.bank_account_holder_name,
        #         vendor_tax_id=self.vendor_tax_id
        #     )
        
        # elif self.region in ['LATAM', 'LA', 'LAA']:
        #     from .la import validate_latam_vendor_info
        #     return validate_latam_vendor_info(
        #         extracted_vendor_name=self.extracted_vendor_name,
        #         extracted_vendor_address=self.extracted_vendor_address,
        #         sap_vendor_name=self.sap_vendor_name,
        #         sap_vendor_address=self.sap_vendor_address,
        #         vendor_code=self.vendor_code,
        #         country=self.country
        #     )