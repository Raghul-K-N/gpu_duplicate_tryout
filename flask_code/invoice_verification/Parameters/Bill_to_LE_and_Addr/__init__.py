"""
Bill To Legal Entity and Address Field Validator
Routes to region-specific validation logic only (no global fallback)
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import LEGAL_ENTITY_NAME, LEGAL_ENTITY_ADDRESS, VENDOR_NAME, VENDOR_ADDRESS

class BillToLegalEntityAndAddress:
    """
    Bill To Legal Entity and Address parameter validator
    Handles legal entity name and address validation based on region-specific rules
    """
    
    def __init__(self, invoice_ocr: OCRData, voucher_ocr: VoucherOCRData, sap_row: SAPRow, 
                        vendors_df=None, vat_df=None, gl_accounts_df=None):
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
        
        # Extract key context fields
        self.region = sap_row.region.upper()
        self.company_code = sap_row.company_code
        self.doc_type = sap_row.doc_type
        self.dp_doc_type = sap_row.dp_doc_type
        self.vendor_code = sap_row.vendor_code
        # SAP values
        self.sap_legal_entity_name = sap_row.legal_entity_name
        self.sap_legal_entity_address = sap_row.legal_entity_address

        log_message(f"BilltoLegalEntityAndAddress init -  SAP data - Region: {self.region}, Companycode: {self.company_code},\
                    Doc Type: {self.doc_type}, DP Doc Type: {self.dp_doc_type}, Vendor Code: {self.vendor_code},\
                     LE Name: {self.sap_legal_entity_name}, LE Address: {self.sap_legal_entity_address}")
        
        # OCR extracted values with fallback logic
        # Primary: Invoice OCR, Fallback: Voucher OCR
        invoice_le_name = getattr(invoice_ocr, LEGAL_ENTITY_NAME, None)
        invoice_le_address = getattr(invoice_ocr, LEGAL_ENTITY_ADDRESS, None)
        voucher_le_name = getattr(voucher_ocr, LEGAL_ENTITY_NAME, None)
        voucher_le_address = getattr(voucher_ocr, LEGAL_ENTITY_ADDRESS, None)

        log_message(f"Invoice data - LE Name: {invoice_le_name}, LE Address: {invoice_le_address}")
        log_message(f"Voucher data - LE Name: {voucher_le_name}, LE Address: {voucher_le_address}")
        
        # Use invoice values if present, otherwise fall back to voucher values
        self.extracted_legal_entity_name = invoice_le_name if not is_empty_value(invoice_le_name) else voucher_le_name
        self.extracted_legal_entity_address = invoice_le_address if not is_empty_value(invoice_le_address) else voucher_le_address
        
        log_message(f"Final values - LE Name: {self.extracted_legal_entity_name}, LE Address: {self.extracted_legal_entity_address}")
        
        # Remit to information (vendor name/address)
        invoice_remit_name = getattr(invoice_ocr, VENDOR_NAME, None)
        invoice_remit_address = getattr(invoice_ocr, VENDOR_ADDRESS, None)
        voucher_remit_name = getattr(voucher_ocr, VENDOR_NAME, None)
        voucher_remit_address = getattr(voucher_ocr, VENDOR_ADDRESS, None)

        log_message(f"Invoice data - Remit To Name: {invoice_remit_name}, Remit To Address: {invoice_remit_address}")
        log_message(f"Voucher data - Remit To Name: {voucher_remit_name}, Remit To Address: {voucher_remit_address}")
        
        self.extracted_remit_to_name = invoice_remit_name if not is_empty_value(invoice_remit_name) else voucher_remit_name
        self.extracted_remit_to_address = invoice_remit_address if not is_empty_value(invoice_remit_address) else voucher_remit_address
        
        log_message(f"Final values - Remit To Name: {self.extracted_remit_to_name}, Remit To Address: {self.extracted_remit_to_address}")
        # File data from SAP row
        self.excel_path = sap_row.excel_paths
        flat_list = [item for sublist in self.sap_row.eml_lines for item in sublist]
        final_string = " ".join(flat_list)
        self.eml_lines = [final_string]
        
    
    def main(self):
        """Main validation method - routes to region-specific logic"""
        try:
            log_message(f"Bill To Legal Entity validation - Region: {self.region}, Transaction: {self.sap_row.transaction_id}")
            return self._validate_legal_entity()
        except Exception as e:
            log_message(f"Error in Bill To Legal Entity validation: {str(e)}", error_logger=True)
            return build_validation_result(
                extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=None,
                method=None,
                supporting_details={}
            )
    
    def _validate_legal_entity(self):
        """Route to region-specific validation - no global fallback"""
        
        if self.region in ['NAA', 'NA']:
            from .na import validate_naa_legal_entity
            return validate_naa_legal_entity(
                eml_lines=self.eml_lines,
                extracted_name=self.extracted_legal_entity_name,
                extracted_address=self.extracted_legal_entity_address,
                sap_row=self.sap_row,
                extracted_remit_to_name=self.extracted_remit_to_name,
                extracted_remit_to_address=self.extracted_remit_to_address
            )
        
        elif self.region in ['EMEAI', 'EMEA']:
            from .emeai import validate_emeai_legal_entity
            return validate_emeai_legal_entity(
                eml_lines=self.eml_lines,
                extracted_name=self.extracted_legal_entity_name,
                extracted_address=self.extracted_legal_entity_address,
                sap_row=self.sap_row,
                extracted_remit_to_name=self.extracted_remit_to_name,
                extracted_remit_to_address=self.extracted_remit_to_address
            )
        
        # elif self.region in ['APAC', 'AP']:
        #     from .apac import validate_apac_legal_entity
        #     return validate_apac_legal_entity(
        #         eml_lines=self.eml_lines,
        #         extracted_name=self.extracted_legal_entity_name,
        #         extracted_address=self.extracted_legal_entity_address,
        #         sap_row=self.sap_row,
        #         extracted_remit_to_name=self.extracted_remit_to_name,
        #         extracted_remit_to_address=self.extracted_remit_to_address
        #     )
        
        # elif self.region in ['LATAM', 'LA', 'LAA']:
        #     from .la import validate_latam_legal_entity
        #     return validate_latam_legal_entity(
        #         eml_lines=self.eml_lines,
        #         extracted_name=self.extracted_legal_entity_name,
        #         extracted_address=self.extracted_legal_entity_address,
        #         sap_row=self.sap_row,
        #         extracted_remit_to_name=self.extracted_remit_to_name,
        #         extracted_remit_to_address=self.extracted_remit_to_address
        #     )
        
        else:
            log_message(f"Unknown region: {self.region}, returning None", error_logger=True)
            return build_validation_result(
                extracted_value={LEGAL_ENTITY_NAME: None, LEGAL_ENTITY_ADDRESS: None},
                is_anomaly=None,
                edit_operation=None,
                highlight=None,
                method=None,
                supporting_details={}
            )