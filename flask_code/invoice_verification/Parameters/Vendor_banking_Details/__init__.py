from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
import pandas as pd
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import BANK_NAME, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, ESR_NUMBER, PARTNER_BANK_TYPE, IBAN, SWIFT_CODE

class VendorBankingDetails:
    """
    Vendor Banking Details parameter validator
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
            vendors_df: Vendor master DataFrame (not used for Vendor Banking Details, kept for consistency)
            vat_df: VAT master DataFrame (not used for Vendor Banking Details, kept for consistency)
            gl_accounts_df: GL accounts DataFrame (not used for Vendor Banking Details, kept for consistency)
        """

        # Store input data
        self.sap_row = sap_row

        # Build vendor banking details dictionary from vendor_df filtered by vendor code
        self.vendor_banking_dict = {}
        if vendors_df is not None and self.sap_row.vendor_code:
            vendors_df['vendor_code'] = vendors_df['vendor_code'].astype(str)
            sap_vendor_code_str = str(self.sap_row.vendor_code)
            filtered_df = vendors_df[vendors_df['vendor_code'] == sap_vendor_code_str].drop_duplicates(
                subset=[BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, PARTNER_BANK_TYPE]
            )
            self.vendor_banking_dict = {
            BANK_ACCOUNT_NUMBER: filtered_df[BANK_ACCOUNT_NUMBER].tolist(),
            BANK_ACCOUNT_HOLDER_NAME: filtered_df[BANK_ACCOUNT_HOLDER_NAME].tolist(),
            PARTNER_BANK_TYPE: filtered_df[PARTNER_BANK_TYPE].tolist(),
            SWIFT_CODE: filtered_df[SWIFT_CODE].tolist(),
            IBAN: filtered_df[IBAN].tolist()
            }
        log_message(f"Vendor Banking Dict: {self.vendor_banking_dict}")

        self.extracted_banking_details = {}
        
        # Safely get attributes with default None if not present
        bank_account_number = getattr(invoice_ocr, BANK_ACCOUNT_NUMBER, None) or getattr(voucher_ocr, BANK_ACCOUNT_NUMBER, None)
        bank_account_holder_name = getattr(invoice_ocr, BANK_ACCOUNT_HOLDER_NAME, None) or getattr(voucher_ocr, BANK_ACCOUNT_HOLDER_NAME, None)
        esr_number = getattr(invoice_ocr, ESR_NUMBER, None) or getattr(voucher_ocr, ESR_NUMBER, None)
        bank_name = getattr(invoice_ocr, BANK_NAME, None) or getattr(voucher_ocr, BANK_NAME, None)
        iban = getattr(invoice_ocr, IBAN, None) or getattr(voucher_ocr, IBAN, None)
        
        self.extracted_banking_details = {
            BANK_ACCOUNT_NUMBER: bank_account_number,
            BANK_ACCOUNT_HOLDER_NAME: bank_account_holder_name,
            ESR_NUMBER: esr_number,
            # BANK_NAME: bank_name,
            IBAN: iban
        }
        
        self.control_list = [BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_HOLDER_NAME, IBAN]
        # Extract key context fields
        self.region = (sap_row.region or "").upper()

        log_message(f"Initialized VendorBankingDetails for transaction: {self.sap_row.transaction_id}, region: {self.region},\
                     extracted_banking_details: {self.extracted_banking_details}")
    
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
            log_message(f"Processing Vendor Banking Details for transaction: {self.sap_row.transaction_id}")
            
            # Route to region-specific validation
            # Validation will update self.extracted_value based on conditions
            result = self._validate_vendor_banking_details()
            
            # Return standardized output
            return result
            
        except Exception as e:
            log_message(f"Error in Vendor Banking Details validation: {str(e)}", error_logger=True)
            return build_validation_result(extracted_value={
                                                            # BANK_NAME: None,
                                                            BANK_ACCOUNT_NUMBER: None,
                                                            BANK_ACCOUNT_HOLDER_NAME: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)
    
    def _validate_vendor_banking_details(self) -> Dict:
        """
        Validate extracted Vendor Banking Details against SAP value
        Routes to region-specific validation logic
        Updates self.extracted_value as needed based on conditions
        """

        if str(self.region).strip().upper() in ["NAA", "NA"]:
            from .na import validate_naa_vendor_banking_details
            result = validate_naa_vendor_banking_details(sap_row=self.sap_row,
                                                         extracted_bank_infos=self.extracted_banking_details,
                                                         vendor_bank_info=self.vendor_banking_dict,
                                                         control_list=self.control_list)
            return result
        
        if str(self.region).strip().upper() in ["EMEAI", "EMEA"]:
            from .emeai import validate_emeai_vendor_banking_details
            result = validate_emeai_vendor_banking_details(sap_row=self.sap_row,
                                                           extracted_bank_infos=self.extracted_banking_details,
                                                           vendor_bank_info=self.vendor_banking_dict,
                                                           control_list=self.control_list)
            return result

        return build_validation_result(extracted_value={
                                                        # BANK_NAME: None,
                                                        BANK_ACCOUNT_NUMBER: None,
                                                        BANK_ACCOUNT_HOLDER_NAME: None},
                                            is_anomaly=None,
                                            highlight=None,
                                            edit_operation=None,
                                            method=None,
                                            supporting_details=None)