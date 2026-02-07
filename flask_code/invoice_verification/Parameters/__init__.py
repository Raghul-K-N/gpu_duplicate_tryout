# Parameters/__init__.py

"""
Parameter Validation Orchestrator
Validates all invoice parameters and populates result objects
"""

from invoice_verification.logger.logger import log_message
from typing import Tuple, Optional
import pandas as pd
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData  # Add actual imports
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.invoice_processing_result import InvoiceProcessingResult
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from invoice_verification.Schemas.invoice_param_config_result import InvoiceParamConfigResult
from invoice_verification.constant_field_names import *
from invoice_verification.db.db_connection import get_param_mappings

# Import all parameter validators
from invoice_verification.Parameters.Invoice_Number import InvoiceNumber
from invoice_verification.Parameters.Gl_Account_Number import GLAccountNumber
from invoice_verification.Parameters.Invoice_amount import InvoiceAmount
from invoice_verification.Parameters.Invoice_Currency import InvoiceCurrency
from invoice_verification.Parameters.Vendor_name_and_Remit_to_Addr import VendorNameAndRemitToAddress
from invoice_verification.Parameters.Bill_to_LE_and_Addr import BillToLegalEntityAndAddress
from invoice_verification.Parameters.Payment_terms import PaymentTerms
from invoice_verification.Parameters.Invoice_date import InvoiceDate
from invoice_verification.Parameters.Text_info import TextInfo
from invoice_verification.Parameters.DOA import Doa
from invoice_verification.Parameters.UDC import Udc
from invoice_verification.Parameters.Transaction_Type import TransactionType
from invoice_verification.Parameters.Vat_Tax_code import VatTaxCode
from invoice_verification.Parameters.Service_Invoice_Confirmation import ServiceInvoiceConfirmation
from invoice_verification.Parameters.Vendor_banking_Details import VendorBankingDetails
from invoice_verification.Parameters.Payment_Method import PaymentMethod
from invoice_verification.Parameters.Invoice_Reciept_date import InvoiceReceiptDate
from invoice_verification.Parameters.Invoice_is_attached import InvoiceIsAttached   # new - validator for attachment check
from invoice_verification.Parameters.Legal_Requirement import LegalRequirement       # new - processed after loop
from invoice_verification.Parameters.PO_Number import PONumber

class ParameterValidationOrchestrator:
    """
    Orchestrates validation of all invoice parameters.
    Uses a configuration-driven approach for better maintainability.
    """
    
    PARAMETER_LIST = [
        # InvoiceIsAttached,
        InvoiceNumber,
        # GLAccountNumber,
        InvoiceAmount,
        InvoiceCurrency,
        # VendorNameAndRemitToAddress,
        # BillToLegalEntityAndAddress,
        # PaymentTerms,
        InvoiceDate,
        # TextInfo,
        # Doa,
        # Udc,
        # TransactionType,
        VatTaxCode,
        # PONumber       
         # ServiceInvoiceConfirmation,
        # VendorBankingDetails,
        # PaymentMethod,
        # InvoiceReceiptDate
    ]

    def __init__(self, invoice_ocr: OCRData, voucher_ocr: VoucherOCRData, sap_row: SAPRow,
                vendors_df: pd.DataFrame, vat_df: Optional[pd.DataFrame] = None, 
                gl_accounts_df: Optional[pd.DataFrame] = None):
        """
        Initialize orchestrator with all required data.

        Args:
            invoice_ocr: OCRData object with extracted invoice data
            voucher_ocr: VoucherOCRData object
            sap_row: SAPRow object
            vendors_df: Vendors DataFrame
            vat_df: VAT DataFrame (optional)
            gl_accounts_df: GL Accounts DataFrame (optional)
        """
        self.invoice_ocr = invoice_ocr
        self.voucher_ocr = voucher_ocr
        self.sap_row = sap_row
        self.vendors_df = vendors_df
        self.vat_df = vat_df
        self.gl_accounts_df = gl_accounts_df
        
        # Result objects with type hints
        self.processing_result: InvoiceProcessingResult
        self.verification_result: InvoiceVerificationResult
        self.param_config_result: InvoiceParamConfigResult
        self.field_anomaly_map, self.field_param_code_map = get_param_mappings()
    
    def validate_all_parameters(self, processing_result: InvoiceProcessingResult, verification_result: InvoiceVerificationResult,
                                param_config_result: InvoiceParamConfigResult) -> Tuple[int, int]:
        """
        Validate all parameters and populate result objects.
        
        Args:
            processing_result: InvoiceProcessingResult object to populate
            verification_result: InvoiceVerificationResult object to populate
            param_config_result: InvoiceParamConfigResult object to populate
        """
        self.processing_result = processing_result
        self.verification_result = verification_result
        self.param_config_result = param_config_result
        
        log_message(f"Starting parameter validation for transaction {self.sap_row.transaction_id}")
        
        success_count = 0
        failed_count = 0
        param_config_added = set()
        # 1. Process all other parameters except legal_requirement
        for ValidatorClass in self.PARAMETER_LIST:
            param_name = ValidatorClass.__name__
            try:
                validator = ValidatorClass(
                    invoice_ocr=self.invoice_ocr,
                    voucher_ocr=self.voucher_ocr,
                    sap_row=self.sap_row,
                    vendors_df=self.vendors_df,
                    vat_df=self.vat_df,
                    gl_accounts_df=self.gl_accounts_df
                )
                
                # Get validation result
                result = validator.main()
                log_message(f"Validation result for {param_name}: {result}")

                # Extract values
                extracted_value = result.get('extracted_value')
                is_anomaly = result.get('is_anomaly')
                method = result.get('method')
                editable = result.get('edit_operation')
                highlight = result.get('highlight')
                supporting_details = result.get('supporting_details')


                for field_name, field_value in extracted_value.items():
                    processing_result.set_field(field_name, field_value)
                    anomaly_field = self.field_anomaly_map.get(field_name)
                    param_code = self.field_param_code_map.get(field_name)
                    if anomaly_field:
                        setattr(self.verification_result, anomaly_field, 
                                1 if is_anomaly else (0 if is_anomaly is False else None))
                    if param_code and param_code not in param_config_added:
                        self.param_config_result.add_param_config(
                            param_code=param_code,
                            validation_method=method,
                            editable=editable,
                            highlight=highlight,
                            supporting_fields=supporting_details
                        )
                        param_config_added.add(param_code)

                success_count += 1
                log_message(f"Validated {param_name} successfully")
                
            except Exception as e:
                log_message(f"Error validating {param_name}: {e}", error_logger=True)
                failed_count += 1
                continue
    
        # 3. Process legal requirement last - needs verification_result
        try:
            print(f"Validating Legal Requirement parameter: region={self.sap_row.region}, dp_doc_type={self.sap_row.dp_doc_type}")
            legal_validator = LegalRequirement(
                sap_row=self.sap_row,
                inv_ver_res=self.verification_result  # Pass verification result
            )
            
            legal_result = legal_validator.main()
            extracted_value = legal_result.get('extracted_value')
            is_anomaly = legal_result.get('is_anomaly')
            method = legal_result.get('method')
            highlight = legal_result.get('highlight')
            editable = legal_result.get('edit_operation')
            supporting_details = legal_result.get('supporting_details')
            
            field_name, field_value = next(iter(extracted_value.items()))
            self.processing_result.set_field(field_name, field_value)
            anomaly_field = self.field_anomaly_map.get(field_name)
            if anomaly_field:
                setattr(self.verification_result, anomaly_field,
                        1 if is_anomaly else (0 if is_anomaly is False else None))
            param_code = self.field_param_code_map.get(field_name)
            if param_code and param_code not in param_config_added:
                self.param_config_result.add_param_config(
                    param_code=param_code,
                    validation_method=method,
                    editable=editable,
                    highlight=highlight,
                    supporting_fields=supporting_details
                )
                param_config_added.add(param_code)
            
            success_count += 1
            log_message("Legal requirement validation complete")
            
        except Exception as e:
            log_message(f"Error validating legal requirement: {e}", error_logger=True)
            failed_count += 1
        
        log_message(f"Parameter validation complete: {success_count} success, {failed_count} failed")
        return success_count, failed_count
