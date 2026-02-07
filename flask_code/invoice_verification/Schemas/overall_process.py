from invoice_verification.Schemas.invoice_processing_result import InvoiceProcessingResult
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from invoice_verification.Schemas.invoice_param_config_result import InvoiceParamConfigResult
from invoice_verification.Schemas.ui_invoice_flat_result import UIInvoiceFlatResult
from invoice_verification.Schemas.invoice_attachments_result import InvoiceAttachmentsResult
from invoice_verification.Schemas.ocr_result_invoice_copy import OCRData
from invoice_verification.Schemas.ocr_result_voucher_copy import VoucherOCRData
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
from invoice_verification.invoice_extraction import get_llama_api_result
from invoice_verification.db.db_connection import insert_complete_invoice_data


class OverallProcess:
    """Main processing orchestrator for a single invoice"""
    
    def __init__(self, sap_row:SAPRow, vendors_df, batch_id, vat_df=None, gl_accounts_df=None):
        self.sap_row = sap_row
        self.vendors_df = vendors_df
        self.batch_id = batch_id
        self.vat_df = vat_df
        self.gl_accounts_df = gl_accounts_df

    
    def main(self):
        """Main processing pipeline"""
        try:
            invoice_ocr_obj = self._process_and_extract_invoice_copy()
            voucher_ocr_obj = self._process_and_extract_voucher_copy()

            log_message(f"INVOICE OCR Data: {invoice_ocr_obj.__dict__}")
            log_message(f"VOUCHER OCR Data: {voucher_ocr_obj.__dict__}")

            processing_result = InvoiceProcessingResult(transaction_id=self.sap_row.transaction_id, account_document_id=self.sap_row.account_document_id)
            verification_result = InvoiceVerificationResult(transaction_id=self.sap_row.transaction_id, account_document_id=self.sap_row.account_document_id)
            param_config_result = InvoiceParamConfigResult()
                        
            from invoice_verification.Parameters import ParameterValidationOrchestrator
            #Validate parameters using ParameterValidationOrchestrator
            orchestrator = ParameterValidationOrchestrator(
                                                        invoice_ocr=invoice_ocr_obj,
                                                        voucher_ocr=voucher_ocr_obj,
                                                        sap_row=self.sap_row,
                                                        vendors_df=self.vendors_df,
                                                        vat_df=self.vat_df,
                                                        gl_accounts_df=self.gl_accounts_df
                                                    )
            
            success_count, failed_count = orchestrator.validate_all_parameters(processing_result, verification_result, param_config_result)

            log_message(f"Parameter validation completed for transaction {self.sap_row.transaction_id}. Success: {success_count}, Failed: {failed_count}")

            # Prepare UI Flat Result
            ui_flat_result = UIInvoiceFlatResult(processing_result, verification_result, param_config_result)

            # Prepare Attachments Result
            attachments_result = InvoiceAttachmentsResult()
            attachments_result.set_attachment_paths(self.sap_row.attachments)

            #Insert results into the database
            insert_complete_invoice_data(processing_result=processing_result,
                                         verification_result=verification_result,
                                         param_config_result=param_config_result,
                                         ui_flat_result=ui_flat_result,
                                         attachments_result=attachments_result,
                                         quarter_label=self.sap_row.quarter_label)
            return True
            
        except Exception as e:
            log_message(f"Overall processing failed for transaction {self.sap_row.transaction_id}: {e}", error_logger=True)
            import traceback
            log_message(traceback.format_exc(), error_logger=True)
            return False
            
    

    def _process_and_extract_invoice_copy(self) -> OCRData:
        """ Process and extract data from invoice copy """
        if not self.sap_row.invoice_pdf_path:
            log_message("No invoice PDF path, returning empty OCRData")
            return OCRData()
        try:
            # if (len(self.sap_row.invoice_lines)< 5) or self.sap_row.dummy_generic_invoice_flag:
            #     log_message("No invoice lines found/Dummpy PDF, No LLM callreturning empty OCRData")
            #     return OCRData()
            # else:
            invoice_ocr_json = get_llama_api_result(
                account_document=str(self.sap_row.account_document_number),
                text_lines=self.sap_row.invoice_lines_1,
                sap_row=self.sap_row,
                invoice_type="invoice"
            )
            log_message(f"LLM OCR extraction result: {invoice_ocr_json}")
            # Convert JSON to OCRData object
            return OCRData.from_dict(invoice_ocr_json)
        
        except Exception as e:
            log_message(f"Invoice OCR extraction failed: {e}", error_logger=True)
            return OCRData()


    def _process_and_extract_voucher_copy(self) -> VoucherOCRData:
        """ Process and extract data from voucher copy """
        if not self.sap_row.voucher_pdf_path:
            log_message("No voucher PDF path, returning empty VoucherOCRData")
            return VoucherOCRData()
            
        try:
            
            voucher_ocr_json = get_llama_api_result(
                account_document=str(self.sap_row.account_document_number),
                text_lines=self.sap_row.voucher_lines,
                sap_row=self.sap_row,
                invoice_type="voucher"
            )
            # Convert JSON to VoucherOCRData object
            return VoucherOCRData.from_dict(voucher_ocr_json)
        
        except Exception as e:
            log_message(f"Voucher OCR extraction failed: {e}", error_logger=True)
            return VoucherOCRData()
