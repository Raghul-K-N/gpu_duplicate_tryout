# Schemas/invoice_processing_result.py

from invoice_verification.constant_field_names import *
from invoice_verification.logger.logger import log_message

class InvoiceProcessingResult:
    """Data class for invoice processing table results"""
    
    def __init__(self, transaction_id, account_document_id):
        self.transaction_id = transaction_id
        self.account_document_id = account_document_id
        self.id = account_document_id
        
        # Initialize all fields using constants (same names as DB columns)
        setattr(self, INVOICE_NUMBER, None)
        setattr(self, INVOICE_DATE, None)
        setattr(self, INVOICE_AMOUNT, None)
        setattr(self, INVOICE_CURRENCY, None)
        setattr(self, VENDOR_NAME, None)
        setattr(self, VENDOR_ADDRESS, None)
        setattr(self, LEGAL_ENTITY_NAME, None)
        setattr(self, LEGAL_ENTITY_ADDRESS, None)
        setattr(self, PAYMENT_TERMS, None)
        setattr(self, PAYMENT_METHOD, None)
        setattr(self, BANK_NAME, None)
        setattr(self, BANK_ACCOUNT_NUMBER, None)
        setattr(self, BANK_ACCOUNT_HOLDER_NAME, None)
        setattr(self, VAT_TAX_CODE, None)
        setattr(self, VAT_TAX_AMOUNT, None)
        setattr(self, GL_ACCOUNT_NUMBER, None)
        setattr(self, TEXT_INFO, None)
        setattr(self, LEGAL_REQUIREMENT, None)
        setattr(self, DOA, None)
        setattr(self, UDC, None)
        setattr(self, TRANSACTION_TYPE, None)
        setattr(self, SERVICE_INVOICE_CONFIRMATION, None)
        setattr(self, INVOICE_RECEIPT_DATE, None)
    
    def set_field(self, field_name: str, value):
        """
        Set a field value with validation.
        
        Args:
            field_name: Field name (must match DB column name from constants)
            value: Value to set
        """
        if not hasattr(self, field_name):
            log_message(f"Warning: Field {field_name} does not exist in InvoiceProcessingResult", error_logger=False)
            return
        # if value is list, store as comma-separated string
        if isinstance(value, list):
            value = ', '.join(map(str, value))
        
        setattr(self, field_name, value)
    
    def to_db_dict(self) -> dict:
        """Convert to dictionary for database insert"""
        return {
            'id': self.account_document_id,
            'transaction_id': self.transaction_id,
            'account_document_id': self.account_document_id,
            INVOICE_NUMBER: getattr(self, INVOICE_NUMBER),
            INVOICE_DATE: getattr(self, INVOICE_DATE),
            INVOICE_AMOUNT: getattr(self, INVOICE_AMOUNT),
            INVOICE_CURRENCY: getattr(self, INVOICE_CURRENCY),
            VENDOR_NAME: getattr(self, VENDOR_NAME),
            VENDOR_ADDRESS: getattr(self, VENDOR_ADDRESS),
            LEGAL_ENTITY_NAME: getattr(self, LEGAL_ENTITY_NAME),
            LEGAL_ENTITY_ADDRESS: getattr(self, LEGAL_ENTITY_ADDRESS),
            PAYMENT_TERMS: getattr(self, PAYMENT_TERMS),
            PAYMENT_METHOD: getattr(self, PAYMENT_METHOD),
            BANK_NAME: getattr(self, BANK_NAME),
            BANK_ACCOUNT_NUMBER: getattr(self, BANK_ACCOUNT_NUMBER),
            BANK_ACCOUNT_HOLDER_NAME: getattr(self, BANK_ACCOUNT_HOLDER_NAME),
            VAT_TAX_CODE: getattr(self, VAT_TAX_CODE),
            VAT_TAX_AMOUNT: getattr(self, VAT_TAX_AMOUNT),
            GL_ACCOUNT_NUMBER: getattr(self, GL_ACCOUNT_NUMBER),
            TEXT_INFO: getattr(self, TEXT_INFO),
            LEGAL_REQUIREMENT: getattr(self, LEGAL_REQUIREMENT),
            DOA: getattr(self, DOA),
            UDC: getattr(self, UDC),
            TRANSACTION_TYPE: getattr(self, TRANSACTION_TYPE),
            SERVICE_INVOICE_CONFIRMATION: getattr(self, SERVICE_INVOICE_CONFIRMATION),
            INVOICE_RECEIPT_DATE: getattr(self, INVOICE_RECEIPT_DATE)
        }