# Schemas/invoice_verification_result.py
from invoice_verification.constant_field_names import *
from invoice_verification.logger.logger import log_message

class InvoiceVerificationResult:
    """Data class for invoice verification table results"""
    
    def __init__(self, transaction_id, account_document_id):
        # Reference fields
        self.transaction_id = transaction_id
        self.account_document_id = account_document_id
        
        # Initialize all fields using constants (same names as DB columns)
        setattr(self, INVOICE_IS_ATTACHED_ANOMALY, None)
        setattr(self, INVOICE_NUMBER_ANOMALY, None)
        setattr(self, GL_ACCOUNT_NUMBER_ANOMALY, None)
        setattr(self, INVOICE_AMOUNT_ANOMALY, None)
        setattr(self, INVOICE_CURRENCY_ANOMALY, None)
        setattr(self, VENDOR_NAME_AND_ADDRESS_ANOMALY, None)
        setattr(self, LEGAL_ENTITY_NAME_AND_ADDRESS_ANOMALY, None)
        setattr(self, PAYMENT_TERMS_ANOMALY, None)
        setattr(self, INVOICE_DATE_ANOMALY, None)
        setattr(self, TEXT_INFO_ANOMALY, None)
        setattr(self, LEGAL_REQUIREMENT_ANOMALY, None)
        setattr(self, DOA_ANOMALY, None)
        setattr(self, UDC_ANOMALY, None)
        setattr(self, TRANSACTION_TYPE_ANOMALY, None)
        setattr(self, VAT_TAX_CODE_ANOMALY, None)
        setattr(self, SERVICE_INVOICE_CONFIRMATION_ANOMALY, None)
        setattr(self, VENDOR_BANKING_DETAILS_ANOMALY, None)
        setattr(self, PAYMENT_METHOD_ANOMALY, None)
        setattr(self, INVOICE_RECEIPT_DATE_ANOMALY, None)
    
    def to_db_dict(self) -> dict:
        """Convert to dictionary for database insert"""
        return {
            'transaction_id': self.transaction_id,
            'account_document_id': self.account_document_id,
            INVOICE_IS_ATTACHED_ANOMALY: getattr(self, INVOICE_IS_ATTACHED_ANOMALY),
            INVOICE_NUMBER_ANOMALY: getattr(self, INVOICE_NUMBER_ANOMALY),
            GL_ACCOUNT_NUMBER_ANOMALY: getattr(self, GL_ACCOUNT_NUMBER_ANOMALY),
            INVOICE_AMOUNT_ANOMALY: getattr(self, INVOICE_AMOUNT_ANOMALY),
            INVOICE_CURRENCY_ANOMALY: getattr(self, INVOICE_CURRENCY_ANOMALY),
            VENDOR_NAME_AND_ADDRESS_ANOMALY: getattr(self, VENDOR_NAME_AND_ADDRESS_ANOMALY),
            LEGAL_ENTITY_NAME_AND_ADDRESS_ANOMALY: getattr(self, LEGAL_ENTITY_NAME_AND_ADDRESS_ANOMALY),
            PAYMENT_TERMS_ANOMALY: getattr(self, PAYMENT_TERMS_ANOMALY),
            INVOICE_DATE_ANOMALY: getattr(self, INVOICE_DATE_ANOMALY),
            TEXT_INFO_ANOMALY: getattr(self, TEXT_INFO_ANOMALY),
            LEGAL_REQUIREMENT_ANOMALY: getattr(self, LEGAL_REQUIREMENT_ANOMALY),
            DOA_ANOMALY: getattr(self, DOA_ANOMALY),
            UDC_ANOMALY: getattr(self, UDC_ANOMALY),
            TRANSACTION_TYPE_ANOMALY: getattr(self, TRANSACTION_TYPE_ANOMALY),
            VAT_TAX_CODE_ANOMALY: getattr(self, VAT_TAX_CODE_ANOMALY),
            SERVICE_INVOICE_CONFIRMATION_ANOMALY: getattr(self, SERVICE_INVOICE_CONFIRMATION_ANOMALY),
            VENDOR_BANKING_DETAILS_ANOMALY: getattr(self, VENDOR_BANKING_DETAILS_ANOMALY),
            PAYMENT_METHOD_ANOMALY: getattr(self, PAYMENT_METHOD_ANOMALY),
            INVOICE_RECEIPT_DATE_ANOMALY: getattr(self, INVOICE_RECEIPT_DATE_ANOMALY)
        }