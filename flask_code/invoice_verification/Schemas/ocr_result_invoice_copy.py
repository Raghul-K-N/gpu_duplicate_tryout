# Schemas/ocr_result_invoice_copy.py
from invoice_verification.constant_field_names import *

class OCRData:
    """Data class to hold OCR extracted data from invoice PDF"""
    
    def __init__(self):
        # Initialize all fields
        setattr(self, INVOICE_NUMBER, None)
        setattr(self, STAMP_NUMBER, None)
        setattr(self, ESR_NUMBER, None)
        setattr(self, INVOICE_DATE, None)
        setattr(self, INVOICE_AMOUNT, None)
        setattr(self, INVOICE_CURRENCY, None)
        setattr(self, VENDOR_NAME, None)
        setattr(self, VENDOR_ADDRESS, None)
        setattr(self, LEGAL_ENTITY_NAME, None)
        setattr(self, LEGAL_ENTITY_ADDRESS, None)
        setattr(self, PAYMENT_TERMS, None)
        setattr(self, TEXT_INFO, None)
        setattr(self, LEGAL_REQUIREMENT, None)
        setattr(self, DOA, None)
        setattr(self, UDC, None)
        setattr(self, TRANSACTION_TYPE, None)
        setattr(self, VAT_TAX_CODE, None)
        setattr(self, VAT_TAX_AMOUNT, None)
        setattr(self, SERVICE_INVOICE_CONFIRMATION, None)
        setattr(self, PAYMENT_METHOD, None)
        setattr(self, INVOICE_RECEIPT_DATE, None)
        setattr(self, BANK_NAME, None)
        setattr(self, BANK_ACCOUNT_NUMBER, None)
        setattr(self, BANK_ACCOUNT_HOLDER_NAME, None)
        setattr(self, GL_ACCOUNT_NUMBER, None)
        
        # All extracted text lines
        self.all_text_lines = []
    
    @classmethod
    def from_dict(cls, data: dict):
        """
        Create OCRData object from JSON dictionary.
        Handles empty/None dictionaries gracefully.
        Args:
            data: Dictionary with OCR extracted data (can be empty or None)
        Returns:
            OCRData object with populated fields
        """
        obj = cls()
        
        if not data or not isinstance(data, dict):
            return obj  # Return empty object
        
        # Map JSON keys to object attributes
        obj.set_field(INVOICE_NUMBER, data.get('invoice_number'))
        obj.set_field(STAMP_NUMBER, data.get('stamp_number'))
        obj.set_field(ESR_NUMBER, data.get('esr_number'))
        obj.set_field(INVOICE_DATE, data.get('invoice_date'))
        obj.set_field(INVOICE_AMOUNT, data.get('invoice_amount'))
        obj.set_field(INVOICE_CURRENCY, data.get('invoice_currency'))
        obj.set_field(VENDOR_NAME, data.get('vendor_name'))
        obj.set_field(VENDOR_ADDRESS, data.get('vendor_address'))
        obj.set_field(LEGAL_ENTITY_NAME, data.get('legal_entity_name'))
        obj.set_field(LEGAL_ENTITY_ADDRESS, data.get('legal_entity_address'))
        obj.set_field(PAYMENT_TERMS, data.get('payment_terms'))
        obj.set_field(TEXT_INFO, data.get('text_info'))
        obj.set_field(LEGAL_REQUIREMENT, data.get('legal_requirement'))
        obj.set_field(DOA, data.get('doa'))
        obj.set_field(UDC, data.get('udc'))
        obj.set_field(TRANSACTION_TYPE, data.get('transaction_type'))
        obj.set_field(VAT_TAX_CODE, data.get('vat_tax_code'))
        obj.set_field(VAT_TAX_AMOUNT, data.get('vat_tax_amount'))
        obj.set_field(SERVICE_INVOICE_CONFIRMATION, data.get('service_invoice_confirmation'))
        obj.set_field(PAYMENT_METHOD, data.get('payment_method'))
        obj.set_field(INVOICE_RECEIPT_DATE, data.get('invoice_receipt_date'))
        obj.set_field(BANK_NAME, data.get('bank_name'))
        obj.set_field(BANK_ACCOUNT_NUMBER, data.get('bank_account_number'))
        obj.set_field(BANK_ACCOUNT_HOLDER_NAME, data.get('bank_account_holder_name'))
        obj.set_field(GL_ACCOUNT_NUMBER, data.get('gl_account_number'))
        
        # Handle text lines if present
        obj.all_text_lines = data.get('all_text_lines', [])
        
        return obj
    

    def set_field(self, field_name: str, value):
        """Set field value if not None"""
        if value is not None:
            setattr(self, field_name, value)
    

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}