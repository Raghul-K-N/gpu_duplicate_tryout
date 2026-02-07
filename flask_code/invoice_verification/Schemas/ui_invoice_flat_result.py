# Schemas/ui_invoice_flat_result.py

class UIInvoiceFlatResult:
    """Data class for UI flat table - combines all invoice data"""
    
    # Define default values as class constants
    DEFAULT_VALIDATION_METHOD = 'Automated'
    DEFAULT_EDITABLE = False
    DEFAULT_HIGHLIGHT = False
    DEFAULT_SUPPORTING_FIELDS = {}
    
    def __init__(self, processing_result, verification_result, param_config_result):
        # Reference fields
        self.transaction_id = processing_result.transaction_id
        self.account_document_id = processing_result.account_document_id
        
        # All invoice fields from processing
        self.invoice_number = processing_result.invoice_number
        self.invoice_date = processing_result.invoice_date
        self.invoice_amount = processing_result.invoice_amount
        self.invoice_currency = processing_result.invoice_currency
        self.vendor_name = processing_result.vendor_name
        self.vendor_address = processing_result.vendor_address
        self.legal_entity_name = processing_result.legal_entity_name
        self.legal_entity_address = processing_result.legal_entity_address
        self.payment_terms = processing_result.payment_terms
        self.payment_method = processing_result.payment_method
        self.vat_tax_code = processing_result.vat_tax_code
        self.vat_tax_amount = processing_result.vat_tax_amount
        self.doa = processing_result.doa
        self.udc = processing_result.udc
        self.transaction_type = processing_result.transaction_type
        self.service_invoice_confirmation = processing_result.service_invoice_confirmation
        self.bank_name = processing_result.bank_name
        self.bank_account_number = processing_result.bank_account_number
        self.bank_account_holder_name = processing_result.bank_account_holder_name
        self.text_info = processing_result.text_info
        self.legal_requirement = processing_result.legal_requirement
        self.invoice_receipt_date = processing_result.invoice_receipt_date
        self.gl_account_number = processing_result.gl_account_number
        
        # All anomaly flags from verification
        self.invoice_is_attached_anomaly = verification_result.invoice_is_attached_anomaly
        self.invoice_number_anomaly = verification_result.invoice_number_anomaly
        self.gl_account_number_anomaly = verification_result.gl_account_number_anomaly
        self.invoice_amount_anomaly = verification_result.invoice_amount_anomaly
        self.invoice_currency_anomaly = verification_result.invoice_currency_anomaly
        self.vendor_name_and_address_anomaly = verification_result.vendor_name_and_address_anomaly
        self.legal_entity_name_and_address_anomaly = verification_result.legal_entity_name_and_address_anomaly
        self.payment_terms_anomaly = verification_result.payment_terms_anomaly
        self.invoice_date_anomaly = verification_result.invoice_date_anomaly
        self.text_info_anomaly = verification_result.text_info_anomaly
        self.legal_requirement_anomaly = verification_result.legal_requirement_anomaly
        self.doa_anomaly = verification_result.doa_anomaly
        self.udc_anomaly = verification_result.udc_anomaly
        self.transaction_type_anomaly = verification_result.transaction_type_anomaly
        self.vat_tax_code_anomaly = verification_result.vat_tax_code_anomaly
        self.service_invoice_confirmation_anomaly = verification_result.service_invoice_confirmation_anomaly
        self.vendor_banking_details_anomaly = verification_result.vendor_banking_details_anomaly
        self.payment_method_anomaly = verification_result.payment_method_anomaly
        self.invoice_receipt_date_anomaly = verification_result.invoice_receipt_date_anomaly
        
        # Initialize metadata dictionaries for non-default values only
        self.editable_fields = {}
        self.highlightable_fields = {}
        self.validation_methods = {}
        self.supporting_fields = {}
        
        # Process param configs
        for config in param_config_result.configs:
            param_code = config['param_code']            
            # Store editable if different from default
            if config['editable'] != self.DEFAULT_EDITABLE:
                self.editable_fields[param_code] = config['editable']
            # Store highlight if different from default
            if config['highlight'] != self.DEFAULT_HIGHLIGHT:
                self.highlightable_fields[param_code] = config['highlight']
            # Store validation method if different from default
            if config['validation_method'] != self.DEFAULT_VALIDATION_METHOD:
                self.validation_methods[param_code] = config['validation_method']
            # Store supporting fields if not empty
            if config['supporting_fields'] != self.DEFAULT_SUPPORTING_FIELDS:
                self.supporting_fields[param_code] = config['supporting_fields']
    
    def to_db_dict(self) -> dict:
        """Convert to dict for database insert"""
        base_dict = {
            'transaction_id': self.transaction_id,
            'account_document_id': self.account_document_id,
            
            # All invoice fields
            'invoice_number': self.invoice_number,
            'invoice_date': self.invoice_date,
            'invoice_amount': self.invoice_amount,
            'invoice_currency': self.invoice_currency,
            'vendor_name': self.vendor_name,
            'vendor_address': self.vendor_address,
            'legal_entity_name': self.legal_entity_name,
            'legal_entity_address': self.legal_entity_address,
            'payment_terms': self.payment_terms,
            'payment_method': self.payment_method,
            'vat_tax_code': self.vat_tax_code,
            'vat_tax_amount': self.vat_tax_amount,
            'doa': self.doa,
            'udc': self.udc,
            'transaction_type': self.transaction_type,
            'service_invoice_confirmation': self.service_invoice_confirmation,
            'bank_name': self.bank_name,
            'bank_account_number': self.bank_account_number,
            'bank_account_holder_name': self.bank_account_holder_name,
            'text_info': self.text_info,
            'legal_requirement': self.legal_requirement,
            'invoice_receipt_date': self.invoice_receipt_date,
            'gl_account_number': self.gl_account_number,
            
            # All anomaly flags
            'invoice_is_attached_anomaly': self.invoice_is_attached_anomaly,
            'invoice_number_anomaly': self.invoice_number_anomaly,
            'gl_account_number_anomaly': self.gl_account_number_anomaly,
            'invoice_amount_anomaly': self.invoice_amount_anomaly,
            'invoice_currency_anomaly': self.invoice_currency_anomaly,
            'vendor_name_and_address_anomaly': self.vendor_name_and_address_anomaly,
            'legal_entity_name_and_address_anomaly': self.legal_entity_name_and_address_anomaly,
            'payment_terms_anomaly': self.payment_terms_anomaly,
            'invoice_date_anomaly': self.invoice_date_anomaly,
            'text_info_anomaly': self.text_info_anomaly,
            'legal_requirement_anomaly': self.legal_requirement_anomaly,
            'doa_anomaly': self.doa_anomaly,
            'udc_anomaly': self.udc_anomaly,
            'transaction_type_anomaly': self.transaction_type_anomaly,
            'vat_tax_code_anomaly': self.vat_tax_code_anomaly,
            'service_invoice_confirmation_anomaly': self.service_invoice_confirmation_anomaly,
            'vendor_banking_details_anomaly': self.vendor_banking_details_anomaly,
            'payment_method_anomaly': self.payment_method_anomaly,
            'invoice_receipt_date_anomaly': self.invoice_receipt_date_anomaly,
        }
        
        # Only include metadata fields if they have non-default values
        if self.editable_fields:
            base_dict['editable_fields'] = self.editable_fields
            
        if self.highlightable_fields:
            base_dict['highlightable_fields'] = self.highlightable_fields
            
        if self.validation_methods:
            base_dict['validation_methods'] = self.validation_methods
            
        if self.supporting_fields:
            base_dict['supporting_fields'] = self.supporting_fields
            
        return base_dict