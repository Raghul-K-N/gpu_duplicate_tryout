# utils/constants.py
"""
Central constants file for field names and parameter codes.
These MUST match the database column names exactly.
"""

# ============================================================================
# DATABASE FIELD NAMES (must match ORM model column names exactly)
# ============================================================================

# Invoice fields
INVOICE_NUMBER = 'invoice_number'
STAMP_NUMBER = 'stamp_number'
INVOICE_DATE = 'invoice_date'
INVOICE_AMOUNT = 'invoice_amount'
INVOICE_CURRENCY = 'invoice_currency'
INVOICE_RECEIPT_DATE = 'invoice_receipt_date'

# Vendor fields
VENDOR_NAME = 'vendor_name'
VENDOR_ADDRESS = 'vendor_address'

# Legal entity fields
LEGAL_ENTITY_NAME = 'legal_entity_name'
LEGAL_ENTITY_ADDRESS = 'legal_entity_address'

# Payment fields
PAYMENT_TERMS = 'payment_terms'
PAYMENT_METHOD = 'payment_method'

# Banking fields
BANK_NAME = 'bank_name'
BANK_ACCOUNT_NUMBER = 'bank_account_number'
BANK_ACCOUNT_HOLDER_NAME = 'bank_account_holder_name'
PARTNER_BANK_TYPE = 'partner_bank_type'
IBAN = "IBAN"
ESR_NUMBER = 'esr_number'
SWIFT_CODE = 'SWIFT_BIC'

# Tax fields
VAT_TAX_CODE = 'vat_tax_code'
VAT_TAX_AMOUNT = 'vat_tax_amount'

# Other fields
GL_ACCOUNT_NUMBER = 'gl_account_number'
TEXT_INFO = 'text_info'
LEGAL_REQUIREMENT = 'legal_requirement'
DOA = 'doa'
UDC = 'udc'
TRANSACTION_TYPE = 'transaction_type'
SERVICE_INVOICE_CONFIRMATION = 'service_invoice_confirmation'
INVOICE_IS_ATTACHED = 'invoice_is_attached'

#Multi-value fields
VENDOR_BANKING_DETAILS = 'vendor_banking_details'
VENDOR_NAME_AND_ADDRESS = 'vendor_name_and_address'
LEGAL_ENTITY_NAME_AND_ADDRESS = 'legal_entity_name_and_address'

PO_NUMBER = 'po_number'
# ============================================================================
# VERIFICATION ANOMALY FIELDS (must match verification table column names)
# ============================================================================

INVOICE_IS_ATTACHED_ANOMALY = 'invoice_is_attached_anomaly'
INVOICE_NUMBER_ANOMALY = 'invoice_number_anomaly'
GL_ACCOUNT_NUMBER_ANOMALY = 'gl_account_number_anomaly'
INVOICE_AMOUNT_ANOMALY = 'invoice_amount_anomaly'
INVOICE_CURRENCY_ANOMALY = 'invoice_currency_anomaly'
VENDOR_NAME_AND_ADDRESS_ANOMALY = 'vendor_name_and_address_anomaly'
LEGAL_ENTITY_NAME_AND_ADDRESS_ANOMALY = 'legal_entity_name_and_address_anomaly'
PAYMENT_TERMS_ANOMALY = 'payment_terms_anomaly'
INVOICE_DATE_ANOMALY = 'invoice_date_anomaly'
TEXT_INFO_ANOMALY = 'text_info_anomaly'
LEGAL_REQUIREMENT_ANOMALY = 'legal_requirement_anomaly'
DOA_ANOMALY = 'doa_anomaly'
UDC_ANOMALY = 'udc_anomaly'
TRANSACTION_TYPE_ANOMALY = 'transaction_type_anomaly'
VAT_TAX_CODE_ANOMALY = 'vat_tax_code_anomaly'
SERVICE_INVOICE_CONFIRMATION_ANOMALY = 'service_invoice_confirmation_anomaly'
VENDOR_BANKING_DETAILS_ANOMALY = 'vendor_banking_details_anomaly'
PAYMENT_METHOD_ANOMALY = 'payment_method_anomaly'
INVOICE_RECEIPT_DATE_ANOMALY = 'invoice_receipt_date_anomaly'
