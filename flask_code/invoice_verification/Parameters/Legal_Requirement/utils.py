# invoice_verification/Parameters/Legal Requirement/utils.py
# Defaults you asked to keep centrally
import invoice_verification.constant_field_names as cfn

LEGAL_DB_DOC_TYPES = ["PO_EC_GLB", "NPO_EC_GLB"]
# Centralized list of mismatch fields
ANOMALY_FIELDS = [
    cfn.INVOICE_NUMBER_ANOMALY,
    cfn.GL_ACCOUNT_NUMBER_ANOMALY,
    cfn.INVOICE_AMOUNT_ANOMALY,
    cfn.INVOICE_CURRENCY_ANOMALY,
    cfn.VENDOR_NAME_AND_ADDRESS_ANOMALY,
    cfn.LEGAL_ENTITY_NAME_AND_ADDRESS_ANOMALY,
    cfn.PAYMENT_TERMS_ANOMALY,
    cfn.INVOICE_DATE_ANOMALY,
    cfn.TEXT_INFO_ANOMALY,
    cfn.DOA_ANOMALY,
    cfn.UDC_ANOMALY,
    cfn.TRANSACTION_TYPE_ANOMALY,
    cfn.VAT_TAX_CODE_ANOMALY,
    cfn.SERVICE_INVOICE_CONFIRMATION_ANOMALY,
    cfn.VENDOR_BANKING_DETAILS_ANOMALY,
    cfn.PAYMENT_METHOD_ANOMALY,
    cfn.INVOICE_RECEIPT_DATE_ANOMALY,
]

def has_any_anomaly(obj):
    """
    Returns True if any of the anomaly fields on obj are True or None.
    """
    return any(
        getattr(obj, field, None) is None or getattr(obj, field, None) is True
        for field in ANOMALY_FIELDS
    )

