from typing import Dict, Optional
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from .utils import has_any_anomaly, LEGAL_DB_DOC_TYPES
from invoice_verification.constant_field_names import LEGAL_REQUIREMENT, INVOICE_IS_ATTACHED_ANOMALY



def validate_emeai_legal_requirement(dp_doc_type: str, anomaly_object: InvoiceVerificationResult) -> Dict:
    """
    Checks if the invoice is attached for document types requiring legal validation in EMEAI,
    and verifies if any anomalies are present in the invoice data.
    Returns a validation result indicating if an anomaly exists or further action is needed.
    """
    log_message("EMEAI Validation Started for Parameter Legal Requirement")
    dp_doc_type = str(dp_doc_type).strip().upper()
    invoice_is_attached_anomaly = getattr(anomaly_object, INVOICE_IS_ATTACHED_ANOMALY, None)

    if dp_doc_type in LEGAL_DB_DOC_TYPES:
        if not invoice_is_attached_anomaly or has_any_anomaly(anomaly_object):
            log_message(f"Anomaly detected for other fields or invoice not attached marking legal requirement as anomaly.")
            return build_validation_result(
                extracted_value={LEGAL_REQUIREMENT: None},
                is_anomaly=True,
                highlight=True,
                edit_operation=False,
                method="Combined",
                supporting_details={"Summary":"Anomalies detected in invoice data or invoice not attached."}
            )
        log_message("No anomalies detected and invoice is attached, marking legal requirement as non-anomalous.")
        return build_validation_result(
            extracted_value={LEGAL_REQUIREMENT: None},
            is_anomaly=False,
            highlight=True,
            edit_operation=False,
            method="Combined",
            supporting_details={"Summary":"No anomalies detected and invoice is attached."}
        )
    log_message("DB Document type does not require legal requirement validation in EMEAI.")
    return build_validation_result(
        extracted_value={LEGAL_REQUIREMENT: None},
        is_anomaly=None,
        highlight=None,
        edit_operation=None,
        method=None,
        supporting_details={"Summary":"DB Document type does not require legal requirement validation in EMEAI."}
    )
