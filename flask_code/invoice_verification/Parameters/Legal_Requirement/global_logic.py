from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from .utils import has_any_anomaly, LEGAL_DB_DOC_TYPES
from invoice_verification.constant_field_names import LEGAL_REQUIREMENT

def validate_global_legal_requirement(dp_doc_type: str, anomaly_object: InvoiceVerificationResult) -> Dict:
    """
    Validates legal requirements for global cases:
    - Checks if document type is PO_EC_GLB or NPO_EC_GLB
    - Verifies if any anomalies are present in the invoice data
    
    Args:
        dp_doc_type: Document processing document type
        anomaly_object: InvoiceVerificationResult object containing anomaly flags
        
    Returns:
        Dict containing validation results with standardized format
    """
    log_message("Global Validation Started for Parameter Legal Requirement")
    dp_doc_type = str(dp_doc_type).strip().upper()

    if dp_doc_type in LEGAL_DB_DOC_TYPES:
        if has_any_anomaly(anomaly_object):
            log_message(f"Anomaly detected for other fields marking legal requirement as anomaly.")
            return build_validation_result(
                extracted_value={LEGAL_REQUIREMENT: None},
                is_anomaly=True,
                highlight=True,
                edit_operation=False,
                method="Combined",
                supporting_details={"Summary":"Anomalies detected in invoice data - Legal Requirement."}
            )
        log_message("No anomalies detected, marking legal requirement as non-anomalous.")
        return build_validation_result(
            extracted_value={LEGAL_REQUIREMENT: None},
            is_anomaly=False,
            highlight=True,
            edit_operation=False,
            method="Combined",
            supporting_details={"Summary":"No anomalies detected - Legal Requirement."}
        )

    log_message("DP Document type does not require legal requirement validation.")
    return build_validation_result(
        extracted_value={LEGAL_REQUIREMENT: None},
        is_anomaly=None,
        highlight=None,
        edit_operation=None,
        method=None,
        supporting_details={"Summary":"DP Document type does not require legal requirement validation."}
    )

