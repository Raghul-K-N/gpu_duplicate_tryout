from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import LEGAL_REQUIREMENT

class LegalRequirement:
    """
    Legal Requirement parameter validator.
    Handles Legal Requirement validation based on region-specific rules.
    """

    def __init__(self, sap_row: SAPRow, inv_ver_res: InvoiceVerificationResult):
        """
        Initialize validator with all required data.
        Args:
            sap_row: SAPRow object with SAP data and file lines.
            inv_ver_res: InvoiceVerificationResult object to use anomaly flags.
        """
        self.sap_row = sap_row
        self.region = (sap_row.region or "").upper()
        self.dp_doc_type = str(sap_row.dp_doc_type)
        self.doc_type = str(sap_row.doc_type)
        self.inv_ver_res = inv_ver_res

        log_message(f"Initialized LegalRequirement for transaction: {self.sap_row.transaction_id}, region: {self.region},\
                     DP Document Type: {self.dp_doc_type},\
                          Document Type: {self.doc_type}")

    def main(self) -> Dict:
        """
        Main method to calculate anomaly and determine value for IV Processing table.
        Returns:
            Dict with standardized output format.
        """
        try:
            log_message(f"Processing Legal Requirement for transaction: {self.sap_row.transaction_id}")
            result = self._validate_legal_requirement()
            return result
        except Exception as e:
            log_message(f"Error in Legal Requirement validation: {str(e)}", error_logger=True)
            return build_validation_result(
                extracted_value={LEGAL_REQUIREMENT: None},
                is_anomaly=None,
                highlight=None,
                edit_operation=None,
                method=None,
                supporting_details=None
            )

    def _validate_legal_requirement(self) -> Dict:
        """
        Validate extracted Legal Requirement against SAP value.
        Routes to region-specific validation logic.
        """
        if str(self.dp_doc_type).strip() == "":
            log_message("Missing DP Document Type - cannot validate Legal Requirement")
            return build_validation_result(
                extracted_value={LEGAL_REQUIREMENT: None},
                is_anomaly=None,
                highlight=None,
                edit_operation=None,
                method=None,
                supporting_details={"Summary": "DP Document Type is missing; Legal Requirement validation skipped."}
            )

        if self.region in ["EMEAI", "EMEA"]:
            from .emeai import validate_emeai_legal_requirement
            return validate_emeai_legal_requirement(
                dp_doc_type=self.dp_doc_type,
                anomaly_object=self.inv_ver_res
            )
        else:
            from .global_logic import validate_global_legal_requirement
            return validate_global_legal_requirement(
                dp_doc_type=self.dp_doc_type,
                anomaly_object=self.inv_ver_res
            )
