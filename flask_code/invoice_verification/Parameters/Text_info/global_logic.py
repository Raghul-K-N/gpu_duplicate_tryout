from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.logger.logger import log_message
from invoice_verification.constant_field_names import TEXT_INFO
from typing import Optional, Dict

def validate_global_text_info(text_field: str) -> Dict:
    """
    Validation process for Global Exceptions,
    and Standard Validation
    """
    log_message("Global Validation Started for Parameter Text Info")

    text_field = str(text_field).strip()
    if text_field == "":
        # Assumption: if the Text Column is Empty then now it returns None instead of True/False.
        return build_validation_result(extracted_value={TEXT_INFO: None},
                                        is_anomaly=None,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Empty Text Info field - Manual process"})
    else:
        return build_validation_result(extracted_value={TEXT_INFO: None},
                                        is_anomaly=False,
                                        highlight=True,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"Summary":"Non-empty Text Info field - Manual process"})