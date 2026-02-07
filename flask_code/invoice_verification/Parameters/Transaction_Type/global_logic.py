from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import TRANSACTION_TYPE
from .utils import sap_transaction_type_map, invoice_transaction_type_map

def validate_global_transaction_type(sap_transaction_type: str,
                                     manual_transaction_type: str|None,
                                     invoice_extracted_transaction_type: str|None,
                                     voucher_extracted_transaction_type: str|None
                                    ) -> Dict:
    """
    Validate the Transaction Type based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    
    log_message("Global validation Started for Parameter Transation Type")

    # TODO: How to identify the Transaction Type?

    sap_transaction_type_mapped = str(sap_transaction_type).strip().upper() if sap_transaction_type else None
    if voucher_extracted_transaction_type:
        extracted_transaction_type = invoice_transaction_type_map.get(str(voucher_extracted_transaction_type).strip().lower(), None)
    elif manual_transaction_type:
        extracted_transaction_type = str(manual_transaction_type).strip().upper()
    elif invoice_extracted_transaction_type:
        extracted_transaction_type = invoice_transaction_type_map.get(str(invoice_extracted_transaction_type).strip().lower(), None)
    else:
        extracted_transaction_type = None
    
    log_message(f"SAP Transaction Type Mapped: {sap_transaction_type_mapped}, Extracted Transaction Type: {extracted_transaction_type}")

    if sap_transaction_type_mapped and extracted_transaction_type:
        if sap_transaction_type_mapped == extracted_transaction_type:
            return build_validation_result(extracted_value={TRANSACTION_TYPE: extracted_transaction_type},
                                            is_anomaly=False,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Automated process - Standard Validation"})
        else:
            return build_validation_result(extracted_value={TRANSACTION_TYPE: extracted_transaction_type},
                                            is_anomaly=True,
                                            highlight=False,
                                            edit_operation=False,
                                            method="Automated",
                                            supporting_details={"Summary":"Automated process - Standard Validation"})
    else:
        return build_validation_result(extracted_value={TRANSACTION_TYPE: extracted_transaction_type}, 
                                            is_anomaly=None,
                                            highlight=False,
                                            edit_operation=True,
                                            method="Automated",
                                            supporting_details={"Summary":"Automated process - Standard Validation"})