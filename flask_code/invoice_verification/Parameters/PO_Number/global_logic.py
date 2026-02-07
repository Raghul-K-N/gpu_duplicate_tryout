from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import PO_NUMBER


def standard_validation_po_number_logic(sap_value_list: list
                                        ,extracted_value_list: list
                                        ,summary: str= ""):
    """
    Docstring for standard_validation_po_number_logic
    
    :param sap_value_list: Description
    :param extracted_value_list: Description
    """
    # Standardize values by stripping whitespace and converting to uppercase
    sap_value_processed = [str(po).strip().upper() for po in sap_value_list]
    extracted_value_processed = [str(po).strip().upper() for po in extracted_value_list]
    
    # Check for any overlap between extracted values and SAP values
    if not set(extracted_value_processed).isdisjoint(sap_value_processed):
        return build_validation_result(
            extracted_value={PO_NUMBER: extracted_value_list},
            is_anomaly=False,
            edit_operation=False,
            highlight=False,
            method="Automated",
            supporting_details={"Summary": summary + " Invoice PO Number matches SAP PO Number."}
        )
    
    # If no matches found, mark as anomaly
    return build_validation_result(
        extracted_value={PO_NUMBER: extracted_value_list},
        is_anomaly=True,
        edit_operation=True,
        highlight=True,
        method="Combined",
        supporting_details={"Summary": summary + " Extracted PO Number does not match SAP PO Number."}
    )