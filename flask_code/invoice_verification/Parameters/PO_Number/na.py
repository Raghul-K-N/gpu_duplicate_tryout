# Parameters/Invoice Number/na.py
"""
NAA region-specific PO number validation logic
"""
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.constant_field_names import PO_NUMBER
import re
from .global_logic import standard_validation_po_number_logic

def validate_naa_po_number(extracted_value: list, 
                           sap_value: list,
                           po_accounting_group: str
                           ) -> dict:
    """
    NAA region PO number validation with region-specific rules

    Args:
        extracted_value: Invoice number from OCR extraction
        sap_value: SAPRow object with all SAP data
    
    Returns:
        Dict with standardized output format
    """
    log_message("Validating NAA PO number")
    log_message(f"extracted PO number: {extracted_value}, SAP PO number: {sap_value}")

    summary: str = ""
    
    # Check for empty extracted value
    if extracted_value==[]:
        log_message("NAA: No extracted po number")
        return build_validation_result(
            extracted_value={PO_NUMBER: None},
            is_anomaly=None,
            edit_operation=True,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"No po number available."}
        )
    
    
    # if not set(extracted_value_processed).isdisjoint(sap_value):
    #     return build_validation_result(
    #         extracted_value={PO_NUMBER:extracted_value},
    #         is_anomaly=False,
    #         edit_operation=False,
    #         highlight=False,
    #         method="Automated",
    #         supporting_details={}
    #     )
    
    # if PO is not in ZVG1 accounting group, mark as anomaly
    if po_accounting_group != "ZVG1":
        summary = f"NAA: PO accounting group is {po_accounting_group}"
        return standard_validation_po_number_logic(sap_value_list=sap_value,
                                                   extracted_value_list=extracted_value,
                                                   summary=summary)
    
    if all(bool(len(po)==10) for po in extracted_value):
        # Pattern matches standard PO formats, use global logic for validation
        if any(str(po).startswith("45") for po in extracted_value):
            log_message("NAA: PO number matches standard 10-digit formats starting with 45")
            summary = "NAA: PO number 45xxxxxx 10-digit Series Detected"
        if any(str(po).startswith("M41") for po in extracted_value):
            log_message("NAA: PO number matches standard 10-digit formats starting with M41")
            summary = "NAA: PO number M41xxxxxx 10-digit Series Detected"
        if any(str(po).startswith("4101") for po in extracted_value):
            log_message("NAA: PO number matches standard 10-digit formats starting with 4101")
            summary = "NAA: PO number 4101xxxxxx 10-digit Series Detected"
        if any(str(po).startswith("4102") for po in extracted_value):
            log_message("NAA: PO number matches standard 10-digit formats starting with 4102")
            summary = "NAA: PO number 4102xxxxxx 10-digit Series Detected"
        
        log_message("NAA: PO number matches standard 10-digit formats starting")
        return standard_validation_po_number_logic(sap_value_list=sap_value, 
                                                   extracted_value_list=extracted_value,
                                                   summary=summary)
    
    if all(bool(len(str(po).strip())==9) for po in extracted_value):
        # Pattern matches DND/DCR/CED formats, use global logic for validation
        log_message("NAA: PO number matches DND/DCR/CED 9-digit formats, applying global logic")
        if any(re.search(r'\bDND\d{6}\b', str(po).strip().upper()) for po in extracted_value):
            log_message("NAA: PO number matches DNDxxxxxx 9-digit Series")
            summary = "NAA: PO number DNDxxxxxx 9-digit Series Detected"
        if any(re.search(r'\bDCR\d{6}\b', str(po).strip().upper()) for po in extracted_value):
            log_message("NAA: PO number matches DCRxxxxxx 9-digit Series")
            summary = "NAA: PO number DCRxxxxxx 9-digit Series Detected"
        if any(re.search(r'\bCED\d{6}\b', str(po).strip().upper()) for po in extracted_value):
            log_message("NAA: PO number matches CEDxxxxxx 9-digit Series")
            summary = "NAA: PO number CEDxxxxxx 9-digit Series Detected"

        return standard_validation_po_number_logic(sap_value_list=sap_value, 
                                                   extracted_value_list=extracted_value,
                                                   summary=summary)
    
    if all(bool(len(str(po).strip())==8 and (str(po).startswith("230"))) for po in extracted_value):
        log_message("NAA: PO number matches 8-digit format starting with 230, applying global logic")
        summary = "NAA: PO number 230xxxxx 8-digit Series Detected"
        return standard_validation_po_number_logic(sap_value_list=sap_value, 
                                                   extracted_value_list=extracted_value,
                                                   summary=summary)
    
    if all(bool(len(str(po).strip())==6) for po in extracted_value):
        log_message("NAA: PO number matches 6-digit format starting with 1-6, applying global logic")
        if any(str(po).startswith("1") for po in extracted_value):
            summary = "NAA: PO number 1xxxxx 6-digit Series Detected"
        if any(str(po).startswith("2") for po in extracted_value):
            summary = "NAA: PO number 2xxxxx 6-digit Series Detected"
        if any(str(po).startswith("3") for po in extracted_value):
            summary = "NAA: PO number 3xxxxx 6-digit Series Detected"
        if any(str(po).startswith("4") for po in extracted_value):
            summary = "NAA: PO number 4xxxxx 6-digit Series Detected"
        if any(str(po).startswith("5") for po in extracted_value):
            summary = "NAA: PO number 5xxxxx 6-digit Series Detected"
        if any(str(po).startswith("6") for po in extracted_value):
            summary = "NAA: PO number 6xxxxx 6-digit Series Detected"
        
        return standard_validation_po_number_logic(sap_value_list=sap_value, 
                                                   extracted_value_list=extracted_value,
                                                   summary=summary)
    
    # If none of the patterns matched, Check for any overlap using global logic
    log_message("NAA: PO number does not match any specific patterns, applying global logic for overlap check")
    return standard_validation_po_number_logic(sap_value_list=sap_value, extracted_value_list=extracted_value)