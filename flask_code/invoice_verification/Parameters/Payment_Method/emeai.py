from typing import Dict
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Parameters.constants import POLAND_COMPANY_CODES, MOROCCO_COMPANY_CODES, SOUTH_AFRICA_COMPANY_CODES
from invoice_verification.constant_field_names import PAYMENT_METHOD
from invoice_verification.Schemas.sap_row import SAPRow

def validate_emeai_payment_method(sap_row: SAPRow,
                                    invoice_extracted_payment_method: str|None,
                                    voucher_extracted_payment_method: str|None
                                    ) -> Dict:
    """
    Validate EMEAI based Exceptions
    """
    log_message("EMEAI region based Exception Handling started for Parameter Payment method")

    company_code = str(sap_row.company_code).strip().upper()
    vendor_code = str(sap_row.vendor_code).strip().upper()
    sap_payment_method = str(sap_row.payment_method).strip().upper()
    sap_payment_method_supplement = str(sap_row.payment_method_supplement).strip().upper()
    
    if company_code in [str(code).strip().upper() for code in POLAND_COMPANY_CODES] and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Poland based exception Handling Started for Parameter Payment Method")
        return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Manual",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":"No invoice or voucher copy present for Poland Company Codes"})
    
    if company_code in ["1485","151","1462"]:
        log_message(f"Company Code - {company_code} based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Automated process for Company Codes: {company_code}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Anomaly detected for Company Codes: {company_code}"})
        
    if company_code == "921" and vendor_code == "1590807":
        log_message(f"India based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Automated process for India Vendor Code: {vendor_code}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Anomaly detected for India Vendor Code: {vendor_code}"})

    if (company_code == "921") or \
        (company_code in [str(code).strip().upper() for code in MOROCCO_COMPANY_CODES]) or \
        (company_code in [str(code).strip().upper() for code in SOUTH_AFRICA_COMPANY_CODES]):
        log_message(f"ForEx based Exception handling Started for parameter Payment Method")
        if (sap_payment_method == "M") and (sap_payment_method_supplement == "$M"):
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=False,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Automated process for India Vendor Code: {vendor_code}"})
        else:
            return build_validation_result(extracted_value={PAYMENT_METHOD:invoice_extracted_payment_method},
                                        is_anomaly=True,
                                        highlight=False,
                                        edit_operation=False,
                                        method="Automated",
                                        supporting_details={"SAP payment_method_supplement":str(sap_row.payment_method_supplement),
                                                            "SAP payment_method_description":str(sap_row.payment_method_description),
                                                            "SAP Payment method Code": str(sap_row.payment_method),
                                                            "Summary":f"Anomaly detected for India Vendor Code: {vendor_code}"})
    
    # No EMEAI region exceptions were met, so call 
    from .global_logic import validate_global_payment_method
    return validate_global_payment_method(
        sap_row=sap_row,
        invoice_extracted_payment_method=invoice_extracted_payment_method,
        voucher_extracted_payment_method=voucher_extracted_payment_method
    )