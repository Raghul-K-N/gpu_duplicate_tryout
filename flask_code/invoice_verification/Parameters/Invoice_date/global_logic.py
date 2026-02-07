from .utils import pandas_date_parser
from typing import Dict, List
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments
from invoice_verification.constant_field_names import INVOICE_DATE
from invoice_verification.Schemas.sap_row import SAPRow


def   validate_global_invoice_date(sap_row: SAPRow, 
                                   invoice_extracted_invoice_date: str|None,
                                   voucher_extracted_invoice_date: str|None
                                   ) -> Dict:
    """
    Validate the Invoice date based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message("Global invoice date validation for Parameter Invoice Date")
    region:str = str(sap_row.region).strip().upper()
    doc_type: str = str(sap_row.doc_type).strip().upper()
    dp_doc_type: str = str(sap_row.dp_doc_type).strip().upper()
    sap_invoice_date: str = str(sap_row.invoice_date).strip().upper()
    vim_comment_lines: List = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter Invoice Date")
        return build_validation_result(extracted_value={INVOICE_DATE: None,},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details={"Summary":"DOCTYPE-KE requires Combined verification."})
    
    if dp_doc_type in ["PO_EC_GLB","NPO_EC_GLB"] and str(sap_row.region).strip().upper() not in ['EMEAI','EMEA'] and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DP DOCTYPE-PO_EC_GLB in EMEAI region started for Parameter INVOICE DATE")
        return check_parameter_in_vim_comments(parameter=INVOICE_DATE,
                                                parameter_value=sap_invoice_date,
                                                sap_vim_comment_lines=vim_comment_lines)

    if (doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter INVOICE DATE")
        return check_parameter_in_vim_comments(parameter=INVOICE_DATE,
                                                parameter_value=sap_invoice_date,
                                                sap_vim_comment_lines=vim_comment_lines)

    if invoice_extracted_invoice_date or voucher_extracted_invoice_date:
        log_message("Started Standard Validation for Parameter Invoice Date")
        result = standard_validation(sap_invoice_date=sap_invoice_date, 
                                     invoice_extracted_invoice_date=invoice_extracted_invoice_date,
                                     voucher_extracted_invoice_date=voucher_extracted_invoice_date,
                                     region=region
                                     )
        return result

    # No extracted invoice date; return an explicit empty result with low confidence
    return build_validation_result(extracted_value={INVOICE_DATE: None},
                        is_anomaly=None,
                        edit_operation=True,
                        highlight=True,
                        method="Combined",
                        supporting_details={"Summary":"No invoice date available in Document."})


def standard_validation(sap_invoice_date: str, 
                        invoice_extracted_invoice_date: str|None,
                        voucher_extracted_invoice_date: str|None,
                        region: str
                        ) -> Dict:
    """
    Usuual SAP to Invoice Validation
    """
    # parse both extracted and SAP dates using pandas for robustness
    extracted_date = None
    invoice_extracted_processed_date = pandas_date_parser(invoice_extracted_invoice_date, region=region) if invoice_extracted_invoice_date else None
    voucher_extracted_processed_date = pandas_date_parser(voucher_extracted_invoice_date, region=region) if voucher_extracted_invoice_date else None
    log_message(f"Processed Invoice Extracted Date: {invoice_extracted_processed_date}, Processed Voucher Extracted Date: {voucher_extracted_processed_date} for Parameter Invoice Date")
    if invoice_extracted_processed_date:
        extracted_date = invoice_extracted_processed_date
    elif voucher_extracted_processed_date:
        extracted_date = voucher_extracted_processed_date

    processed_sap = pandas_date_parser(sap_invoice_date, region=region)
    sap_date = processed_sap if processed_sap else None
    log_message(f"Invoice date: [Extracted Value: {extracted_date}, SAP Value: {sap_date} ] for Parameter Invoice Date")

    # Validate
    anomaly_flag = extracted_date != sap_date if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Date")

    extracted_invoice_date = {INVOICE_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_invoice_date,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Standard Validation."})