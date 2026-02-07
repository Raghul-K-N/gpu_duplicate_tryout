from typing import Optional, Dict, List
from invoice_verification.logger.logger import log_message
from .utils import pandas_date_parser, get_earliest_email_date, get_date_from_xml, get_latest_email_date, get_date_from_text_column
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result, check_parameter_in_vim_comments
from invoice_verification.constant_field_names import INVOICE_RECEIPT_DATE

def validate_global_invoice_receipt_date(sap_row: SAPRow, 
                                        payment_certificate_receipt_date: str|None,
                                        invoice_extracted_receipt_date : str|None, 
                                        invoice_extracted_invoice_date: str|None,
                                        voucher_extracted_receipt_date: str|None
                                        ) -> Dict:
    """
    Validate the Invoice Receipt Date based on Standard Validation,
    That is based on SAP value to Invoice extracted Value.
    """
    log_message(f"Started Global level exceptions validation for Parameter Invoice Receipt Date")

    # Initializing Required Variables
    region = str(sap_row.region).strip().upper()
    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    sap_receipt_date = str(sap_row.invoice_receipt_date) 
    sap_invoice_date = str(sap_row.invoice_date)
    sap_text_field = str(sap_row.text_field)
    eml_lines = list(sap_row.eml_lines) if sap_row.eml_lines else []
    xml_lines = list(sap_row.xml_lines) if sap_row.xml_lines else []
    vim_comment_lines: List = sap_row.vim_comment_lines if sap_row.vim_comment_lines else []

    if (doc_type == "KS" and dp_doc_type in ["NPO_RP_GLB","PO_RP_GLB"]) and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter Invoice Receipt Date")
        return check_parameter_in_vim_comments(parameter=INVOICE_RECEIPT_DATE,
                                                parameter_value=sap_receipt_date,
                                                sap_vim_comment_lines=vim_comment_lines)
    
    # Script Invoice Exception
    if str(doc_type).strip().upper() == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Started Script invocie KE doc type exception handling for Parameter Invoice Receipt Date")
        result = script_ke(sap_receipt_date=sap_receipt_date,
                            eml_lines=eml_lines,
                            region=region)
        return result
    
    # Electronic Invoice Exception
    if xml_lines and ((sap_row.ariba_flag or sap_row.elemica_flag) or str(dp_doc_type).strip().upper() in ["FR_EI_AUT", "PO_EI_AUT", "NPO_EI_AUT"] \
                or str(sap_row.channel_id).strip().upper() == "OCR_ML" or dp_doc_type in ["PO_NFE_BRA","FR_CTE_BRA"]):
        log_message("Started Electronic invocie doc type exception handling for Parameter Invoice Receipt Date")
        if invoice_extracted_receipt_date:
            extracted_date = invoice_extracted_receipt_date
        elif voucher_extracted_receipt_date:
            extracted_date = voucher_extracted_receipt_date
        elif payment_certificate_receipt_date:
            extracted_date = payment_certificate_receipt_date
        else:
            extracted_date = None
        result = electronic_doctype(sap_receipt_date=sap_receipt_date,
                                    xml_lines=xml_lines,
                                    region=region,
                                    extracted_receipt_date=extracted_date)
        return result
    
    # TODO:Return to Vendor and Resubmitted Invoice Identification
    if sap_row.return_to_vendor or sap_row.resubmitted_invoice:
        log_message("Started Return to Vendor invoice exception handling for Parameter Invoice Receipt Date")
        result = return_to_vendor_resubmitted_invoice(eml_lines=eml_lines,
                                                      region=region)
        return result
    
    # OWAD/ Service Exceptions
    if (xml_lines or eml_lines) or str(sap_row.channel_id).strip().upper().startswith("OAWD"):
        log_message("Started OAWD/ Service Exception Handling for Parameter Invoice Receipt Date")
        if invoice_extracted_invoice_date:
            extracted_receipt_date = invoice_extracted_receipt_date
        elif voucher_extracted_receipt_date:
            extracted_receipt_date = voucher_extracted_receipt_date
        elif payment_certificate_receipt_date:
            extracted_receipt_date = payment_certificate_receipt_date
        else:
            extracted_receipt_date = None
        result = oawd_service_validation(sap_text_field=sap_text_field,
                                        xml_lines=xml_lines,
                                        sap_receipt_date=sap_receipt_date,
                                        eml_lines=eml_lines,
                                        region=region,
                                        extracted_receipt_date=extracted_receipt_date)
        return result
    
    if dp_doc_type in ["PO_EC_GLB","NPO_EC_GLB"] and str(sap_row.region).strip().upper() not in ['EMEAI','EMEA']:
        log_message("Invoice is Attached Special-case Handling for DP DOCTYPE-PO_EC_GLB in EMEAI region started for Parameter INVOICE RECEIPT DATE")
        return check_parameter_in_vim_comments(parameter=INVOICE_RECEIPT_DATE,
                                                parameter_value=sap_receipt_date,
                                                sap_vim_comment_lines=vim_comment_lines)
    
    if doc_type == "KE" and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KE started for Parameter Invoice Receipt Date")
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                        is_anomaly=None,
                                        highlight=True,
                                        edit_operation=True,
                                        method="Combined",
                                        supporting_details={"Summary":"DOCTYPE-KE requires Combined verification."})
    
    # Voucher Invoice Exception
    if voucher_extracted_receipt_date:
        log_message("Started Voucher invoice exception handling for Parameter Invoice Receipt Date")
        result = voucher_date_validation(sap_receipt_date=sap_receipt_date,
                                        voucher_extracted_receipt_date=voucher_extracted_receipt_date,
                                        region=region)
        return result
    

    
    result = standard_validation(sap_invoice_date=sap_invoice_date,
                                sap_receipt_date=sap_receipt_date,
                                payment_certificate_receipt_date=payment_certificate_receipt_date,
                                invoice_extracted_invoice_date=invoice_extracted_invoice_date,
                                invoice_extracted_receipt_date=invoice_extracted_receipt_date,
                                voucher_extracted_receipt_date=voucher_extracted_receipt_date,
                                eml_lines=eml_lines,
                                region=region)
    return result


def earliest_email_date_validation(sap_receipt_date: str,
                                    eml_lines: List,
                                    region: str
                                    ) -> Dict:
    """
    get the earliest email date and
    do validation
    """
    sap_receipt_date_processed = pandas_date_parser(s=sap_receipt_date, region=region)
    sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
    email_date = get_earliest_email_date(eml_lines=eml_lines,
                                         region=region)
    extracted_date = email_date if email_date else None
    log_message(f"Invoice Receipt Date: extracted date: {extracted_date} and sap date: {sap_date} for Parameter Invoice Receipt Date")

    if not extracted_date:
        # No extracted invoice receipt date; return an explicit empty result with low confidence
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                    is_anomaly=None,
                                    edit_operation=False,
                                    highlight=False,
                                    method="Automated",
                                    supporting_details={"Summary":"No invoice receipt date found in email for Shadow PO invoice - Automated process"})
    
    # Validate
    anomaly_flag = extracted_date.strftime("%Y-%m-%d") != sap_date.strftime("%Y-%m-%d") if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

    extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Shadow PO invoice - Automated process"})


def script_ke(sap_receipt_date: str,
            eml_lines: List,
            region: str
            ) -> Dict:
    """
    for script invoices with doc type KE,
    we take the oldest date from email and
    validate it to SAP data.
    """
    result = earliest_email_date_validation(sap_receipt_date=sap_receipt_date,
                                        eml_lines=eml_lines,
                                        region=region)
    result["edit_operation"] = True
    return result


def electronic_doctype(sap_receipt_date: str,
                        xml_lines: List,
                        region: str,
                        extracted_receipt_date: str|None
                        ) -> Dict:
    """
    for electronic invoices with doc type KE,
    we take the date from XML data.
    """
    sap_receipt_date_processed = pandas_date_parser(sap_receipt_date, region=region)
    sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
    extracted_xml_date = get_date_from_xml(xml_lines=xml_lines, region=region)
    if extracted_xml_date is not None:
        extracted_date = extracted_xml_date
    else:
        extracted_date = pandas_date_parser(extracted_receipt_date, region=region) if extracted_receipt_date else None
    log_message(f"Invoice Receipt Date: extracted date: {extracted_date} and sap date: {sap_date} for Parameter Invoice Receipt Date")

    if not extracted_date:
        # No extracted invoice receipt date; return an explicit empty result with low confidence
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                            is_anomaly=None,
                            edit_operation=False,
                            highlight=False,
                            method="Automated",
                            supporting_details={"Summary":"Electronic invoice - Automated process"})
    
    # Validate
    anomaly_flag = extracted_date.strftime("%Y-%m-%d") != sap_date.strftime("%Y-%m-%d") if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

    extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Electronic invoice - Automated process"})


def voucher_date_validation(sap_receipt_date: str, 
                            voucher_extracted_receipt_date: str,
                            region: str
                            ) -> Dict:
    """
    For Voucher invoices exception,
    Process and validate based on 
    VOUCHER COPY.
    """
    sap_receipt_date_processed = pandas_date_parser(sap_receipt_date, region=region)
    sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
    extracted_date_processed = pandas_date_parser(voucher_extracted_receipt_date, region=region)
    extracted_date = extracted_date_processed if extracted_date_processed else None
    log_message(f"Invoice Receipt Date: extracted date: {extracted_date} and sap date: {sap_date} for Parameter Invoice Receipt Date")
    
    # Validate
    anomaly_flag = extracted_date.strftime("%Y-%m-%d") != sap_date.strftime("%Y-%m-%d") if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

    extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Voucher invoice - Automated process"})


def return_to_vendor_resubmitted_invoice(eml_lines: List,
                                         region: str
                                         ) -> Dict:
    """
    For Return to Vendor invoices exception,
    Process based on latest date from email file
    and manual validation.
    """
    extracted_email_date = get_latest_email_date(eml_lines=eml_lines, region=region)
    latest_email_date = extracted_email_date if extracted_email_date else None
    log_message(f"Latest Email Date: {latest_email_date} for Parameter Invoice Receipt Date")
    
    if not latest_email_date:
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                is_anomaly=None,
                                edit_operation=False,
                                highlight=True,
                                method="Manual",
                                supporting_details={"Summary":"Return to Vendor invoice - Manual process"})
    
    extracted_value = {INVOICE_RECEIPT_DATE: latest_email_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=None,
                                edit_operation=False,
                                highlight=True,
                                method="Manual",
                                supporting_details={"Summary":"Return to Vendor invoice - Manual process"})


def oawd_service_validation(sap_text_field: str, 
                            xml_lines: List,
                            sap_receipt_date: str,
                            eml_lines: List,
                            region: str,
                            extracted_receipt_date: str|None
                            ) -> Dict:
    """
    For OWAD/ Service Postings, if email file present then
    we take earliest date from email, if email not present,
    then check in SAP Text column
    Text column date format:
    Format: Ticket Number + Invoice receipt date
    """
    if xml_lines and "dhrecbto" in "".join([line.lower() for line in xml_lines]):
        log_message("XML File based Processing Strated for Parameter Invoice Receipt Date")
        return electronic_doctype(sap_receipt_date=sap_receipt_date,
                                xml_lines=xml_lines,
                                region=region,
                                extracted_receipt_date=extracted_receipt_date)
    elif eml_lines:
        log_message("Email File based Processing Strated for Parameter Invoice Receipt Date")
        return earliest_email_date_validation(sap_receipt_date=sap_receipt_date,
                                            eml_lines=eml_lines,
                                            region=region)
    else:
        sap_receipt_date_processed = pandas_date_parser(sap_receipt_date, region=region)
        sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
        # extracted_date = str(get_date_from_text_column(sap_text_field=sap_text_field, date_format=date_format))
        text_extracted_date = get_date_from_text_column(sap_text_field=sap_text_field, region=region)
        if text_extracted_date is not None:
            extracted_date = text_extracted_date
        else:
            extracted_date = pandas_date_parser(extracted_receipt_date) if extracted_receipt_date else None
        log_message(f"Invoice Receipt Date: extracted date: {extracted_date} and sap date: {sap_date} for Parameter Invoice Receipt Date")

        if not extracted_date:
            return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                    is_anomaly=None,
                                    edit_operation=False,
                                    highlight=False,
                                    method="Combined",
                                    supporting_details={"Summary":"OAWD requires Email/XML File or any Supporting document for verification."})
        
        # Validate
        anomaly_flag = extracted_date.strftime("%Y-%m-%d") != sap_date.strftime("%Y-%m-%d") if extracted_date and sap_date else False
        log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

        extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

        return build_validation_result(extracted_value=extracted_value,
                                    is_anomaly=anomaly_flag,
                                    edit_operation=False,
                                    highlight=False,
                                    method="Combined",
                                    supporting_details={"Summary":"OAWD - Combined process"})


def standard_validation(sap_invoice_date: str, 
                        sap_receipt_date: str,
                        payment_certificate_receipt_date: str|None,
                        invoice_extracted_invoice_date: str|None,
                        invoice_extracted_receipt_date: str|None,
                        voucher_extracted_receipt_date: str|None,
                        eml_lines: List,
                        region: str
                        ) -> Dict:
    """
    Standard validation for Invoice Receipt Date.
    
    Logic:
    1. Parse SAP invoice and receipt dates
    2. Parse extracted invoice date and receipt date from invoice copy
    3. Get earliest email date from email lines
    4. Compare invoice receipt date with email date, select the earliest
    5. If both are None, use extracted invoice date
    6. Compare selected date with SAP invoice date, select the latest
    7. Validate against SAP receipt date and return anomaly flag
    
    Args:
        sap_invoice_date: Invoice date from SAP
        sap_receipt_date: Receipt date from SAP
        invoice_extracted_invoice_date: Invoice date extracted from invoice copy
        invoice_extracted_receipt_date: Receipt date extracted from invoice copy
        eml_lines: Email lines for date extraction
        region: Region for date parsing
        date_format: Date format string
        
    Returns:
        Dict containing validation result with extracted value, anomaly flag, and metadata
    """

    log_message("Started Standard Validation for Parameter Invoice Receipt Date")
    log_message(f"SAP Invoice Date: {sap_invoice_date} and SAP Receipt Date: {sap_receipt_date} for Parameter Invoice Receipt Date")
    log_message(f"Invoice Extracted Invoice Date: {invoice_extracted_invoice_date} and Invoice Extracted Receipt Date: {invoice_extracted_receipt_date} for Parameter Invoice Receipt Date")

    # parse both extracted and SAP dates using pandas for robustness
    # Store the function result in variables,and then apply strftime on them if not None
    parsed_invoice_date = pandas_date_parser(sap_invoice_date, region=region)
    parsed_receipt_date = pandas_date_parser(sap_receipt_date, region=region)

    log_message(f"Parsed SAP Invoice Date: {parsed_invoice_date} and Parsed SAP Receipt Date: {parsed_receipt_date} for Parameter Invoice Receipt Date")
    # if invoice receipt date present in invoice copy, take it if not consider invoice extracted invoice receipt date

    extracted_invoice_date = pandas_date_parser(invoice_extracted_invoice_date, region=region) if invoice_extracted_invoice_date else None
  
    invoice_receipt_date = pandas_date_parser(invoice_extracted_receipt_date, region=region) if invoice_extracted_receipt_date else None

    voucher_receipt_date = pandas_date_parser(voucher_extracted_receipt_date, region=region) if voucher_extracted_receipt_date else None
    
    log_message(f"Parsed Invoice Extracted Invoice Date: {extracted_invoice_date} and Parsed Invoice Extracted Receipt Date: {invoice_receipt_date} for Parameter Invoice Receipt Date")
    
    payment_certificate_receipt_date_processed = pandas_date_parser(payment_certificate_receipt_date, region=region) if payment_certificate_receipt_date else None
    log_message(f"Payment Certificate Receipt Date found: {payment_certificate_receipt_date} for Parameter Invoice Receipt Date")

    parsed_email_date = get_earliest_email_date(eml_lines=eml_lines,
                                                region=region)
    email_date = parsed_email_date if parsed_email_date else None # get_earliest_email_date already returns String only
    log_message(f"Email Receipt Date: {email_date} for Parameter Invoice Receipt Date")


    log_message("Comparing Invoice Receipt Date, Email Date and Invoice Date for Parameter Invoice Receipt Date")
    log_message(f"Values are Invoice Receipt Date: {invoice_receipt_date}, Email Date: {email_date}, Invoice Date: {extracted_invoice_date} for Parameter Invoice Receipt Date")
    
    if payment_certificate_receipt_date_processed:
        extracted_value = payment_certificate_receipt_date_processed
        log_message(f"Payment Certificate Receipt Date is present. Taking Payment Certificate Receipt Date: {extracted_value} for Parameter Invoice Receipt Date")
    # elif invoice_receipt_date and email_date:
    #     # compare invoice extracted receipt date with email date and pick the oldest
    #     # Convert email_date string to Timestamp for comparison
    #     email_date_parsed = pandas_date_parser(str(email_date), region=region)
    #     extracted_value = invoice_receipt_date if (email_date_parsed and invoice_receipt_date < email_date_parsed) else email_date
    #     log_message(f"Oldest Date between invoice receipt date and email date is {extracted_value} for Parameter Invoice Receipt Date")
    elif voucher_receipt_date:
        extracted_value = voucher_receipt_date
        log_message(f"Date selected as voucher Receipt Date: {extracted_value} for Parameter Invoice Receipt Date")
    elif email_date:
        extracted_value = email_date
        log_message(f"Date selected as Email Receipt Date: {extracted_value} for Parameter Invoice Receipt Date")
    elif invoice_receipt_date:
        extracted_value = invoice_receipt_date
        log_message(f"Date selected as Invoice Receipt Date: {extracted_value} for Parameter Invoice Receipt Date")
    else:
        log_message("No Extracted Value found")
        extracted_value = extracted_invoice_date
        log_message(f"Both Invoice Receipt Date and Email Date are None. Taking Invoice Date: {extracted_value} for Parameter Invoice Receipt Date")

    log_message(f"Date selected after comparing Invoice Receipt Date, Email Date and Invoice Date is {extracted_value} for Parameter Invoice Receipt Date")

    if extracted_value is not None and extracted_invoice_date is not None:
        log_message(f"Extracted receipt date: {extracted_value} and extracted invoice date: {extracted_invoice_date} Pick the oldest date")
        extracted_value = extracted_value if extracted_value < extracted_invoice_date else extracted_invoice_date

    if extracted_value is None and parsed_invoice_date:
        extracted_value = parsed_invoice_date
        log_message(f"Extracted Date was None. Taking SAP Invoice Date: {extracted_value} for Parameter Invoice Receipt Date")

    log_message(f"Final extracted date is {extracted_value} for Parameter Invoice Receipt Date")

    # Validate
    anomaly_flag = extracted_value.strftime("%Y-%m-%d") != parsed_receipt_date.strftime("%Y-%m-%d") if extracted_value and parsed_receipt_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

    extracted_date = {INVOICE_RECEIPT_DATE: extracted_value}

    return build_validation_result(extracted_value=extracted_date,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Automated",
                                supporting_details={"Summary":"Standard Validation - Automated process"})