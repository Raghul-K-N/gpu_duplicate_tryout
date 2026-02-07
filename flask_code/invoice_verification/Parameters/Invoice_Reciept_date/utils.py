import pandas as pd
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, email_dict,global_pandas_date_parser
from invoice_verification.constant_field_names import INVOICE_RECEIPT_DATE
from typing import Dict, List, Optional
import re

import re

def _normalize_text_date(s: str) -> str:
    # Insert space after comma if missing: "Oct 27,2025" → "Oct 27, 2025"
    s = re.sub(r',(\d)', r', \1', s)
    return s

def pandas_date_parser(s: str, region: Optional[str] = None):
    return global_pandas_date_parser(s=s, region=region)

def get_payment_certificate_date(payment_certificate_lines) -> str:
    """
    This function checks for Invoice receipt date in the payment certificate lines
    Returns:
        Tuple[bool,str]: A tuple containing a boolean indicating if Invoice receipt date was found,
                        and the line content where it was found, or empty string if not found
    """
    keyword = "payment certificate"
    lines_text = " \n ".join(payment_certificate_lines).lower()

    if keyword in lines_text:
        if "dow sap shadow po no" in lines_text:
            if "invoice received date" in lines_text:
                match = re.search(r'invoice received date[:\s]*([^\n]+)', lines_text, re.IGNORECASE)
                if match:
                    date_str = match.group(1).strip()
                    try:
                        return str(pd.to_datetime(date_str).date())
                    except:
                        return ""
        match = re.search(r'payment certificate date[:\s]*([^\n]+)', lines_text, re.IGNORECASE)
        if match:
            date_str = match.group(1).strip()
            try:
                return str(pd.to_datetime(date_str).date())
            except:
                return ""
    return ""


def get_date_from_text_column(sap_text_field: str,
                              region: str):
    """
    This function takes the invoice 
    receipt date from the text column
    in SAP data
    """
    text_lines = [line.strip() for line in sap_text_field.split('\n') if line.strip()]
    for line in text_lines:
        text = line.lower().split()
        for word in text:
            parsed_date = pandas_date_parser(word, region=region)
            if parsed_date:
                return parsed_date
    return None

def extract_date_lines(eml_lines: List, mail_id_list: List) -> List:
    """
    Extract date lines from email content based on mail IDs.
    Returns lines that follow 'To:' lines containing specified mail IDs and start with 'Sent:'.
    """
    date_eml_list = []
    length = len(eml_lines)
    for i in range(length - 1):
        if (eml_lines[i].lower().startswith("to:") and 
            any((mail).strip().lower() in eml_lines[i].lower() for mail in (mail_id_list or []))):
            if ((i+2) < length) and eml_lines[i + 2].lower().startswith("sent:"):
                date_eml_list.append(eml_lines[i + 2].replace("sent:", "").strip())
            elif ((i+1) < length) and eml_lines[i + 1].lower().startswith("sent:"):
                date_eml_list.append(eml_lines[i + 1].replace("sent:", "").strip())
            elif ((i-1) < length) and eml_lines[i - 1].lower().startswith("sent:"):
                date_eml_list.append(eml_lines[i - 1].replace("sent:", "").strip())
    return date_eml_list


def get_email_dates(eml_lines: List, 
                    region: str):
    """
    This function gets the latest 
    date from Email File
    """
    region = "EMEAI" if str(region).strip().upper() in ["EMEA","EMEAI"] else region
    mail_id_list = email_dict.get(str(region).strip().upper(), [])

    # Extract date lines from email content
    log_message(f"Email lines for date extraction: {eml_lines}")
    date_eml_list = []
    if eml_lines:
        for lines in eml_lines:
            eml_lines_temp = [line.strip().lower() for line in lines if line.strip().lower()]
            # date_eml_list = [i.replace("sent:", "").strip() for i in eml_lines if "sent: " in i]
            date_eml_list = extract_date_lines(eml_lines_temp, mail_id_list)
            if date_eml_list:
                break
    
    # If no date lines found, return None
    if not date_eml_list:
        return None
    
    # Parse dates and find the latest one
    parsed_dates = [pandas_date_parser(date_str, region=region) for date_str in date_eml_list]
    valid_dates = [dt for dt in parsed_dates if dt is not None]
    if not valid_dates:
        return None
    return valid_dates

def get_latest_email_date(eml_lines: List, region: str):
    """
    This function gets the latest 
    date from Email File
    """
    email_dates = get_email_dates(eml_lines, region=region)
    if not email_dates:
        return None
    # Return the latest date
    latest_date = max(email_dates)
    return latest_date

def get_earliest_email_date(eml_lines: List, region: str):
    """
    This function gets the earliest 
    date from Email File
    """
    email_dates = get_email_dates(eml_lines, region=region)
    if not email_dates:
        return None
    # Return the earliest date
    earliest_date = min(email_dates)
    return earliest_date

def get_date_from_xml(xml_lines: List,
                      region: str):
    """
    This function gets the date value
    from XML data given"""
    for line in xml_lines:
        line = line.strip().lower()
        if "dhrecbto" in line:
            start_idx = line.find("dhrecbto") + len("dhrecbto")
            date_str = line[start_idx:].strip()
            parsed_date = pandas_date_parser(date_str, region=region)
            if parsed_date:
                return parsed_date #.strftime('%Y-%m-%d')
    return None

def shadow_po(sap_receipt_date: str,
                eml_lines: List,
                region: str
                ) -> Dict:
    """
    for shadow_po Invoices, we take the earliest date
    from the email file.
    """
    sap_receipt_date_processed = pandas_date_parser(sap_receipt_date, region=region)
    sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
    extracted_receipt_date = get_earliest_email_date(eml_lines=eml_lines, region=region)
    extracted_date = extracted_receipt_date if extracted_receipt_date else None
    log_message(f"Invoice receipt Date: sap value:{sap_date} and extracted value:{extracted_date}")

    if not extracted_date:
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                is_anomaly=None,
                                edit_operation=False,
                                highlight=False,
                                method="Combined",
                                supporting_details={"Summary":"No invoice receipt date found in email for Shadow PO invoice - Combined process"})
    
    # Validate
    anomaly_flag = extracted_date != sap_date if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag}")

    extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Combined",
                                supporting_details={"Summary":"Shadow PO invoice - Combined process"})


def emeai_naa_doc_type_xml(sap__receipt_date: str,
                            xml_lines: List,
                            region: str
                            ) -> Dict:
    """
    for Region EMEAI and NAA,
    for certain doctype, it will have XML data
    available, then we take the generated date
    as invoice receipt date
    """
    sap_receipt_date_processed = pandas_date_parser(sap__receipt_date, region=region)
    sap_date = sap_receipt_date_processed if sap_receipt_date_processed else None
    extracted_xml_date = get_date_from_xml(xml_lines=xml_lines, region=region)
    extracted_date = extracted_xml_date if extracted_xml_date else None
    log_message(f"Invoice Receipt Date: extracted date: {extracted_date} and sap date: {sap_date} for Parameter Invoice Receipt Date")

    if not extracted_date:
        return build_validation_result(extracted_value={INVOICE_RECEIPT_DATE: None},
                                    is_anomaly=None,
                                    edit_operation=False,
                                    highlight=False,
                                    method="Combined",
                                    supporting_details={"Summary":"No invoice receipt date found in XML for EMEAI/NAA doc type - Combined process"})
    
    # Validate
    anomaly_flag = extracted_date != sap_date if extracted_date and sap_date else False
    log_message(f"Anomaly Flag: {anomaly_flag} for Parameter Invoice Receipt Date")

    extracted_value = {INVOICE_RECEIPT_DATE: extracted_date}

    return build_validation_result(extracted_value=extracted_value,
                                is_anomaly=anomaly_flag,
                                edit_operation=False,
                                highlight=False,
                                method="Combined",
                                supporting_details={"Summary":"EMEAI/NAA doc type XML - Combined process"})

def get_aviation_date(invoice_lines: List) -> str|None:
    """
    This function checks for Aviation keyword
    in the invoice lines and returns the date
    if found
    """
    invoice_text = " \n ".join(invoice_lines).strip().lower()
    if "received" in invoice_text:
        received_idx = invoice_text.find("received")
        sliced_text = invoice_text[received_idx:]
        
        # Pattern: "by [1-5 words with alphanumeric/brackets] at [1-2 digits]:[1-2 digits] [am/pm], [month] [1-2 day digits], [4-digit year]"
        pattern = r'by\s+(.+?)\s+at\s+\d{1,2}\s*:\s*\d{1,2}\s*(?:am|pm)?\s*,?\s*((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{1,2}\s*,\s*\d{4})'

        match = re.search(pattern, sliced_text, re.IGNORECASE)
        log_message(f"Aviation Matched String: {match}")
        if match:
            date_str = match.group().strip().split("am")[-1].split("pm")[-1].replace(","," ").strip()
            log_message(f"Extracted Aviation Date String: {date_str}")
            parsed_date = pandas_date_parser(date_str)
            log_message(f"Parsed Aviation Date: {parsed_date}")
            return parsed_date.strftime('%Y-%m-%d') if parsed_date else None
    log_message("No Aviation date found in invoice lines")
    return None