from typing import Dict, List
from invoice_verification.logger.logger import log_message
from .utils import shadow_po, emeai_naa_doc_type_xml, get_aviation_date, pandas_date_parser
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Parameters.utils import build_validation_result, kc_ka_doctype_common_handling, check_parameter_in_vim_comments
from invoice_verification.constant_field_names import INVOICE_RECEIPT_DATE

def validate_naa_invoice_receipt_date(sap_row: SAPRow, 
                                    payment_certificate_receipt_date: str|None,
                                    invoice_extracted_receipt_date: str|None,
                                    invoice_extracted_invoice_date: str|None,
                                    voucher_extracted_receipt_date: str|None
                                    ) -> Dict:
    """NAA-specific invoice receipt_date extraction logic"""

    log_message(f"Inside NAA based Exceptions for Parameter Invoice Receipt Date")
    region = str(sap_row.region).strip().upper()

    doc_type = str(sap_row.doc_type).strip().upper()
    dp_doc_type = str(sap_row.dp_doc_type).strip().upper()
    vim_comment_lines: List = sap_row.vim_comment_lines if isinstance(sap_row.vim_comment_lines, list) else []
    eml_lines: List = sap_row.eml_lines if isinstance(sap_row.eml_lines, list) else []
    invoice_lines: List = sap_row.invoice_lines if isinstance(sap_row.invoice_lines, list) else []
    aviation_date: str|None = get_aviation_date(invoice_lines=invoice_lines)
    po_numbers = sap_row.po_number if isinstance(sap_row.po_number, list) else []

    shadow_po_number_check = any(str(po).strip().startswith("414") or str(po).strip().startswith("5") for po in po_numbers)

    # Invoice is Attached Special-case Handling
    if doc_type in ["KA","KC"] and not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KA and KC started for Parameter INVOICE CURRENCY")
        return kc_ka_doctype_common_handling(parameter=INVOICE_RECEIPT_DATE)

    elif doc_type == "KS" and dp_doc_type in ["NPO_EC_GLB","PO_EC_GLB"] and ''.join(vim_comment_lines).strip() != "" and \
            not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag):
        log_message("Invoice is Attached Special-case Handling for DOCTYPE-KS started for Parameter INVOICE CURRENCY")
        return check_parameter_in_vim_comments(parameter=INVOICE_RECEIPT_DATE,
                                                parameter_value=sap_row.invoice_receipt_date,
                                                sap_vim_comment_lines=vim_comment_lines)

    if eml_lines and ((str(sap_row.order_type).strip().upper() == "SHADOW PO") \
            or (str(sap_row.transaction_code).strip().upper() == "ME23N") \
            or shadow_po_number_check):
            # and (str(sap_row.po_number).strip().startswith("414") or (str(sap_row.po_number).strip().startswith("5"))):
        log_message(f"Shadow-PO Exception Handlding Started for Parameter Invoice Receipt Date")
        result = shadow_po(sap_receipt_date=sap_row.invoice_receipt_date,
                            eml_lines=eml_lines,
                            region=region)
        return result

    # NOTE: AVIATION Identification
    elif aviation_date is not None:
        log_message("Aviation Exception handling: Manual process for Parameter Invoice Receipt Date")
        sap_receipt_date = pandas_date_parser(sap_row.invoice_receipt_date, region)
        extracted_receipt_date = pandas_date_parser(aviation_date)
        if sap_receipt_date is not None and extracted_receipt_date is not None and sap_receipt_date != extracted_receipt_date:
            is_anomaly = True
        else:
            is_anomaly = False
        return build_validation_result(
                extracted_value={INVOICE_RECEIPT_DATE: str(extracted_receipt_date)},
                is_anomaly=is_anomaly,
                edit_operation=False,
                highlight=True,
                method='Manual',
                supporting_details={"Summary":"Aviation invoice identified - Manual process"}
            )
    
    # NOTE: GULFSTREAM Identification
    elif not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag) and (str(sap_row.transaction_code).strip().upper() == "ME23N" \
            and str(sap_row.order_type).strip().upper() == "SHADOW PO (PATH2ZERO)"):
        log_message("Gulfstream Exception handling: Manual process for Parameter Invoice Receipt Date")
        return build_validation_result(
                extracted_value={INVOICE_RECEIPT_DATE: None},
                is_anomaly=None,
                edit_operation=False,
                highlight=True,
                method='Manual',
                supporting_details={"Summary":"Gulfstream invoice identified - Manual process"}
            )
    
    # NAA doc type based XML Exception
    elif (doc_type == "KD" and dp_doc_type == "PO_CS_GLB"):
        log_message("Started NAA doc type based XML Exception handling for Parameter Invoice Receipt Date")
        result = emeai_naa_doc_type_xml(sap__receipt_date=sap_row.invoice_receipt_date,
                                        xml_lines=sap_row.xml_lines,
                                        region=region)
        return result
    
    # No NAA based Exceptions were met
    log_message("No NAA based Exceptions were met for Parameter Invoice Receipt date")
    from .global_logic import validate_global_invoice_receipt_date
    return validate_global_invoice_receipt_date(
            sap_row=sap_row,
            payment_certificate_receipt_date=payment_certificate_receipt_date,
            invoice_extracted_invoice_date=invoice_extracted_invoice_date,
            invoice_extracted_receipt_date=invoice_extracted_receipt_date,
            voucher_extracted_receipt_date=voucher_extracted_receipt_date)