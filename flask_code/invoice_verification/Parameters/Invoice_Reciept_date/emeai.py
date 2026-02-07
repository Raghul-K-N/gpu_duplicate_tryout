from typing import Dict, List
from invoice_verification.logger.logger import log_message
from .utils import shadow_po, emeai_naa_doc_type_xml
from invoice_verification.Parameters.utils import build_validation_result
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.constant_field_names import INVOICE_RECEIPT_DATE
from invoice_verification.Parameters.constants import TURKEY_COMPANY_CODE, POLAND_COMPANY_CODE


def validate_emeai_invoice_receipt_date(sap_row: SAPRow,
                                        payment_certificate_receipt_date: str|None,
                                        invoice_extracted_receipt_date: str|None,
                                        invoice_extracted_invoice_date: str|None,
                                        voucher_extracted_receipt_date: str|None
                                        ) -> Dict:
    """EMEAI-specific invoice receipt_date extraction logic"""
    log_message(f"Validate Emeai based Exceptions for Parameter Invoice Receipt Date")

    region: str = str(sap_row.region).strip().upper()
    company_code = str(sap_row.company_code).strip().upper()
    eml_lines: List = sap_row.eml_lines if isinstance(sap_row.eml_lines, list) else []
    po_numbers = sap_row.po_number if isinstance(sap_row.po_number, list) else []

    shadow_po_number_check = any(str(po).strip().startswith("414") or str(po).strip().startswith("5") for po in po_numbers)

    if eml_lines and (str(sap_row.order_type).strip().upper() == "SHADOW PO") \
            and (str(sap_row.transaction_code).strip().upper() == "ME23N") \
            and shadow_po_number_check:
        log_message(f"Shadow-PO Exception Handlding Started for Parameter Invoice Receipt Date")
        result = shadow_po(sap_receipt_date=sap_row.invoice_receipt_date,
                           region=region,
                           eml_lines=eml_lines)
        return result

    elif not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag) and \
            company_code in [str(code).strip().upper() for code in TURKEY_COMPANY_CODE]:
        log_message("Turkey Exception handling: Manual process for Parameter Invoice Receipt Date")
        return build_validation_result(
                extracted_value={INVOICE_RECEIPT_DATE: None},
                is_anomaly=None,
                edit_operation=False,
                highlight=True,
                method='Manual',
                supporting_details={"Summary":"No Invoice or Voucher copy present for Turkey Company Code - Manual process"}
            )
    
    elif not(sap_row.invoice_copy_flag or sap_row.voucher_copy_flag) and \
            company_code in [str(code).strip().upper() for code in POLAND_COMPANY_CODE]:
        log_message("Poland Exception handling: Manual process for Parameter Invoice Receipt Date")
        return build_validation_result(
                extracted_value={INVOICE_RECEIPT_DATE: None},
                is_anomaly=None,
                edit_operation=False,
                highlight=True,
                method='Manual',
                supporting_details={"Summary":"No Invoice or Voucher copy present for Poland Company Code - Manual process"}
            )
    
    # EMEAI and NAA doc type based XML Exception
    elif sap_row.xml_lines and ((str(sap_row.doc_type).strip().upper() == "KQ") and (str(sap_row.dp_doc_type).strip().upper() == "PO_CS_GLB")) \
                    and (str(sap_row.channel_id).strip().upper() == "EINV_XML"):
        log_message("Started EMEAI and NAA doc type based XML Exception handling for Parameter Invoice Receipt Date")
        result = emeai_naa_doc_type_xml(sap__receipt_date=sap_row.invoice_receipt_date,
                                        xml_lines=sap_row.xml_lines,
                                        region=region)
        return result
    
    # No EMEAI based Exceptions were met
    log_message("No EMEAI based Exceptions were met for Parameter Invoice Receipt date")
    from .global_logic import validate_global_invoice_receipt_date
    return validate_global_invoice_receipt_date(
            sap_row=sap_row,
            payment_certificate_receipt_date=payment_certificate_receipt_date,
            invoice_extracted_invoice_date=invoice_extracted_invoice_date,
            invoice_extracted_receipt_date=invoice_extracted_receipt_date,
            voucher_extracted_receipt_date=voucher_extracted_receipt_date)