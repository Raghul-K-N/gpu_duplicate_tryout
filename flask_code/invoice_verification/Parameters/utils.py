from typing import Any, Optional, Dict, List, Union
from invoice_verification.logger.logger import log_message
from rapidfuzz import fuzz
from pathlib import Path
import pandas as pd
import re

# Import constants - using direct import to avoid circular imports
# These are simple string constants with no dependencies
from invoice_verification.constant_field_names import (
    INVOICE_RECEIPT_DATE,
    INVOICE_DATE,
    INVOICE_AMOUNT,
    INVOICE_CURRENCY,
    VAT_TAX_CODE,
    VAT_TAX_AMOUNT,
    GL_ACCOUNT_NUMBER,
    INVOICE_NUMBER,
    VENDOR_BANKING_DETAILS,
    VENDOR_NAME_AND_ADDRESS,
    VENDOR_NAME,
    VENDOR_ADDRESS
)


# Dictionary mapping parameter field names to their extraction keywords in standard VIM comments format
# Used for legal requirement doc types: PO_EC_GLB, NPO_EC_GLB
# For parameters with multiple fields (like VAT), map the specific field that has a keyword in VIM
VIM_STANDARD_FORMAT_KEYWORD_MAP: Dict[str, str] = {
    INVOICE_RECEIPT_DATE: "Incoming Date",
    INVOICE_DATE: "Invoice Date",
    INVOICE_AMOUNT: "Invoice Amount",
    INVOICE_CURRENCY: "Invoice Amount",  # Currency is extracted from the same field as Invoice Amount
    VAT_TAX_AMOUNT: "Tax Amount",  # Only VAT_TAX_AMOUNT has keyword, VAT_TAX_CODE will be None
}

# List of parameters allowed for direct string search in VIM comments
# Parameters not in this list will skip VIM comments check even if function is called
VIM_DIRECT_SEARCH_ALLOWED_PARAMS: List[str] = [
    GL_ACCOUNT_NUMBER,
    INVOICE_NUMBER,
    INVOICE_AMOUNT,
    VAT_TAX_CODE,
    
    
]


# Dataframe creation based on the provided table
data = {
    "Region": [
        "APAC", "APAC", "APAC", "APAC", "APAC", "APAC", "APAC", "APAC", "APAC", "APAC", "APAC",
        "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI", "EMEAI",
        "LAA", "LAA", "LAA", "LAA", "LAA", "LAA",
        "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "NAA", "LAA", "LAA", "LAA", "LAA", "LAA", "LAA", "LAA", "LAA",
        "APAC", "EMEAI", "EMEAI", "EMEAI", "NAA", "NAA", "NAA", "EMEAI"
    ],
    "Mailbox Country/Name": [
        "China", "Hong Kong", "Singapore", "Japan", "Australia", "New Zealand", "Philippines", "Malaysia", "Jakarta", "Vietnam", "Taiwan",
        "Africa", "Austria, Germany", "Belgium", "Denmark", "Finland", "France", "Hungary, Romania", "Italy", "Netherlands", "Norway", "Portugal", "Saudi Arabia", "Spain", "Sweden", "Switzerland", "UAE", "United Kingdom", "Urgent emails",
        "All other LAA Countries", "Brazil", "Colombia", "Low Volume Countries", "Mexico", "Mexico - Blue Cube",
        "Dow Misui", "FREIGHT: Air", "FREIGHT: Misc. Rail", "FREIGHT: Ocean", "FREIGHT: Rail", "FREIGHT: Truck", "General_Invoice_Processing_General", "Hydrocarbon_Houston", "Raw_Material", "Sadara_Gulfstream", "Utilities", "Scripts/Uploads",
        "","","","","","","","","","","","","","","",""
    ],
    "Short Name": [
        "FPASCAN", "FHKSCAN", "FSGSCAN", "FJPSCAN", "FAUAPAY", "FNZSCAN", "FPHSCAN", "FMYSCAN", "FJKSCAN", "FVNSCAN", "FTWSCAN",
        "FTNINZA", "FTNAPGE", "FTNAPBE", "FTNAPDK", "FTNAPFI", "FTNAPFR", "FTNAPOR", "FTNAPIT", "FTNAPNL", "FTNAPSE", "FTNAPPT", "FTNAPAE", "FTNAPES", "FTNAPSE", "FTNAPCH", "FTNAPAE", "FTNAPUK", "FEUSCAN",
        "FACTURALADOW", "FATURABRDOW", "FACTURACOL", "FAEEINV", "FACTURAMEX", "FMZBCEI",
        "FUSDMCA", "FUSFREIG", "FNARPMT", "FUSELOI", "FUSRAIL", "FUSELTI", "FUSELAP", "FUSHCNE", "FUSNRMP", "FUSPMS2", "FUSAPUT", "FUSFSCR",
        "","","","","","","","","","","","","","","",""
    ],
    "email id": [
        "FPASCAN@dow.com", "FHKSCAN@dow.com", "FSGSCAN@dow.com", "FJPSCAN@dow.com", "FAUAPAY@dow.com", "FNZSCAN@dow.com", "FPHSCAN@dow.com", "FMYSCAN@dow.com", "FJKSCAN@dow.com", "FVNSCAN@dow.com", "FTWSCAN@dow.com",
        "FTNINZA@dow.com", "FTNAPGE@dow.com", "FTNAPBE@dow.com", "FTNAPDK@dow.com", "FTNAPFI@dow.com", "FTNAPFR@dow.com", "FTNAPOR@dow.com", "FTNAPIT@dow.com", "FTNAPNL@dow.com", "FTNAPSE@dow.com", "FTNAPPT@dow.com", "FTNAPAE@dow.com", "FTNAPES@dow.com", "FTNAPSE@dow.com", "FTNAPCH@dow.com", "FTNAPAE@dow.com", "FTNAPUK@dow.com", "FEUSCAN@dow.com",
        "FACTURALADOW@dow.com", "FATURABRDOW@dow.com", "FACTURACOL@dow.com", "FAEEINV@dow.com", "FACTURAMEX@dow.com", "FMZBCEI@dow.com",
        "FUSDMCA@dow.com", "FUSFREIG@dow.com", "FNARPMT@dow.com", "FUSELOI@dow.com", "FUSRAIL@dow.com", "FUSELTI@dow.com", "FUSELAP@dow.com", "FUSHCNE@dow.com", "FUSNRMP@dow.com", "FUSPMS2@dow.com", "FUSAPUT@dow.com", "FUSFSCR@dow.com", "FLASCRI@dow.com","faturabrdow@dow.com","facturaladow@dow.com","facturacol@dow.com","facturamex@dow.com","fbroawd@dow.com","flaoawd@dow.com","fspapay@dow.com", "FSLAPAY@dow.com", "ftnaplo@dow.com","FTNSCRP@dow.com","ftnmeeq@dow.com", "FUSCRIP@dow.com" ,"FUSCRIP@DOW.COM","FUSNETT@dow.com", "FIDDUTY@dow.com"
    ]
}

# TODO: Read this data from a config file or database in the future
# df_emails = pd.read_excel("invoice_verification/Parameters/Invoice_Reciept_date/Invoice_Receipt_Date_Email_Mapping.xlsx")
df_emails = pd.DataFrame(data)

# Separate lists for each region
APAC_EMAILS = df_emails[df_emails["Region"] == "APAC"]["email id"].tolist()
EMEAI_EMAILS = df_emails[df_emails["Region"] == "EMEAI"]["email id"].tolist()
LAA_EMAILS = df_emails[df_emails["Region"] == "LAA"]["email id"].tolist()
NAA_EMAILS = df_emails[df_emails["Region"] == "NAA"]["email id"].tolist()

email_dict = {
    "APAC": APAC_EMAILS,
    "EMEAI": EMEAI_EMAILS,
    "LAA": LAA_EMAILS,
    "NAA": NAA_EMAILS
}

credit_memo_keywords = {"EMEAI":["Abono /fra. rectif",
                                              "Abono",
                                              "Abono de suministro de Gas",
                                              "Abono rectificativo",
                                              "Added CR in amount",
                                              "Annulation factur",
                                              "AVIS DE CREDIT",
                                              "AVOIR / CREDIT NOTE",
                                              "Belastung / Selfbilling",
                                              "C A N C E L L A T I 0 N",
                                              "CANCEL. INVOICE",
                                              "Cancellation Document",
                                              "Cancellation invoice",
                                              "Cancellation of Inv",
                                              "Commercial Credit Memo",
                                              "Correction Credit Note",
                                              "CREDIT - NOTE",
                                              "Credit and amount in minus",
                                              "CREDIT FREIGHT NOTE",
                                              "Credit invoice",
                                              "Credit memo",
                                              "Credit Note",
                                              "Credit Note for Invoice",
                                              "Credit Note for Return",
                                              "CREDIT NOTE N°:",
                                              "CREDIT NOTE NO",
                                              "CREDIT NOTE NO.:",
                                              "CREDIT NOTE Number",
                                              "Credit Note Returns",
                                              "Credit note to invoice",
                                              "Creditnota",
                                              "CREDITNOTE",
                                              "EXPORT CREDIT NOT",
                                              "Export Credit Note",
                                              "EXPORT TAX CREDIT NOTE",
                                              "Fac Rectificativa  and amount in minus",
                                              "FACTURA RECTIFICATIVA",
                                              "Faktura korygujaca",
                                              "FRA.RECTIFICATIVA ABONO",
                                              "GUTSCHRIFT",
                                              "Gutschrift",
                                              "Invoice cancellation",
                                              "INVOICE CORRECTION",
                                              "Kaufmannische Gutschrif",
                                              "Kaufmannische Gutschrift",
                                              "Kreditfaktura",
                                              "Kreditnota",
                                              "Line-Item Credit Memo",
                                              "NOTA DE CREDITO",
                                              "Nota de Credito / Credit Note",
                                              "Nota di accredito",
                                              "Nota di credito",
                                              "Nota di Credito Nr",
                                              "Note de credit",
                                              "Rechnungskorrektur",
                                              "Sales - Credit Memo",
                                              "STORNO",
                                              "Storno-Rechnung",
                                              "Stornorechnung-Non English",
                                              "TAX CREDIT NOTE",
                                              "Tax Invoice- (Credit No:)",
                                              "VALUE CREDIT NOTE"],
                                    "NAA":["Credit invoice",
                                            "Credit memo",
                                            "Credit Note",
                                            "Invoice cancellation",
                                            "INVOICE CORRECTION",
                                            "Line-Item Credit Memo",
                                            "NOTA DE CREDITO",
                                            "TAX CREDIT NOTE"],
                                    "APAC":["Adjustment credit Note",
                                            "Adjustment note",
                                            "Adjustment Number",
                                            "Being payment to your Company",
                                            "Credit Adjustment Note",
                                            "CREDIT ADJUSTMENT TAX INVOICE",
                                            "Credit Advice",
                                            "Credit invoice",
                                            "Credit memo",
                                            "Credit Note",
                                            "Invoice cancellation",
                                            "INVOICE CORRECTION",
                                            "Line-Item Credit Memo",
                                            "Line-Item Credit Note",
                                            "NOTA DE CREDITO",
                                            "Reduction adjustment for invoice",
                                            "TAX CREDIT NOTE",
                                            "TAX CREDIT Number",
                                            "红字发票",
                                            "進貨退出或折讓證明單證明聯",
                                            "销项负数",
                                            "電子發票銷貨退回"],
                                    "LAA":["CARTA DE CREDITO",
                                            "Credit invoice",
                                            "Credit memo",
                                            # "Credit Memo (Invoice received with initial "CM"",
                                            "Credit Note",
                                            "CREDITO",
                                            "CRÉDITO",
                                            "DESCONTO",
                                            "DEVOLUCAO",
                                            "Invoice cancellation",
                                            "INVOICE CORRECTION",
                                            "Line-Item Credit Memo",
                                            "N.CREDITO",
                                            "NOTA CRÉDITO DE LA FACTURA ELECTRÓNICA DE VENTA",
                                            "Nota Crédito Electrónica",
                                            "NOTA DE CREDITO",
                                            "NOTA DE CRÉDITO ELECTRÓNICA",
                                            "Nota de Crédito Electrónica MiPyme",
                                            "Original Credit Memo",
                                            "TAX CREDIT NOTE",
                                            "Total due to Dow Silico es Corporation"]}

otm_apac_vendor_codes: List[str] = ["57553",
                            "239970","257842","283588","409772","990172","1163087","1298813","1474139","1815819","1888161","1949793","1978863","2072841","2127866","2165302","2356994","2394516","2547518","202970","203713","261762","477479","714523","1552475","1866264","1888161","2132642","2344799","2344887","2348592","2517912","231866","231866","231866","231866","269814","269814","329392","329392","354079","354079","354079","493428","493428","524054","542439","542439","600831","600831","678092","714523","853666","871927","871927","908029",
                            "1059793","1106155","1297017","1297017","1336382","1336382","1348856","1348856","1397136","1414063","1432050","1432050","1432076","1432076","1432080","1432080","1506989","1506989","1506989","1564110","1579342","1767839","1809425","1855976","1855976","1858392","1871859","1871859","1888161","1888161","1509623","1909229","1909229","1909229","2084925","2167196","2167196","2235548","2335539","2335539","2344799","2344887","2347626","2347626","2347648",
                            "2347648","2348592","2349650","2359439","2367790","2384762","2384762","2384762","2443787","2460999","2460999","2498463","2508130","2517912","2531304","2531304","2543697","2543697","2547518","2547518","2550150","2557525","2564499","57553","239970","257842","283588","409772","990172","1163087","1298813","1474139","1606877","1815819","1888161","1949793","1978863","2072841","2127866","2165302","2356994",
                            "2394516","2547518","206926","302662","518950","518952","864829","1066982","1067311","1297017","1335014","1921329","2184607","2448154","2480972","2537788","2538004","206926","302662","518950","518952","864829","1066982","1067311","1297017","1335014","1921329","2184607","2448154","2480972","2537788","2538004",
                            "678092","714523","923403","1557973","1858392","1888161","2152868","2344799","2344887","2348592","2517912","2526286","2547518","2550150","2564499","714523","923403","1557973","2152868","2344799","2344887","2348592","2517912","2526286","2547518","2564499",
                            "1051968","1503662","1503662","1548437","1564110","1780929","2405077","2492581","2540711","1051968","1503662","1503662","1780929","2492581","2540711","1503662","1548437","1780929","2492581","1503662","1503662","1679262","1503662","1699552","1780929","2540711","1503662","1699552","2540711","1503662","1699552","1780929","2492581","2540711","1780929","2405077"]


def is_empty_value(value: Any) -> bool:
    """
    Check if a value is empty, None, NaN, or whitespace.
    Handles:
    - None
    - pd.NA, pd.NaT
    - math.nan, float('nan')
    - Empty strings or whitespace-only strings
    - numpy NaN values
    Args:
        value: Any value to check
    Returns:
        bool: True if value is considered empty, False otherwise
    """
    # Handle None, NaN, pd.NA, pd.NaT using pandas
    if pd.isna(value):
        return True
    
    # Handle empty strings or whitespace-only strings
    if isinstance(value, str) and not value.strip():
        return True
    
    return False


def build_validation_result(
    extracted_value: Dict[str,Any],
    is_anomaly: Optional[bool],
    edit_operation: Optional[bool],
    highlight: Optional[bool],
    method: Optional[str],
    supporting_details: Optional[dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return canonical validation-result dict (fresh dict each call)."""

    if method in ["Combined", "Manual"] and is_anomaly is None:
        log_message(f"Changing 'is_anomaly' from None to True for Parameter '{str(extracted_value.keys())}'")
        is_anomaly = True
    
    return {
        "extracted_value": extracted_value,
        "is_anomaly": is_anomaly,
        "edit_operation": edit_operation,
        "highlight": highlight,
        "method": method,
        "supporting_details": supporting_details or {},
    }

def kc_ka_doctype_common_handling(parameter) -> Dict[str,Any]:
    """
    This Function Handles the
    DOC_TYPE: KA and KC.

    Args:
        parameter: Either a string (single parameter) or a list of strings (multiple parameters)

    Returns:
        Dict
    """
    # Handle list of fields for a parameter
    if isinstance(parameter, list):
        extracted_value = {field: None for field in parameter}
    else:
        # Handle single parameter (string)
        extracted_value = {parameter: None}
    
    return build_validation_result(extracted_value=extracted_value,
                                    is_anomaly=True,
                                    highlight=True,
                                    edit_operation=False,
                                    method="Combined",
                                    supporting_details={"Summary":"KA/KC Doc type - No invoice, Combined process"})


def check_parameter_in_vim_comments(sap_vim_comment_lines: List,
                                    parameter: str,
                                    parameter_value: Union[str, Dict[str, Any]],
                                    vim_keyword_map: Optional[Dict[str, str]] = None,
                                    sap_vendor_code: Optional[str] = None
                                    ) -> Dict:
    """
    Check parameter values in VIM comments using either standard format extraction or direct string search.
    
    For legal requirement doc types (PO_EC_GLB, NPO_EC_GLB), VIM comments follow a standard format.
    This function extracts values using keywords from that format and validates against SAP values.
    For other doc types, direct string search is performed for allowed parameters.
    Args:
        sap_vim_comment_lines: List of VIM comment lines
        parameter: Name of the parameter being checked (e.g., 'invoice_date', 'gl_account_number')
        parameter_value: SAP value(s) to validate:
                        - Single string: SAP value for the parameter (uses `parameter` to get keyword)
                        - Dict: {field_name: sap_value} mapping for multi-field parameters
                          e.g., {VAT_TAX_CODE: "V1", VAT_TAX_AMOUNT: "100.00"}
        vim_keyword_map: Optional dictionary mapping parameter/field to extraction keyword.
                        If provided and parameter is in the map, standard format extraction is used.
                        If None or parameter not in map, direct string search is attempted.
        sap_vendor_code: Optional SAP vendor code for PI Number validation (used for VENDOR_NAME_AND_ADDRESS
                        and VENDOR_BANKING_DETAILS parameters)
    Returns:
        Dict with validation result containing extracted_value, is_anomaly, highlight, etc.
    """
    # Use default keyword map if not provided
    if vim_keyword_map is None:
        vim_keyword_map = VIM_STANDARD_FORMAT_KEYWORD_MAP
    
    # Combine all VIM comment lines into a single string for regex extraction
    vim_comments_combined = " ".join(str(comment) for comment in sap_vim_comment_lines)
    
    # Special handling for vendor-related parameters that need PI Number extraction
    # These parameters use PI Number from VIM to validate vendor code
    vendor_related_params = [VENDOR_NAME_AND_ADDRESS, VENDOR_BANKING_DETAILS]
    
    # Handle single value vs dict of field:sap_value
    if isinstance(parameter_value, dict):
        # Multi-field parameter: {field_name: sap_value}
        field_sap_map = parameter_value
        fields_with_keywords = [field for field in field_sap_map.keys() if field in vim_keyword_map]
        
        if fields_with_keywords or parameter in vendor_related_params:
            return extract_from_standard_vim_format(
                vim_comments_combined=vim_comments_combined,
                parameter=parameter,
                field_sap_map=field_sap_map,
                vim_keyword_map=vim_keyword_map,
                sap_vendor_code=sap_vendor_code
            )
    else:
        # Single value: use parameter to get keyword
        sap_value = parameter_value
        if parameter in vim_keyword_map:
            return extract_from_standard_vim_format(
                vim_comments_combined=vim_comments_combined,
                parameter=parameter,
                field_sap_map={parameter: sap_value},
                vim_keyword_map=vim_keyword_map,
                sap_vendor_code=sap_vendor_code
            )
    
    # Check if parameter is allowed for direct string search
    if parameter in VIM_DIRECT_SEARCH_ALLOWED_PARAMS:
        # For direct search, normalize to dict format
        if isinstance(parameter_value, dict):
            field_sap_map = parameter_value
        else:
            field_sap_map = {parameter: parameter_value}
        
        return direct_string_search_in_vim(
            sap_vim_comment_lines=sap_vim_comment_lines,
            parameter=parameter,
            field_sap_map=field_sap_map
        )
    
    # Parameter not in keyword map and not in allowed direct search list
    # Return None anomaly with highlight for manual review
    log_message(f"[VIM Check] Parameter '{parameter}' is not configured for VIM comments check. Skipping.")
    if isinstance(parameter_value, dict):
        extracted_value = {field: None for field in parameter_value.keys()}
    else:
        extracted_value = {parameter: None}
    
    return build_validation_result(
        extracted_value=extracted_value,
        is_anomaly=None,
        highlight=True,
        edit_operation=True,
        method="Manual",
        supporting_details={"Summary": f"Unable to validate {parameter} using VIM comments"}
    )


def _extract_pi_number_from_vim(vim_comments: str) -> Optional[str]:
    """
    Extract PI Number from VIM comments and strip leading zeros.
    Args:
        vim_comments: Combined VIM comment string
    Returns:
        Extracted PI Number with leading zeros stripped, or None if not found/empty
    """
    # Match "PI Number:" followed by value, ending at "/" or end of line
    pattern = r'PI\s*Number\s*:\s*([^/\n]*)'
    match = re.search(pattern, vim_comments, re.IGNORECASE)
    
    if match:
        pi_value = match.group(1).strip()
        if pi_value:
            # Strip leading zeros
            stripped_value = pi_value.lstrip('0')
            log_message(f"[VIM Extraction] PI Number extracted: '{pi_value}' -> stripped: '{stripped_value}'")
            return stripped_value if stripped_value else None
        else:
            log_message("[VIM Extraction] PI Number keyword found but value is empty")
            return None
    
    log_message("[VIM Extraction] PI Number keyword not found in VIM comments")
    return None


def _extract_payee_number_from_vim(vim_comments: str) -> Optional[str]:
    """
    Extract Payee Number from VIM comments and strip leading zeros.
    Args:
        vim_comments: Combined VIM comment string
    Returns:
        Extracted Payee Number with leading zeros stripped, or None if not found/empty
    """
    # Match "Payee Number:" followed by value, ending at "/" or end of line
    pattern = r'Payee\s*Number\s*:\s*([^/\n]*)'
    match = re.search(pattern, vim_comments, re.IGNORECASE)
    
    if match:
        payee_value = match.group(1).strip()
        if payee_value:
            # Strip leading zeros
            stripped_value = payee_value.lstrip('0')
            log_message(f"[VIM Extraction] Payee Number extracted: '{payee_value}' -> stripped: '{stripped_value}'")
            return stripped_value if stripped_value else None
        else:
            log_message("[VIM Extraction] Payee Number keyword found but value is empty")
            return None
    
    log_message("[VIM Extraction] Payee Number keyword not found in VIM comments")
    return None


def extract_from_standard_vim_format(vim_comments_combined: str,
                                      parameter: str,
                                      field_sap_map: Dict[str, Any],
                                      vim_keyword_map: Dict[str, str],
                                      sap_vendor_code: Optional[str] = None) -> Dict:
    """
    Extract values from standard VIM format using regex and validate against SAP values.
    Standard VIM format example:
    Invoice Header Data:-
    Incoming Date:20250610 / Invoice Date:20250530 / Invoice Amount:9344.16 GBP / Tax Amount:0.00 GBP /
    PI Number:0000000000 / Payee Number: / Partner Bank:0000
    
    Args:
        vim_comments_combined: Combined VIM comment string
        parameter: Parent parameter name (for logging)
        field_sap_map: Dict mapping field names to their SAP values {field: sap_value}
        vim_keyword_map: Dictionary mapping field names to VIM keywords
        sap_vendor_code: SAP vendor code for PI Number validation (used for VENDOR_NAME_AND_ADDRESS
                        and VENDOR_BANKING_DETAILS parameters)
    Returns:
        Validation result dict with extracted values for each field
    """
    # Special handling for VENDOR_NAME_AND_ADDRESS - use PI Number extraction
    if parameter == VENDOR_NAME_AND_ADDRESS:
        log_message(f"[VIM Extraction] Using PI Number extraction for {parameter}")
        
        # Extract PI Number from VIM comments
        extracted_pi_number = _extract_pi_number_from_vim(vim_comments_combined)
        
        if extracted_pi_number is None:
            # Could not extract PI Number - mark as manual review
            log_message(f"[VIM Extraction] PI Number could not be extracted for {parameter}")
            return build_validation_result(
                extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
                is_anomaly=None,
                highlight=True,
                edit_operation=True,
                method="Manual",
                supporting_details={"Summary": f"PI Number not found or empty in VIM comments"}
            )
        
        # Compare extracted PI Number with SAP vendor code (both should be stripped of leading zeros)
        sap_vendor_code_stripped = str(sap_vendor_code).lstrip('0') if sap_vendor_code else None
        
        if sap_vendor_code_stripped and extracted_pi_number == sap_vendor_code_stripped:
            # PI Number matches SAP vendor code - use SAP vendor name and address as extracted values
            # This will result in no anomaly since extracted and SAP values are the same
            sap_vendor_name = field_sap_map.get(VENDOR_NAME)
            sap_vendor_address = field_sap_map.get(VENDOR_ADDRESS)
            
            log_message(f"[VIM Validation] PI Number '{extracted_pi_number}' matches SAP vendor code '{sap_vendor_code_stripped}' - using SAP vendor details")
            
            return build_validation_result(
                extracted_value={VENDOR_NAME: sap_vendor_name, VENDOR_ADDRESS: sap_vendor_address},
                is_anomaly=False,
                highlight=False,
                edit_operation=False,
                method="Automated",
                supporting_details={"Summary": "Used VIM comments, PI Number validated - Vendor details matched"}
            )
        else:
            # PI Number does not match SAP vendor code - mark as anomaly
            log_message(f"[VIM Validation] PI Number '{extracted_pi_number}' does NOT match SAP vendor code '{sap_vendor_code_stripped}' - marking as anomaly")
            
            return build_validation_result(
                extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
                is_anomaly=True,
                highlight=False,
                edit_operation=False,
                method="Automated",
                supporting_details={"Summary": "Used VIM comments for VENDOR_NAME_AND_ADDRESS, PI Number mismatch"}
            )
    
    # Special handling for VENDOR_BANKING_DETAILS - placeholder for future Payee Number extraction
    # Note: Logic for VENDOR_BANKING_DETAILS using Payee Number will be implemented later
    if parameter == VENDOR_BANKING_DETAILS:
        log_message(f"[VIM Extraction] VENDOR_BANKING_DETAILS - Payee Number extraction logic to be implemented")
        # For now, extract Payee Number but don't use it yet
        extracted_payee_number = _extract_payee_number_from_vim(vim_comments_combined)
        # Fall through to standard processing for now
    
    # Standard extraction logic for other parameters
    extracted_value_dict = {}
    any_extracted = False
    all_matched = True
    
    for field, sap_value in field_sap_map.items():
        if field in vim_keyword_map:
            # Field has a keyword mapping - extract the value
            keyword = vim_keyword_map[field]
            extracted_vim_value = _extract_value_by_keyword(vim_comments_combined, keyword, field)
            
            if extracted_vim_value is None:
                # Keyword found but value is empty
                log_message(f"[VIM Extraction] Parameter '{parameter}', Field '{field}': Keyword '{keyword}' found but no value present")
                extracted_value_dict[field] = None
                all_matched = False
            elif extracted_vim_value == "KEYWORD_NOT_FOUND":
                # Keyword not found
                log_message(f"[VIM Extraction] Parameter '{parameter}', Field '{field}': Keyword '{keyword}' not found in VIM comments")
                extracted_value_dict[field] = None
                all_matched = False
            else:
                # Successfully extracted - now validate against SAP value
                extracted_value_dict[field] = extracted_vim_value
                any_extracted = True
                log_message(f"[VIM Extraction] Parameter '{parameter}', Field '{field}': Successfully extracted value '{extracted_vim_value}' using keyword '{keyword}'")
                
                # Validate extracted value against SAP value
                if not is_empty_value(sap_value):
                    is_match = _compare_vim_and_sap_values(extracted_vim_value, sap_value, field)
                    if not is_match:
                        all_matched = False
                        log_message(f"[VIM Validation] Parameter '{parameter}', Field '{field}': VIM value '{extracted_vim_value}' does NOT match SAP value '{sap_value}'")
                    else:
                        log_message(f"[VIM Validation] Parameter '{parameter}', Field '{field}': VIM value '{extracted_vim_value}' matches SAP value '{sap_value}'")
                else:
                    log_message(f"[VIM Validation] Parameter '{parameter}', Field '{field}': SAP value is empty, skipping validation")
                    all_matched = False
        else:
            # Field has no keyword mapping - set to None
            extracted_value_dict[field] = None
            log_message(f"[VIM Extraction] Parameter '{parameter}', Field '{field}': No keyword mapping configured, setting to None")
            all_matched = False
            log_message(f"[VIM VALIDATION] Parameter '{parameter}', Field '{field}': No keyword mapping - So Anomaly set to True")

    
    if not any_extracted:
        # No values were extracted for any field
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=None,
            highlight=True,
            edit_operation=True,
            method="Manual",
            supporting_details={"Summary": f"Used VIM comments for {parameter}"}
        )
    
    if all_matched:
        # All extracted values match their SAP values
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=False,
            highlight=False,
            edit_operation=False,
            method="Automated",
            supporting_details={"Summary": f"{parameter} extracted from VIM comments"}
        )
    else:
        # Some values don't match
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=True,
            highlight=False,
            edit_operation=False,
            method="Combined",
            supporting_details={"Summary": f"{parameter} extracted from VIM comments"}
        )


def _extract_value_by_keyword(vim_comments: str, keyword: str, parameter: str) -> Optional[str]:
    """
    Extract value from VIM comments using keyword-based regex.
    Args:
        vim_comments: Combined VIM comment string
        keyword: Keyword to search for (e.g., 'Invoice Date', 'Invoice Amount')
        parameter: Parameter/field name for context-specific extraction
    Returns:
        Extracted value string, None if keyword found but value empty, 
        or "KEYWORD_NOT_FOUND" if keyword not present
    """
    # Check if keyword exists in comments
    if keyword.lower() not in vim_comments.lower():
        return "KEYWORD_NOT_FOUND"
    
    # Special handling for currency extraction (from Invoice Amount field)
    if parameter == INVOICE_CURRENCY:
        # Pattern: Invoice Amount:9344.16 GBP or Invoice Amount:9344.16GBP
        pattern = rf"{re.escape(keyword)}\s*:\s*[\d,.\-]+\s*([A-Z]{{3}})"
        match = re.search(pattern, vim_comments, re.IGNORECASE)
        if match:
            extracted = match.group(1).strip().upper()
            log_message(f"[VIM Regex] Parameter '{parameter}': Currency extraction matched '{extracted}'")
            return extracted
        log_message(f"[VIM Regex] Parameter '{parameter}': Currency pattern did not match for keyword '{keyword}'")
        return None
    
    # Special handling for invoice amount (numeric value with optional decimals)
    if parameter == INVOICE_AMOUNT:
        # Pattern: Invoice Amount:9344.16 or Invoice Amount: 9,344.16
        pattern = rf"{re.escape(keyword)}\s*:\s*([\d,.\-]+)"
        match = re.search(pattern, vim_comments, re.IGNORECASE)
        if match:
            # Clean up the value (remove commas, keep decimals)
            value = match.group(1).strip().replace(",", "")
            log_message(f"[VIM Regex] Parameter '{parameter}': Amount extraction matched '{value}'")
            return value
        log_message(f"[VIM Regex] Parameter '{parameter}': Amount pattern did not match for keyword '{keyword}'")
        return None
    
    # Special handling for tax amount (VAT_TAX_AMOUNT)
    if parameter == VAT_TAX_AMOUNT:
        # Pattern: Tax Amount:0.00 or Tax Amount: 123.45
        pattern = rf"{re.escape(keyword)}\s*:\s*([\d,.\-]+)"
        match = re.search(pattern, vim_comments, re.IGNORECASE)
        if match:
            value = match.group(1).strip().replace(",", "")
            log_message(f"[VIM Regex] Parameter '{parameter}': Tax amount extraction matched '{value}'")
            return value
        log_message(f"[VIM Regex] Parameter '{parameter}': Tax amount pattern did not match for keyword '{keyword}'")
        return None
    
    # Special handling for date fields (invoice_receipt_date, invoice_date)
    # Legal requirement standard format always uses YYYYMMDD
    if parameter in [INVOICE_RECEIPT_DATE, INVOICE_DATE]:
        # Pattern: Incoming Date:20250610 or Invoice Date:20250530 (YYYYMMDD format)
        pattern = rf"{re.escape(keyword)}\s*:\s*(\d{{8}})"
        match = re.search(pattern, vim_comments, re.IGNORECASE)
        if match:
            extracted = match.group(1).strip()
            log_message(f"[VIM Regex] Parameter '{parameter}': Date extraction matched '{extracted}' (YYYYMMDD format)")
            return extracted
        log_message(f"[VIM Regex] Parameter '{parameter}': Date pattern (YYYYMMDD) did not match for keyword '{keyword}'")
        return None
    
    # Generic extraction: Keyword:Value (until space, slash, or newline)
    pattern = rf"{re.escape(keyword)}\s*:\s*([^\s/]+)"
    match = re.search(pattern, vim_comments, re.IGNORECASE)
    if match:
        extracted = match.group(1).strip()
        log_message(f"[VIM Regex] Parameter '{parameter}': Generic extraction matched '{extracted}' for keyword '{keyword}'")
        return extracted
    
    log_message(f"[VIM Regex] Parameter '{parameter}': No pattern matched for keyword '{keyword}'")
    return None


def _compare_vim_and_sap_values(vim_value: str, sap_value: str, parameter: str) -> bool:
    """
    Compare extracted VIM value with SAP value, with type-specific handling.
    Args:
        vim_value: Value extracted from VIM comments
        sap_value: Value from SAP
        parameter: Parameter/field name for type-specific comparison
    Returns:
        True if values match, False otherwise
    """
    if is_empty_value(vim_value) or is_empty_value(sap_value):
        log_message(f"[VIM Compare] Parameter '{parameter}': Empty value detected - VIM='{vim_value}', SAP='{sap_value}'")
        return False
    
    # Date comparison using pd.to_datetime
    if parameter in [INVOICE_RECEIPT_DATE, INVOICE_DATE]:
        try:
            vim_date = pd.to_datetime(vim_value, errors='coerce')
            sap_date = pd.to_datetime(sap_value, errors='coerce')
            
            if pd.isna(vim_date) or pd.isna(sap_date):
                log_message(f"[VIM Compare] Parameter '{parameter}': Date parsing failed - VIM='{vim_value}' -> {vim_date}, SAP='{sap_value}' -> {sap_date}")
                return False
            
            # Compare only date part (ignore time)
            result = vim_date.date() == sap_date.date()
            log_message(f"[VIM Compare] Parameter '{parameter}': Date comparison - VIM={vim_date.date()}, SAP={sap_date.date()}, Match={result}")
            return result
        except Exception as e:
            log_message(f"[VIM Compare] Parameter '{parameter}': Date comparison error - VIM='{vim_value}', SAP='{sap_value}', Error: {e}", error_logger=True)
            return False
    
    # Numeric comparison for amounts
    if parameter in [INVOICE_AMOUNT, VAT_TAX_AMOUNT]:
        try:
            vim_num = float(str(vim_value).replace(",", ""))
            sap_num = float(str(sap_value).replace(",", ""))
            # Allow small tolerance for floating point comparison
            result = abs(vim_num - sap_num) < 0.01
            log_message(f"[VIM Compare] Parameter '{parameter}': Numeric comparison - VIM={vim_num}, SAP={sap_num}, Diff={abs(vim_num - sap_num)}, Match={result}")
            return result
        except (ValueError, TypeError) as e:
            log_message(f"[VIM Compare] Parameter '{parameter}': Numeric comparison error - VIM='{vim_value}', SAP='{sap_value}', Error: {e}", error_logger=True)
            return False
    
    # Currency comparison (case-insensitive string match)
    if parameter == INVOICE_CURRENCY:
        result = str(vim_value).strip().upper() == str(sap_value).strip().upper()
        log_message(f"[VIM Compare] Parameter '{parameter}': Currency comparison - VIM='{vim_value}', SAP='{sap_value}', Match={result}")
        return result
    
    # Default string comparison (case-insensitive)
    result = str(vim_value).strip().lower() == str(sap_value).strip().lower()
    log_message(f"[VIM Compare] Parameter '{parameter}': String comparison - VIM='{vim_value}', SAP='{sap_value}', Match={result}")
    return result


def direct_string_search_in_vim(sap_vim_comment_lines: List,
                                  parameter: str,
                                  field_sap_map: Dict[str, Any]) -> Dict:
    """
    Perform direct string search for parameter values in VIM comments.
    Used for non-standard format VIM comments.
    Args:
        sap_vim_comment_lines: List of VIM comment lines
        parameter: Parameter name
        field_sap_map: Dict mapping field names to their SAP values {field: sap_value}
    
    Returns:
        Validation result dict
    """
    extracted_value_dict = {}
    all_found = True
    any_value_to_check = False
    
    for field, sap_value in field_sap_map.items():
        # Skip empty values
        if is_empty_value(sap_value):
            extracted_value_dict[field] = None
            log_message(f"[VIM Direct Search] Parameter '{parameter}', Field '{field}': SAP value is empty, skipping")
            continue
        
        any_value_to_check = True
        value_found = False
        
        for comment in sap_vim_comment_lines:
            if str(sap_value).strip().lower() in str(comment).lower():
                value_found = True
                extracted_value_dict[field] = str(sap_value)
                log_message(f"[VIM Direct Search] Parameter '{parameter}', Field '{field}': Value '{sap_value}' found in VIM comments")
                break
        
        if not value_found:
            extracted_value_dict[field] = None
            all_found = False
            log_message(f"[VIM Direct Search] Parameter '{parameter}', Field '{field}': Value '{sap_value}' NOT found in VIM comments")
    
    # If no values to check, return highlight for manual review
    if not any_value_to_check:
        log_message(f"[VIM Direct Search] Parameter '{parameter}': No non-empty values to check")
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=None,
            highlight=True,
            edit_operation=True,
            method="Manual",
            supporting_details={"Summary": f"{parameter} uses VIM comments for extraction"}
        )
    
    if all_found:
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=False,
            highlight=True,
            edit_operation=True,
            method="Manual",
            supporting_details={"Summary": f"{parameter} value(s) found in VIM comments"}
        )
    else:
        # Value not found in direct search - return None anomaly with highlight for manual review
        return build_validation_result(
            extracted_value=extracted_value_dict,
            is_anomaly=None,
            highlight=True,
            edit_operation=True,
            method="Manual",
            supporting_details={"Summary": f"{parameter} value(s) not found in VIM comments - manual review required"}
        )


def check_parameter_values_in_vim_comments(sap_vim_comment_lines: List,
                                           field_values: List[Any],
                                           parameter: str=""
                                        ) -> Optional[bool]:
    """
    Check whether all field values appear in vim_comment_lines.
    Args:
        sap_vim_comment_lines: List of comment lines from SAP VIM
        field_values: List of values to check in comments
        parameter: Name of the parameter being checked (for logging)
    Returns:
        True if all values found in comments
        False if any value not found
        None if any value in list is missing/empty
    """
    # If any value is missing/empty, return None
    if any(is_empty_value(value) for value in field_values):
        return None
    
    # Check if all values appear in vim comments
    for value in field_values:
        value_found = False
        for comment in sap_vim_comment_lines:
            if str(value).strip().lower() in str(comment).lower():
                value_found = True
                log_message(f"{parameter} field value found: '{value}' in comment: {comment}")
                break
        
        # If any value is not found, return False
        if not value_found:
            log_message(f"{parameter} field value '{value}' not found in any comment.")
            return False
    
    # All values found
    return True



def parse_supplier_remit_details_ke(file_paths: List, sheet_name: str = 'Supplier Remit Details') -> Dict[str, Optional[str]]:
    """
    Parse supplier remit details from Excel file with specific format.
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Sheet name or index (default: 'Supplier Remit Details')
    
    Returns:
        Dictionary with extracted fields:
        - supplier_name
        - bill_to_legal_entity
        - supplier_tax_registration_number
        - supplier_physical_address
        - bill_to_legal_address
        - bank_account_number
        - routing_number
    """
    for file_path in file_paths:
        try:
            # Read Excel without headers, starting from row 2 (index 1)
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, skiprows=1)
            
            # Initialize result dictionary
            result = {
                'supplier_name': None,
                'bill_to_legal_entity': None,
                'supplier_tax_registration_number': None,
                'supplier_physical_address': None,
                'bill_to_legal_address': None,
                'bank_account_number': None,
                'routing_number': None
            }
            
            # Helper function to clean values
            def clean_value(val):
                if pd.isna(val):
                    return None
                return str(val).strip()
            
            # Parse through the dataframe
            max_rows = len(df)
            
            # Row 0: Contains "Supplier Name:" and "*Bill To Legal Entity:" (labels)
            # Row 1: Contains actual values - ABC Company and Dow
            if max_rows > 1:
                # Get values from row 1 (index 1), not row 0
                result['supplier_name'] = clean_value(df.iloc[1, 0])
                if len(df.columns) > 1:
                    result['bill_to_legal_entity'] = clean_value(df.iloc[1, 1])
            
            # Find and extract Supplier Tax Registration Number
            for i in range(max_rows):
                cell_value = clean_value(df.iloc[i, 0])
                if cell_value and 'Supplier Tax Registration Number' in cell_value:
                    # Check next row for the value
                    if i + 1 < max_rows:
                        result['supplier_tax_registration_number'] = clean_value(df.iloc[i + 1, 0])
                    break
            
            # Find and extract Supplier Physical Address
            supplier_address_lines = []
            bill_to_address_lines = []
            
            for i in range(max_rows):
                cell_value = clean_value(df.iloc[i, 0])
                if cell_value and 'Supplier Physical Address' in cell_value:
                    # Collect address lines until we hit empty rows or next section
                    j = i + 1
                    while j < max_rows:
                        addr_line_col0 = clean_value(df.iloc[j, 0])
                        addr_line_col1 = clean_value(df.iloc[j, 1]) if len(df.columns) > 1 else None
                        
                        # Stop if we hit "Remit To Information" or similar section header
                        if addr_line_col0 and ('Remit To' in addr_line_col0 or 'Bank Account' in addr_line_col0):
                            break
                        
                        if addr_line_col0:
                            supplier_address_lines.append(addr_line_col0)
                        if addr_line_col1:
                            bill_to_address_lines.append(addr_line_col1)
                        
                        # Stop if both columns are empty
                        if not addr_line_col0 and not addr_line_col1:
                            break
                            
                        j += 1
                    break
            
            result['supplier_physical_address'] = ', '.join(supplier_address_lines) if supplier_address_lines else None
            result['bill_to_legal_address'] = ', '.join(bill_to_address_lines) if bill_to_address_lines else None
            
            # Find and extract Bank Account Number and Routing Number
            for i in range(max_rows):
                cell_value = clean_value(df.iloc[i, 0])
                if cell_value:
                    if 'Bank Account Number' in cell_value:
                        # Extract value after colon
                        parts = cell_value.split(':')
                        if len(parts) > 1:
                            result['bank_account_number'] = parts[1].strip()
                    elif 'Routing Number' in cell_value:
                        # Extract value after colon
                        parts = cell_value.split(':')
                        if len(parts) > 1:
                            result['routing_number'] = parts[1].strip()
            
            log_message(f"Successfully parsed supplier remit details from {file_path}")
            return result
            
        except Exception as e:
            log_message(f"Error parsing supplier remit details from {file_path}: {str(e)}", error_logger=True)
            continue
    return {
        'supplier_name': None,
        'bill_to_legal_entity': None,
        'supplier_tax_registration_number': None,
        'supplier_physical_address': None,
        'bill_to_legal_address': None,
        'bank_account_number': None,
        'routing_number': None
    }



import re

def fix_ocr_months(s: str) -> str:
    # Common OCR confusions
    month_fixes = {
    r'\b0ct\b': 'Oct',
    r'\b0ctober\b': 'October',
    r'\bjui\b': 'Jul',
    r'\bjuiy\b': 'July',
    r'\bfebrary\b': 'February',
}


    for pattern, replacement in month_fixes.items():
        s = re.sub(pattern, replacement, s, flags=re.I)

    return s


def normalize_date_string(s: str) -> str:
    # Remove leading/trailing whitespace introduced by OCR or copy-paste
    s = s.strip()

    # Remove zero-width and invisible Unicode characters (OCR artifacts that break parsing)
    # Examples: ZERO WIDTH SPACE, ZERO WIDTH JOINER, BYTE ORDER MARK
    s = re.sub(r'[\u200B-\u200D\uFEFF]', '', s)

    # Collapse multiple spaces/tabs/newlines into a single space
    # Fixes cases like: "27   October    2025"
    s = re.sub(r'\s+', ' ', s)

    # Ensure there is a space after commas before digits
    # Fixes: "October 27,2025" → "October 27, 2025"
    s = re.sub(r',(\d)', r', \1', s)

    # Remove ordinal suffixes from day numbers
    # Fixes: "27th October 2025" → "27 October 2025"
    s = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s, flags=re.I)

    # Normalize Unicode dashes (en dash, em dash, minus) to ASCII hyphen
    # Fixes: "27–10–2025", "27—10—2025"
    s = s.translate(str.maketrans("–—−", "---"))

    # Remove trailing punctuation or noise characters
    # Fixes: "27/10/2025.", "27-10-2025*", "27-10-2025;"
    s = re.sub(r'[^\w\s/-]+$', '', s)

    # Normalize text casing so month names are consistently capitalized
    # Fixes: "october 27, 2025", "OCT 27 2025"
    # s = s.title()

    return s


MONTH_PREFIXES = "jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december"

def strip_leading_non_month_word(s: str) -> str:
    pattern = rf"""
        ^\s*
        (?!({MONTH_PREFIXES})\b)   # the word itself must NOT be a month
        [A-Za-zÀ-ÿ]+               # leading alphabetic word
        \s+(?=\d)                  # followed by a digit
    """
    return re.sub(pattern, "", s, flags=re.I | re.VERBOSE).strip()



def  global_pandas_date_parser(s: str, region: Optional[str] = None):
    """
    Parse date strings robustly with multiple format attempts.
    Supports region-specific preferences and fallback to pandas inference.
   
    Args:
        s: Date string to parse
        region: Region code ('EMEAI' for dd-mm-yyyy preference, else mm-dd-yyyy)
   
    Returns:
        datetime object or None
    """
    if not s or not isinstance(s, str):
        return None
   
    s = s.strip()
    s = strip_leading_non_month_word(s)
    s = fix_ocr_months(s)
    if not s:
        return None
    
    s = normalize_date_string(s)
   
    try:
        # Priority 1: ISO format (yyyy-mm-dd) - unambiguous (4-digit year only)
        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']:
            try:
                dt = pd.to_datetime(s, format=fmt, errors='raise')
                if not pd.isna(dt):
                    return  pd.to_datetime(dt.date())
            except Exception as e:
                continue
       
        # Without separators - 8-digit yyyymmdd
        if len(s) == 8 and s.isdigit():
            try:
                dt = pd.to_datetime(s, format='%Y%m%d', errors='raise')
                if not pd.isna(dt):
                    return  pd.to_datetime(dt.date())
            except:
                pass
       
        # Without separators - 6-digit yymmdd
        if len(s) == 6 and s.isdigit():
            try:
                dt = pd.to_datetime(s, format='%y%m%d', errors='raise')
                if not pd.isna(dt):
                    return  pd.to_datetime(dt.date())
            except:
                pass
       
        # Priority 2: Region-specific formats
        if region and region.upper().startswith('EMEA'):
            # EMEAI: dd-mm-yyyy priority
            formats = [
                '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',  # 4-digit year
                '%d-%m-%y', '%d/%m/%y', '%d.%m.%y'   # 2-digit year
            ]
        else:
            # Other regions: mm-dd-yyyy priority
            formats = [
                '%m-%d-%Y', '%m/%d/%Y', '%m.%d.%Y',  # 4-digit year
                '%m-%d-%y', '%m/%d/%y', '%m.%d.%y'   # 2-digit year
            ]
       
        for fmt in formats:
            try:
                dt = pd.to_datetime(s, format=fmt, errors='raise')
                if not pd.isna(dt):
                    return  pd.to_datetime(dt.date())
            except:
                continue
       
        # Priority 3: Text dates (e.g., "Jan 15, 2025", "15 January 2025")
        try:
            

            dt = pd.to_datetime(s, errors='raise')
            if not pd.isna(dt):
                return  pd.to_datetime(dt.date())
        except Exception as e:
            pass
       
        # Priority 4: Fallback with region preference
        dayfirst = bool(region and region.upper().startswith('EMEA'))
        dt = pd.to_datetime(s, errors='coerce', dayfirst=dayfirst)
       
        if pd.isna(dt):
            return None
       
        return  pd.to_datetime(dt.date())
       
    except Exception as e:
        return None
    
def find_sources_dir(start: Optional[Path] = None) -> Optional[Path]:
    """
    Walk up the directory tree from `start` (defaults to this file's dir)
    and return the first 'sources' directory found. Raises FileNotFoundError
    if not found.
    """
    # Normalize start to a Path object; default to this file's directory when None
    if start is None:
        start_path = Path(__file__).resolve().parent
    else:
        start_path = Path(start).resolve()

    for p in [start_path] + list(start_path.parents):
        candidate = p / "sources"
        if candidate.is_dir():
            return candidate
    log_message("Could not find a 'sources' directory in this repo tree.")
    return None


def remove_page_markers(text_lines: List[str]) -> List[str]:
    """
    Remove page marker patterns from a list of strings.
    
    Args:
        text_lines: List of text lines that may contain page markers
    
    Returns:
        List of strings with page markers removed
    """
    page_marker_pattern = r'^={34}<Page \d+ - (START|END)>={32}$'
    return [line for line in text_lines if not re.match(page_marker_pattern, line)]

def partial_match(str1: Optional[str], 
                               str2: Optional[str], 
                               threshold: float = 85.0,
                               minimum_length: int = 10) -> Optional[bool]:
    """Partial match using fuzzy matching"""
    if not str1 or not str2:
        return None
    
    if len(str1) < minimum_length or len(str2) < minimum_length:
        return None
    
    similarity = fuzz.partial_ratio(str1.strip().upper(), str2.strip().upper())
    return similarity >= threshold