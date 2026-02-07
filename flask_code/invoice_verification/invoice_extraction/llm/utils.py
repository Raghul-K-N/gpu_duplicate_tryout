from typing import List
from price_parser.parser import Price
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.Bill_to_LE_and_Addr.utils import normalize_legal_entity_name
from invoice_verification.Parameters.utils import find_sources_dir, partial_match
from invoice_verification.Parameters.constants import CANADA_COMPANY_CODES
from invoice_verification.Schemas.sap_row import SAPRow
import os
import pandas as pd

NA_CURRENCY_MAP = {
    "$": "USD",     # United States (default for NA unless specified)
    "US$": "USD",
    "CA$": "CAD",
    "C$": "CAD",
    "MX$": "MXN"
}

EMEA_CURRENCY_MAP = {
    "€": "EUR",
    "£": "GBP",
    "₺": "TRY",
    "₽": "RUB",
    "kr": "SEK",    # Assumes Nordics default to SEK
    "CHF": "CHF",
    "zł": "PLN",
    "د.إ": "AED",
    "﷼": "SAR",
    "₪": "ILS",
    "R": "ZAR"
}

LA_CURRENCY_MAP = {
    "$": "USD",     # LATAM invoices often USD-denominated
    "R$": "BRL",
    "AR$": "ARS",
    "CLP$": "CLP",
    "COP$": "COP",
    "S/": "PEN",
    "Bs": "VES",
    "₡": "CRC"
}


APAC_CURRENCY_MAP = {
    "₹": "INR",
    "Rs": "INR",
    "¥": "JPY",     # Default to JPY unless China explicitly known
    "CN¥": "CNY",
    "元": "CNY",
    "₩": "KRW",
    "A$": "AUD",
    "AU$": "AUD",
    "NZ$": "NZD",
    "SG$": "SGD",
    "HK$": "HKD",
    "₱": "PHP",
    "฿": "THB",
    "₫": "VND",
    "RM": "MYR",
    "Rp": "IDR"
}


CURRENCY_MAP = {
    "NA": NA_CURRENCY_MAP,
    "EMEA": EMEA_CURRENCY_MAP,
    "LA": LA_CURRENCY_MAP,
    "APAC": APAC_CURRENCY_MAP,
}


def search_value_in_text_lines(value: str|None, text_lines: List[str]) -> bool:
    """
    Utility function to search for a value in the text lines.
    """
    if value is None and str(value).strip() == "":
        return False
    value = str(value).strip().lower()
    for line in text_lines:
        if value in line.lower():
            return True
    return False

def is_valid_currency(value) -> bool:
    """Check if currency value is valid and non-empty."""
    if not value:
        return False
    
    elif value is None:
        return False
    
    elif isinstance(value, list):
        return len(value) > 0 and any(v and str(v).strip() != 'None' for v in value)
    
    elif isinstance(value, str):
        return bool(value.strip() and value not in ('None', '[None]'))
    
    return False


def extract_invoice_currency_from_raw_response(actual_response, raw_response,sap_row:SAPRow):
    """
    Extract invoice currency from amount in raw LLM response if not present in actual response.
    """
    invoice_currency = actual_response.get("invoice_currency", None)
    if is_valid_currency(invoice_currency):
        return invoice_currency, 'From actual response'
    
    region = str(sap_row.region or "").upper()
    company_code = str(sap_row.company_code or "").strip()
    if company_code in CANADA_COMPANY_CODES:
        return ['CAD'], 'Derived from company code indicating Canada'
    
    # Attempt to extract from raw response
    raw_invoice_amount = raw_response.get("invoice_amount", None)
    if raw_invoice_amount and isinstance(raw_invoice_amount, str):
        # Example format: "$500, 
        # Select which currency map to use
        region_to_lookup = None
        if str(region).upper() in ['NA','NAA']:
            region_to_lookup = 'NA'
        elif str(region).upper() in ['EMEA','EMEAA']:
            region_to_lookup = 'EMEA'
        elif str(region).upper() in ['LA','LAA','LATAM']:
            region_to_lookup = 'LA'
        elif str(region).upper() in ['APAC','APACA','ASIA']:
            region_to_lookup = 'APAC'

        region_currency_map = CURRENCY_MAP.get(region_to_lookup, {}) if region_to_lookup else {}
        parsed_price = Price.fromstring(raw_invoice_amount)
        currency_symbol = parsed_price.currency
        if currency_symbol:
            # Map symbol to currency code
            currency_code = region_currency_map.get(currency_symbol, None)
            if currency_code:
                return [currency_code], 'Extracted from raw response invoice amount'
            
    return None, 'Could not extract currency'
    

def extract_params_manually_from_textlines(sap_row: SAPRow,text_lines: List[str],invoice_type: str) -> dict:
    """
    This function is an additional logic to extract certain fields from the text lines
    Step 1: use LLM to extract key fields
    Step 2: use sap_row to extract certain fields.

    Args:
        sap_row (SAPRow): The SAPRow object containing relevant data.
        text_lines (List[str]): The list of text lines from the invoice.

    Returns:
        Dict: A dictionary containing the extracted fields.

    """

    # "gl_account_number", "invoice_number","stamp_number", "invoice_amount",
    # "vendor_name","vendor_address", "legal_entity_name","legal_entity_address","invoice_currency", "payment_method",
    #"bank_account_number", "bank_name","bank_account_holder_name", "payment_terms", "invoice_date", "vat_tax_code",
    # "vat_tax_amount", "invoice_receipt_date", "transaction_type"

    # "bank_account_number", "bank_name","bank_account_holder_name" will not be in SAP Data

    key_to_sap_attributes_mapping = {'gl_account_number':sap_row.gl_account_number,
                                     'invoice_number':sap_row.invoice_number,
                                     'invoice_amount':sap_row.invoice_amount,
                                     'vendor_name':sap_row.vendor_name,
                                     'vendor_address':sap_row.vendor_address,
                                     'legal_entity_name':sap_row.legal_entity_name,
                                     'legal_entity_address':sap_row.legal_entity_address,
                                     'invoice_currency':sap_row.invoice_currency,
                                     'payment_method':sap_row.payment_method,
                                     'payment_terms':sap_row.payment_terms,
                                     'invoice_date':sap_row.invoice_date,
                                     'vat_tax_code':sap_row.vat_tax_id,
                                     'vat_tax_amount':sap_row.vat_amount,
                                     'invoice_receipt_date':sap_row.invoice_receipt_date,
                                     'transaction_type':sap_row.transaction_type
                                    }
    manual_results = {}
    fields_to_search_for = []

    if invoice_type == "invoice":
        fields_to_search_for = ["invoice_number","invoice_amount",
                                "vendor_name","vendor_address", "legal_entity_name","legal_entity_address",
                                "invoice_currency", "invoice_date", "vat_tax_code",
                                "vat_tax_amount", "invoice_receipt_date","gl_account_number"]
    else:
        fields_to_search_for = ["invoice_number","invoice_amount",
                                "vendor_name","vendor_address", "legal_entity_name","legal_entity_address",
                                "invoice_currency", 'invoice_date','vat_tax_code',"payment_terms",
                                'vat_tax_amount', 'invoice_receipt_date',"gl_account_number"]

    for field in fields_to_search_for:
        sap_value = key_to_sap_attributes_mapping.get(field, None)
        if sap_value is None:
            continue
        elif isinstance(sap_value, list):
            result = []
            if field == "vat_tax_code":
                # for vat tax code, generate comprehensive variants with spaces/hyphens at all positions
                variants = set()
                
                def generate_variants(text: str) -> set:
                    """Generate all combinations of spaces and hyphens at different positions."""
                    text = str(text).strip()
                    if not text:
                        return set()
                    
                    # Start with cleaned base (no spaces/hyphens)
                    base = text.replace(" ", "").replace("-", "")
                    local_variants = {text, base}  # Original and cleaned
                    
                    if len(base) <= 1:
                        return local_variants
                    
                    # Generate variants with separators between characters
                    for sep in ["", " ", "-"]:
                        if sep == "":
                            continue  # Already have base
                        # Between all chars
                        local_variants.add(sep.join(base))
                        
                        # Between specific positions (up to reasonable length)
                        if len(base) <= 10:  # Optimize for common VAT code lengths
                            for i in range(1, len(base)):
                                # Single separator at position i
                                variant = base[:i] + sep + base[i:]
                                local_variants.add(variant)
                                
                                # For multi-part codes (e.g., XX-XXXXXXX-X pattern)
                                if i < len(base) - 1:
                                    for j in range(i + 1, len(base)):
                                        variant = base[:i] + sep + base[i:j] + sep + base[j:]
                                        local_variants.add(variant)
                    
                    # Common transformations
                    local_variants.add(text.replace(" ", ""))  # Remove spaces
                    local_variants.add(text.replace("-", ""))  # Remove hyphens
                    local_variants.add(text.replace(" ", "-"))  # Space to hyphen
                    local_variants.add(text.replace("-", " "))  # Hyphen to space
                    local_variants.add(" ".join(text.split()))  # Normalize spaces
                    
                    return local_variants
                
                # Generate variants for all values
                for val in sap_value:
                    variants.update(generate_variants(val))
                
                log_message(f"Generated VAT tax code variants for searching: {variants}")
                
                # Search for any matching variant
                for variant in variants:
                    found = search_value_in_text_lines(value=variant, text_lines=text_lines)
                    if found:
                        result.append(sap_value[0] if sap_value else variant)
                        break
                
                if result:
                    manual_results[field] = result
                continue

            for val in sap_value:
                found = search_value_in_text_lines(value=val, text_lines=text_lines)
                if found:
                    # manual_results[field] = val
                    # if field == "gl_account_number":
                    result.append(val)
                    # else:
                    #     break
                    manual_results[field] = result
        else:
            found = search_value_in_text_lines(value=sap_value, text_lines=text_lines)
            if found:
                manual_results[field] = sap_value


    return manual_results



def validate_extracted_gl_account_numbers_list(extracted_values):
    from invoice_verification.logger.logger import log_message
    if not extracted_values or not isinstance(extracted_values, list) or len(extracted_values) == 0:
        return extracted_values
    
    try:

        sources_path = find_sources_dir()  
        gl_acc_path = os.path.join(str(sources_path),"TR system - GL account details.xlsx")
        if os.path.exists(gl_acc_path):
            trd403_df = pd.read_excel(gl_acc_path, sheet_name="TRD - GL Account")
            trm_655_df = pd.read_excel(gl_acc_path, sheet_name="TRM655 - GL Account")
            trm663_df = pd.read_excel(gl_acc_path, sheet_name="TRM663 - GL Account")  
            valid_gl_accounts = set()
            for df in [trm_655_df, trm663_df, trd403_df]:
                if 'GL Account' in df.columns:
                        valid_gl_accounts.update(df['GL Account'].dropna().astype(int).astype(str).str.strip().tolist())   

            results = []
            not_present = []
            for gl_account in extracted_values:
                if gl_account and str(gl_account).strip() in valid_gl_accounts:
                    results.append(str(gl_account).strip())
                else:
                    not_present.append(str(gl_account).strip())

            log_message(f"From original List {extracted_values}, Filtered GL Numbers: {results}")
            log_message(f"GL Account Numbers not present in TR system data: {not_present}")
            return results         
        else:
            log_message(f"GL Account details file not found at path: {gl_acc_path}")
            return extracted_values

    except Exception as e:
        log_message(f"Error in validating GL Account Numbers: {e}")
        return extracted_values



import re
from typing import List


def filter_ocr_noise_hybrid(
    lines: List[str],
    min_long_length: int = 50,
    prose_block_min_lines: int = 3,
    window_size: int = 12,
    max_numeric_ratio: float = 0.2,
    max_prose_windows_before_stop: int = 3) -> List[str]:
    """
    Hybrid OCR noise filter for invoices.

    - Uses normalized text ONLY for decision-making
    - Returns original OCR lines unmodified
    """

    if not lines or not isinstance(lines, list):
        return []

    # ---- Preserve originals ----
    original_lines = [
        line for line in lines
        if isinstance(line, str) and line.strip()
    ]

    # ---- Normalized view (analysis only) ----
    normalized_lines = [
        re.sub(r"\s+", " ", line).strip()
        for line in original_lines
    ]

    def has_digit(text: str) -> bool:
        return any(c.isdigit() for c in text)

    def is_long_prose(text: str) -> bool:
        return len(text) >= min_long_length and not has_digit(text)

    output: List[str] = []

    i = 0
    prose_dominant_window_streak = 0

    while i < len(normalized_lines):

        # ---- Step 1: detect local prose block ----
        block_indices = []

        while i < len(normalized_lines) and is_long_prose(normalized_lines[i]):
            block_indices.append(i)
            i += 1

        # ---- Step 2: handle prose block ----
        if block_indices:
            if len(block_indices) >= prose_block_min_lines:
                # Drop local prose block → do nothing
                pass
            else:
                # Single long line → keep original
                for idx in block_indices:
                    output.append(original_lines[idx])
        else:
            # Normal line → keep original
            output.append(original_lines[i])
            i += 1

        # ---- Step 3: global prose dominance detection ----
        if i + window_size <= len(normalized_lines):
            window = normalized_lines[i : i + window_size]

            numeric_lines = sum(has_digit(l) for l in window)
            long_prose_lines = sum(is_long_prose(l) for l in window)

            numeric_ratio = numeric_lines / window_size
            prose_ratio = long_prose_lines / window_size

            if numeric_ratio <= max_numeric_ratio and prose_ratio >= 0.6:
                prose_dominant_window_streak += 1
            else:
                prose_dominant_window_streak = 0

            if prose_dominant_window_streak >= max_prose_windows_before_stop:
                break

    return output

def swap_vendor_bill_to_details_if_needed(sap_row: SAPRow,
                                    extracted_dict: dict) -> dict:
    """
    Swap vendor and bill-to details in extracted_dict if they appear to be swapped
    based on SAP row data.
    """

    vendor_name = extracted_dict.get("vendor_name", None)
    vendor_address = extracted_dict.get("vendor_address", None)
    legal_entity_name = extracted_dict.get("legal_entity_name", None)
    legal_entity_address = extracted_dict.get("legal_entity_address", None)

    sap_vendor_name = sap_row.vendor_name
    sap_vendor_address = sap_row.vendor_address
    sap_legal_entity_name = sap_row.legal_entity_name
    sap_legal_entity_address = sap_row.legal_entity_address

    # Normalize names for comparison
    norm_vendor_name = normalize_legal_entity_name(vendor_name)
    norm_legal_entity_name = normalize_legal_entity_name(legal_entity_name)
    norm_sap_vendor_name = normalize_legal_entity_name(sap_vendor_name)
    norm_sap_legal_entity_name = normalize_legal_entity_name(sap_legal_entity_name)
    norm_vendor_address = normalize_legal_entity_name(vendor_address)
    norm_legal_entity_address = normalize_legal_entity_name(legal_entity_address)
    norm_sap_vendor_address = normalize_legal_entity_name(sap_vendor_address)
    norm_sap_legal_entity_address = normalize_legal_entity_name(sap_legal_entity_address)

    # Check for potential swap using partial matching
    vendor_matches_legal_entity = partial_match(norm_vendor_name, norm_sap_legal_entity_name)
    legal_entity_matches_vendor = partial_match(norm_legal_entity_name, norm_sap_vendor_name)

    # Check addresses if available
    vendor_address_matches_legal_entity = partial_match(norm_vendor_address, norm_sap_legal_entity_address)
    legal_entity_address_matches_vendor = partial_match(norm_legal_entity_address, norm_sap_vendor_address)

    if vendor_matches_legal_entity is not None and vendor_matches_legal_entity:
        log_message("Detected potential swap between vendor and bill-to details based on names. Swapping them.")
        # Swap Vendor name to legal entity name
        log_message(f"SAP row vendor name: '{sap_vendor_name}', legal entity name: '{sap_legal_entity_name}'")
        log_message(f"Swapping extracted vendor name '{vendor_name}' to extracted legal entity name '{legal_entity_name}'")
        extracted_dict["legal_entity_name"] = vendor_name
    
    if legal_entity_matches_vendor is not None and legal_entity_matches_vendor:
        log_message("Detected potential swap between vendor and bill-to details based on names. Swapping them.")
        # Swap Legal entity name to vendor name
        log_message(f"SAP row vendor name: '{sap_vendor_name}', legal entity name: '{sap_legal_entity_name}'")
        log_message(f"Swapping extracted legal entity name '{legal_entity_name}' to extracted vendor name '{vendor_name}'")
        extracted_dict["vendor_name"] = legal_entity_name

    if vendor_address_matches_legal_entity is not None and vendor_address_matches_legal_entity:
        log_message("Detected potential swap between vendor and bill-to details based on addresses. Swapping them.")
        # Swap Vendor address to legal entity address
        log_message(f"SAP row vendor address: '{sap_vendor_address}', legal entity address: '{sap_legal_entity_address}'")
        log_message(f"Swapping extracted vendor address '{vendor_address}' to extracted legal entity address '{legal_entity_address}'")
        extracted_dict["legal_entity_address"] = vendor_address

    if legal_entity_address_matches_vendor is not None and legal_entity_address_matches_vendor:
        log_message("Detected potential swap between vendor and bill-to details based on addresses. Swapping them.")
        # Swap Legal entity address to vendor address
        log_message(f"SAP row vendor address: '{sap_vendor_address}', legal entity address: '{sap_legal_entity_address}'")
        log_message(f"Swapping extracted legal entity address '{legal_entity_address}' to extracted vendor address '{vendor_address}'")
        extracted_dict["vendor_address"] = legal_entity_address

    return extracted_dict