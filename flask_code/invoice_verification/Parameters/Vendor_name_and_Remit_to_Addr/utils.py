# Parameters/Vendor name & Remmit to Addr/utils.py
"""
Utility functions for vendor name and remit to address validation
"""
import re
from rapidfuzz import fuzz
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.utils import build_validation_result, is_empty_value
from invoice_verification.constant_field_names import VENDOR_NAME, VENDOR_ADDRESS
from typing import Optional
import pandas as pd

def normalize_vendor_name(vendor_name: Optional[str], strict: bool = False) -> str:
    """
    Normalize vendor name for comparison
    Args:
        vendor_name: Raw vendor name string
        strict: If True, keep only alphanumeric and spaces (for exact matching)
    Returns:
        Normalized vendor name string
    """
    if not vendor_name:
        return ""
    
    name_str = str(vendor_name).strip().upper()
    
    if strict:
        name_str = re.sub(r'[^A-Z0-9\s]', '', name_str)
        name_str = re.sub(r'\s+', ' ', name_str)
    else:
        name_str = re.sub(r'\s+', ' ', name_str)
    
    return name_str.strip()


def normalize_address(address: Optional[str]) -> str:
    """
    Normalize address for comparison
    Args: address: Raw address string
    Returns: Normalized address string
    """
    if not address:
        return ""
    addr_str = str(address).strip().upper()
    # replacements = {
    #     'STREET': 'ST',
    #     'AVENUE': 'AVE',
    #     'ROAD': 'RD',
    #     'BOULEVARD': 'BLVD',
    #     'APARTMENT': 'APT',
    #     'SUITE': 'STE'
    # }
    # for full, abbr in replacements.items():
    #     addr_str = addr_str.replace(full, abbr)
    
    addr_str = re.sub(r'\s+', ' ', addr_str)
    return addr_str.strip()


def fuzzy_match_vendor(str1: str, str2: str, threshold: float = 80.0) -> bool:
    """
    Fuzzy match two strings using sequence matcher
    Args:
        str1: First string
        str2: Second string
        threshold: Similarity threshold (0.0 to 100.0)
    Returns:
        bool: True if similarity >= threshold
    """
    if not str1 or not str2:
        return False
    
    similarity = fuzz.ratio(str1.upper(), str2.upper())
    
    return similarity >= threshold


def exact_match_vendor(str1: str, str2: str) -> bool:
    """
    Exact match comparison for vendor names (English invoices)
    Args:
        str1: First string (normalized)
        str2: Second string (normalized)
    Returns:
        bool: True if exact match
    """
    if not str1 or not str2:
        return False
    
    return str1.upper() == str2.upper()


def get_payee_address_from_vendor_master(vendor_code: str, vendors_df: Optional[pd.DataFrame]):
    """
    Get payee address from vendor master DataFrame
    Args:
        vendor_code: Vendor code to lookup
        vendors_df: Vendor master DataFrame
    Returns:
        str: Payee address or None if not found
    """
    if vendors_df is None or vendors_df.empty:
        log_message("Vendor master DataFrame is empty")
        return None
    
    vendor_row = vendors_df[vendors_df['vendor_code'] == str(vendor_code)]
    
    if vendor_row.empty:
        log_message(f"Vendor code {vendor_code} not found in vendor master")
        return None
    
    payee_address = vendor_row.iloc[0].get('payee_address')
    
    if payee_address:
        log_message(f"Found payee address for vendor {vendor_code}")
        return str(payee_address)
    else:
        log_message(f"Payee address not found for vendor {vendor_code}")
        return None


def detect_language(vendor_name: Optional[str]):
    """
    Detect language of vendor name (english, chinese, local)
    Args:
        vendor_name: Vendor name string
    Returns:
        str: 'english', 'chinese', or 'local'
    """
    pass


def standard_vendor_validation(extracted_vendor_name: Optional[str], sap_vendor_name: str, 
                             extracted_vendor_address: Optional[str] = None, 
                             sap_vendor_address: Optional[str] = None,
                             validate_address: bool = True,
                             address_threshold: float = 75.0) -> dict:
    """
    Standard vendor validation logic for name and address
    Args:
        extracted_vendor_name: Vendor name from OCR
        sap_vendor_name: Vendor name from SAP
        extracted_vendor_address: Vendor address from OCR
        sap_vendor_address: Vendor address from SAP
        validate_address: Whether to validate address (default True)
        address_threshold: Threshold for address fuzzy matching
    Returns:
        Dict with standardized output format
    """
    log_message("Using standard vendor validation")
    
    # Check for empty/None vendor name first
    if is_empty_value(extracted_vendor_name) or is_empty_value(extracted_vendor_address):
        log_message("Global: Extracted vendor name or address is Empty or None")
        return build_validation_result(
            extracted_value={VENDOR_NAME: None, VENDOR_ADDRESS: None},
            is_anomaly=None,
            edit_operation=None,
            highlight=True,
            method='Combined',
            supporting_details={"Summary":"Vendor name or address is missing in Invoice/Voucher copy."}
        )
    
    # Normalize vendor name
    norm_extracted_name = normalize_vendor_name(extracted_vendor_name)
    norm_sap_name = normalize_vendor_name(sap_vendor_name)
    
    # Always validate vendor name
    name_match = fuzzy_match_vendor(norm_extracted_name, norm_sap_name)
    
    # Initialize address match
    address_match = False
    
    # Validate address if required
    if validate_address:
        norm_extracted_addr = normalize_address(extracted_vendor_address)
        norm_sap_addr = normalize_address(sap_vendor_address)
        address_match = fuzzy_match_vendor(norm_extracted_addr, norm_sap_addr, threshold=address_threshold)
        log_message("Validating both vendor name and address")
    else:
        address_match = True
        log_message("Skipping address validation")
    
    if name_match and address_match:
        return build_validation_result(
            extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
            is_anomaly=False,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Standard Validation."}
        )
    else:
        return build_validation_result(
            extracted_value={VENDOR_NAME: extracted_vendor_name, VENDOR_ADDRESS: extracted_vendor_address},
            is_anomaly=True,
            edit_operation=False,
            highlight=False,
            method='Automated',
            supporting_details={"Summary":"Vendor name or address mismatch detected."}
        )