# Parameters/Bill to LE and Addr/utils.py
"""Utility functions for bill to legal entity validation"""
import re
from rapidfuzz import fuzz
import pandas as pd
from invoice_verification.logger.logger import log_message
from typing import Optional, List

def normalize_legal_entity_name(name: Optional[str]) -> str:
    """Normalize legal entity name for comparison"""
    if not name:
        return ""
    name_str = str(name).strip().upper()
    name_str = re.sub(r'\s+', ' ', name_str)
    return name_str.strip()


def normalize_legal_entity_address(address: Optional[str]) -> str:
    """Normalize legal entity address for comparison"""
    if not address:
        return ""
    addr_str = str(address).strip().upper()
    addr_str = re.sub(r'\s+', ' ', addr_str)
    return addr_str.strip()


def exact_match_legal_entity(str1: Optional[str], str2: Optional[str]) -> bool:
    """Exact match comparison"""
    if not str1 or not str2:
        return False
    return str1.upper().strip() == str2.upper().strip()


def partial_match_legal_entity(str1: Optional[str], str2: Optional[str], threshold: float = 85.0) -> bool:
    """Partial match using fuzzy matching"""
    if not str1 or not str2:
        return False
    similarity = fuzz.partial_ratio(str1.upper(), str2.upper())
    return similarity >= threshold


# Region-specific VIP VESON email checkers
def check_naa_vip_veson_emails(eml_lines) -> bool:
    """Check for NAA VIP VESON email: fusappr@dow.com"""
    if not eml_lines:
        return False
    email_content = ' '.join(eml_lines).upper()
    return 'FUSAPPR@DOW.COM' in email_content


def check_emeai_vip_veson_emails(eml_lines) -> bool:
    """Check for EMEAI VIP VESON email: FTNAPLO@dow.com"""
    if not eml_lines:
        return False
    email_content = ' '.join(eml_lines).upper()
    return 'FTNAPLO@DOW.COM' in email_content


def check_apac_vip_veson_emails(eml_lines) -> bool:
    """Check for APAC VIP VESON email: RPVIP01@dow.com"""
    if not eml_lines:
        return False
    email_content = ' '.join(eml_lines).upper()
    return 'RPVIP01@DOW.COM' in email_content


def check_latam_vip_veson_emails(eml_lines) -> bool:
    """Check for LATAM VIP VESON emails: fbroawd@dow.com or flaoawd@dow.com"""
    if not eml_lines:
        return False
    email_content = ' '.join(eml_lines).upper()
    return 'FBROAWD@DOW.COM' in email_content or 'FLAOAWD@DOW.COM' in email_content


CANADA_APPROVED_VARIATIONS = {
    '0002': [
        'DOW CHEMICAL CANADA ULC',
        'DOW CHEM CANADA ULC'
    ],
    '4004': [
        'ROHM AND HAAS CANADA LP',
        'RH CANADA LP',
        'ROHM & HAAS CANADA LP',
        'ROHM HAAS CANADA LP'
    ],
    '2078': [
        'ALBERTA & ORIENT CLYCOL'
    ]
}

