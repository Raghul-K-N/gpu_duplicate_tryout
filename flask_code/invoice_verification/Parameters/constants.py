from typing import Dict, Optional, List
import pandas as pd
from .utils import find_sources_dir
import os

"""
constants.py

Hardcoded path to an Excel file that contains company-code <-> country mapping.
Provides functions to load the mapping and to lookup a country by company code.
"""

# Hardcoded path to the Excel file (change filename if needed)
sources_path = find_sources_dir()
EXCEL_PATH = os.path.join(str(sources_path),"Company Code Master_All Zones 2024-v1.xlsb")
# EXCEL_PATH = r"C:\Users\ShriramSrinivasan\Downloads\OneDrive_2025-11-12\Thinkrisk data requirement Oct 23rd\Company Code Master_All Zones 2024-v1.xlsb"

COMPANY_CODE_COLUMN_NAME = "Company code"
COUNTRY_COLUMN_NAME = "Country name"



def build_country_company_map(path: str = EXCEL_PATH,
                              normalize_country: bool = True,
                              force_reload: bool = False) -> Dict[str, List[str]]:
    """
    Build and return a dict mapping country -> list of company codes.
    Example: {"india": ["0921", "0922"], "switzerland": ["090909", "080808"]}

    - country keys are lower-cased if normalize_country is True.
    - company codes are returned as strings (preserving leading zeros).
    """
    mapping = load_company_country_map(path=path, force_reload=force_reload)
    country_map: Dict[str, List[str]] = {}

    for code, country in mapping.items():
        if country is None or code is None:
            continue
        key = str(country).strip().lower() if normalize_country else str(country).strip()
        code_str = str(code).strip()
        country_map.setdefault(key, []).append(code_str)

    # optional: keep deterministic order
    for k in country_map:
        country_map[k] = sorted(country_map[k])

    return country_map


def load_company_country_map(path: str = EXCEL_PATH, force_reload: bool = False) -> Dict[str, str]:
    """
    Read the Excel file at `path` and build a dict mapping company_code -> country.
    The function tries to find columns containing 'company' and 'code' for the company code,
    and a column containing 'country' for the country.

    Returns an empty dict if the file cannot be read or the required columns are not found.
    """

    df = pd.read_excel(path, dtype=object, engine="pyxlsb")

    codes = df[COMPANY_CODE_COLUMN_NAME].dropna().astype(str).str.strip()
    countries = df[COUNTRY_COLUMN_NAME].dropna().astype(str).str.strip()
    return dict(zip(codes, countries))


def get_country_by_company_code(company_code: str) -> Optional[str]:
    """
    Given a company_code (any type), return the corresponding country string from the
    Excel mapping, or None if the company code is unknown / not present.
    """

    code_key = str(company_code).strip()

    mapping = load_company_country_map()
    return mapping.get(code_key, None)



# pre-build the country->company list mapping for convenience
# COUNTRY_COMPANY_MAP: Dict[str, List[str]] = build_country_company_map()
COUNTRY_COMPANY_MAP: Dict[str, List[str]] = {} # Company Code mapping is not neded for Pepsico
# Convenience lists for specific countries (empty list if not present)
INDIA_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("india", ["921"])
SWITZERLAND_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("switzerland", ["2120"])
GERMANY_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("germany", [])  # not used anywhere as of now
POLAND_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("poland", [])
KENYA_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("kenya", [])
CANADA_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("canada", [])
UNITED_STATES_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("united states", [])
FRANCE_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("france", [])
MOROCCO_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("morocco", [])
SOUTH_AFRICA_COMPANY_CODES: List[str] = COUNTRY_COMPANY_MAP.get("south africa", [])
TURKEY_COMPANY_CODE: List[str] = COUNTRY_COMPANY_MAP.get("turkey", [])
POLAND_COMPANY_CODE: List[str] = COUNTRY_COMPANY_MAP.get("poland", [])

CITI_BANK_VENDOR_CODES: List[str] = ["247945","2462006","1210280","796997","573928","796997","576444"]