"""
Utility for currency normalization across all regions.
Maps symbols, localized text variants, and abbreviations to ISO 4217 currency codes.
"""

from invoice_verification.logger.logger import log_message

CURRENCY_MAPPING = {
    "$": "USD",
    "US$": "USD",
    "USD": "USD",
    "U.S. DOLLAR": "USD",
    "DOLLAR": "USD",
    "US DOLLARS": "USD",

    "€": "EUR",
    "EURO": "EUR",
    "EUROS": "EUR",
    "EUR": "EUR",

    "£": "GBP",
    "POUND": "GBP",
    "POUNDS": "GBP",
    "STERLING": "GBP",
    "GBP": "GBP",

    "₹": "INR",
    "RS": "INR",
    "INR": "INR",
    "RUPEE": "INR",
    "RUPEES": "INR",
    "RS.": "INR",

    "¥": "JPY",
    "YEN": "JPY",
    "JPY": "JPY",

    "RMB": "CNY",
    "¥CN": "CNY",
    "CNY": "CNY",
    "YUAN": "CNY",

    "A$": "AUD",
    "AUD": "AUD",
    "AU$": "AUD",
    "AUSTRALIAN DOLLAR": "AUD",

    "C$": "CAD",
    "CAD": "CAD",
    "CANADIAN DOLLAR": "CAD",

    "R$": "BRL",
    "BRL": "BRL",
    "BRAZILIAN REAL": "BRL",

    "MEX$": "MXN",
    "MXN": "MXN",
    "PESO": "MXN",
    "PESOS": "MXN",

    "R": "ZAR",
    "ZAR": "ZAR",
    "RAND": "ZAR",

    "CHF": "CHF",
    "SWISS FRANC": "CHF",

    "NONE": None
}

def normalize_currency(value: str | None) -> str | None:
    """
    Normalize OCR extracted currency to ISO standard using deterministic mapping.

    Args:
        value: Raw OCR currency value (symbol, abbreviation, or text)

    Returns:
        Standardized ISO 4217 currency code (e.g., 'USD', 'INR') or None if not determinable.
    """
    if not value:
        return None

    val = value.strip().upper().replace(".", "").replace(" ", "")

    if val in CURRENCY_MAPPING:
        return CURRENCY_MAPPING[val]

    log_message(f"Unrecognized currency variant: '{value}' — unable to normalize.")
    return None
