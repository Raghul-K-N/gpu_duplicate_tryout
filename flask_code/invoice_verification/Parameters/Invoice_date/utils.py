"""
Utility helpers, small pure functions intended to be deterministic and unit-testable.
Keep heavy lifting here (parsers, normalizers, logger, small regexes) so region files stay concise.
"""
from typing import Optional

from invoice_verification.Parameters.utils import global_pandas_date_parser

def pandas_date_parser(s: str, region: Optional[str] = None):
    return global_pandas_date_parser(s=s, region=region)
    