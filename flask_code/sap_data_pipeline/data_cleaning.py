import pandas as pd
import numpy as np

def clean_amount_column(series: pd.Series) -> pd.Series:
    """
    Cleans SAP-style amount fields:
    - Handles European format (dot as thousand separator, comma as decimal)
    - Handles US format (comma as thousand separator, dot as decimal)
    - Moves trailing '-' (negative sign) to the front
    - Removes leading '/' if present
    - Converts cleaned string to float

    Args:
        series (pd.Series): Raw amount column (string or mixed type).

    Returns:
        pd.Series: Cleaned numeric column (float64), NaN for invalid values.
    
    Examples:
        European format: "55.00,45" → 5500.45
        European format: "1.234,56" → 1234.56
        US format: "55,000.45" → 55000.45
        Negative: "123.45-" → -123.45
    """
    try:
        if not isinstance(series, pd.Series):
            raise TypeError("Input must be a pandas Series.")

        # Convert all to string for uniform cleaning
        cleaned = series.astype(str)

        # Remove leading "/" (SAP export artifacts)
        cleaned = cleaned.str.lstrip('/')

        # Strip spaces early
        cleaned = cleaned.str.strip()

        # Define function to detect and fix European format
        def fix_european_format(val: str) -> str:
            """
            Detects European number format where:
            - Dot (.) is thousand separator
            - Comma (,) is decimal separator
            
            Detection: If comma appears after the last dot position-wise,
            it's European format (e.g., "1.234,56" or "55.00,45")
            """
            if pd.isna(val) or val == '' or val == 'nan' or val == 'None':
                return val
            
            last_dot = val.rfind('.')
            last_comma = val.rfind(',')
            
            # European format: comma appears after dot (position-wise)
            # Examples: "1.234,56", "55.00,45", "1.234.567,89"
            if last_comma > last_dot and last_comma != -1:
                # Remove all dots (thousand separators)
                # Replace comma with dot (decimal separator)
                val = val.replace('.', '').replace(',', '.')
            # US format: dot appears after comma or no comma at all
            # Examples: "1,234.56", "55,000.45"
            elif last_dot > last_comma or (last_dot != -1 and last_comma == -1):
                # Remove all commas (thousand separators)
                val = val.replace(',', '')
            # Only comma, no dot: could be European decimal
            # Example: "1234,56"
            elif last_comma != -1 and last_dot == -1:
                val = val.replace(',', '.')
            # Only dot, no comma: already correct
            # Example: "1234.56"
            else:
                val = val.replace(',', '')
            
            return val

        # Apply European format fix to each value
        cleaned = cleaned.apply(fix_european_format)

        # Move trailing '-' to the front (e.g., "123.45-" → "-123.45")
        cleaned = cleaned.str.replace(r'^([0-9.]+)-$', r'-\1', regex=True)

        # Convert to float, coercing invalid to NaN
        cleaned = pd.to_numeric(cleaned, errors='coerce')

        return cleaned
    except Exception as e:
        from .logger_config import get_logger
        logger = get_logger()
        logger.error(f"Error in clean_amount_column: {e}")
        raise ValueError(f"Failed to clean amount column. Error: {e}") 


def clean_date_column(series: pd.Series, can_be_null: bool = True) -> pd.Series:
    """
    Cleans SAP-style date fields into standard pandas datetime64[ns].

    Handles:
    - Formats like '2024.04.23', '20240423', '2024-04-23'
    - Placeholder dates like '0000.00.00', '00000000' → converted to NaT (if can_be_null=True)
    - Trims whitespace and invalid characters
    - Returns datetime64[ns] (NaT or raises error based on `can_be_null`)

    Args:
        series (pd.Series): Raw date column (string or mixed types).
        can_be_null (bool): 
            True  → invalid or missing dates are coerced to NaT (default).
            False → invalid dates raise a ValueError.

    Returns:
        pd.Series: Cleaned pandas datetime64[ns] column.
    """
    if not isinstance(series, pd.Series):
        raise TypeError("Input must be a pandas Series.")
    
    cleaned = series.copy()

    # Convert all to string
    cleaned = cleaned.astype(str).str.strip()

    # --- 1. NORMALIZE SEPARATORS FIRST ----------------------------------------
    # Replace separators by hyphens
    cleaned = cleaned.str.replace('/', '-', regex=False)
    cleaned = cleaned.str.replace('.', '-', regex=False)
    
    # --- 2. PLACEHOLDERS / EMPTY VALUES ---------------------------------------
    placeholder_patterns = [
        r'^$',            # empty after strip
        r'^None$',        # literal
        r'^NaT$',         # literal
        r'^nan$',         # literal
        r'^0+$',          # 0, 00000000, 0000.00.00, etc.
        r'^0+-0+-0+$',    # 0000-00-00, 00-00-0000, 0-0-0, etc. (after normalization)
    ]
    placeholder_regex = "(" + ")|(".join(placeholder_patterns) + ")"
    placeholder_mask = cleaned.str.match(placeholder_regex, na=False)

    if can_be_null:
        cleaned.loc[placeholder_mask] = np.nan
    else:
        if placeholder_mask.any():
            raise ValueError(
                "Empty/invalid date values not allowed: "
                f"{cleaned[placeholder_mask].unique()}"
            )

        
    # --- 3. PATTERN MATCHING FOR ALLOWED FORMATS ------------------------------
    # Matches:
    valid_regex = (
        r'^(\d{2}-\d{2}-\d{4})$'  # mm-dd-yyyy (after normalization)
        r'|^(\d{8})$'              # yyyymmdd
    )

    valid_mask = cleaned.str.match(valid_regex, na=False)
    invalid_mask = (~valid_mask) & (~cleaned.isna())

    # Replace invalid dates with NaN (no error raised)
    if invalid_mask.any():
        cleaned.loc[invalid_mask] = np.nan   #type: ignore
        #raise ValueError(
        #    f"Invalid date format encountered: {cleaned[invalid_mask].unique()}. "
        #    "Allowed formats: MM-DD-YYYY, MM/DD/YYYY, MM.DD.YYYY, YYYYMMDD"
        #)

    # # Expand compact YYYYMMDD → YYYY-MM-DD
    # compact_mask = cleaned.str.match(r'^\d{8}$', na=False)
    # if compact_mask.any():
    #     cleaned.loc[compact_mask] = (
    #         cleaned.loc[compact_mask]
    #         .str.replace(
    #             r'(\d{4})(\d{2})(\d{2})',
    #             r'\1-\2-\3',
    #             regex=True
    #         )
    #     )

    year_series = cleaned.str[-4:].astype('Int64')
    non_null_mask = cleaned.notna()

    # Identify out-of-bound years (greater than pd.Timestamp.max.year) and set to NaN
    out_of_bound_mask = non_null_mask & (year_series > pd.Timestamp.max.year)

    if out_of_bound_mask.any():
        cleaned.loc[out_of_bound_mask] = np.nan  #type: ignore


   # --- 4. CONVERT TO DATETIME (STRICT) --------------------------------------
    try:
        result = pd.to_datetime(cleaned, format="%m-%d-%Y", errors="raise")
    except Exception as e:
        raise ValueError(
            "Invalid calendar dates in column."
        ) from e

    return result


def add_quarter_label(df: pd.DataFrame, date_col: str, label_col: str = 'QUARTER_LABEL') -> pd.DataFrame:
    """
    Adds a quarter label column (e.g., Q1-2025) based on a given datetime column.
    """
    if date_col not in df.columns:
        raise KeyError(f"Date column '{date_col}' not found in DataFrame.")
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        raise TypeError(f"Column '{date_col}' must be datetime dtype. Convert using pd.to_datetime first.")
    
    df[label_col] = (
        'q' + df[date_col].dt.quarter.astype(int).astype(str) + '_' + df[date_col].dt.year.astype(int).astype(str)
    )
    return df
