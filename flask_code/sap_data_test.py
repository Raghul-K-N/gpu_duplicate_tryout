from sap_data_pipeline.main import test_sap_data_pipeline

test_sap_data_pipeline(z_block=True)

# import pandas as pd

# df = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\vendor_master_core_with_lfbk_20251201_120625.csv")
# print(df.shape)
# print(df.head(2))
# print(df.isna().sum()[df.isna().sum()>0])
# print(df.columns.tolist())
# import pandas as pd
# df = pd.read_csv(r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\nov-26\merged_data\vendor_master_core_with_lfbk_20251127_112710.csv")
# print(df.shape)
# print(df.columns)

# df.rename(columns={'SUPPLIER_ID':'VENDORCODE'},inplace=True)

# filtered_df = df.drop_duplicates(subset=['VENDORCODE','VENDOR_NAME'],keep='last')
# print(filtered_df.shape)

# filtered_df = filtered_df[['VENDORCODE','VENDOR_NAME']]
# print(filtered_df.shape)

# filepath = r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\nov-26\merged_data\ap_flow_vendor_data.csv"
# filtered_df.to_csv(filepath, index=False)


# import pandas as pd

# vendor_df = pd.read_csv(r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\nov-26\merged_data\vendor_master_core_with_lfbk_20251127_050605.csv")
# print(vendor_df.columns.tolist())
# print(vendor_df.shape)

# print(vendor_df.dtypes)

# df = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\first_150_filtered_sap_data.csv")
# print(df.shape)
# print(df['unique_id'].nunique())

# print([each for each in df.columns ])


# """
# Utility helpers, small pure functions intended to be deterministic and unit-testable.
# Keep heavy lifting here (parsers, normalizers, logger, small regexes) so region files stay concise.
# """
# from typing import Dict, Optional
# import pandas as pd
# from datetime import datetime
# from typing import Optional
# import pandas as pd
# from datetime import datetime

# def pandas_date_parser(s: str, region: Optional[str] = None) -> Optional[datetime]:
#     """
#     Parse date strings robustly with multiple format attempts.
#     Supports region-specific preferences and fallback to pandas inference.
    
#     Args:
#         s: Date string to parse
#         region: Region code ('EMEAI' for dd-mm-yyyy preference, else mm-dd-yyyy)
    
#     Returns:
#         datetime object or None
#     """
#     if not s or not isinstance(s, str):
#         return None
    
#     s = s.strip()
#     if not s:
#         return None
    
#     try:
#         # Priority 1: ISO format (yyyy-mm-dd) - unambiguous (4-digit year only)
#         for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']:
#             try:
#                 dt = pd.to_datetime(s, format=fmt, errors='raise')
#                 if not pd.isna(dt):
#                     return dt
#             except:
#                 continue
        
#         # Without separators - 8-digit yyyymmdd
#         if len(s) == 8 and s.isdigit():
#             try:
#                 dt = pd.to_datetime(s, format='%Y%m%d', errors='raise')
#                 if not pd.isna(dt):
#                     return dt
#             except:
#                 pass
        
#         # Without separators - 6-digit yymmdd
#         if len(s) == 6 and s.isdigit():
#             try:
#                 dt = pd.to_datetime(s, format='%y%m%d', errors='raise')
#                 if not pd.isna(dt):
#                     return dt
#             except:
#                 pass
        
#         # Priority 2: Region-specific formats
#         if region and region.upper().startswith('EMEA'):
#             # EMEAI: dd-mm-yyyy priority
#             formats = [
#                 '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',  # 4-digit year
#                 '%d-%m-%y', '%d/%m/%y', '%d.%m.%y'   # 2-digit year
#             ]
#         else:
#             # Other regions: mm-dd-yyyy priority
#             formats = [
#                 '%m-%d-%Y', '%m/%d/%Y', '%m.%d.%Y',  # 4-digit year
#                 '%m-%d-%y', '%m/%d/%y', '%m.%d.%y'   # 2-digit year
#             ]
        
#         for fmt in formats:
#             try:
#                 dt = pd.to_datetime(s, format=fmt, errors='raise')
#                 if not pd.isna(dt):
#                     return dt
#             except:
#                 continue
        
#         # Priority 3: Text dates (e.g., "Jan 15, 2025", "15 January 2025")
#         try:
#             dt = pd.to_datetime(s, errors='raise')
#             if not pd.isna(dt):
#                 return dt
#         except:
#             pass
        
#         # Priority 4: Fallback with region preference
#         dayfirst = bool(region and region.upper().startswith('EMEA'))
#         dt = pd.to_datetime(s, errors='coerce', dayfirst=dayfirst)
        
#         if pd.isna(dt):
#             return None
        
#         return dt
        
#     except Exception:
#         return None
    


# ## test cases

# print(pandas_date_parser("2023-12-31") == datetime(2023, 12, 31), "ISO format failed")
# print(pandas_date_parser("23-12-31") == datetime(2023, 12, 31), "ISO format with 2-digit year failed")
# print(pandas_date_parser("20231231") == datetime(2023, 12, 31), "yyyymmdd without separators failed")
# print(pandas_date_parser("231231") == datetime(2023, 12, 31), "yymmdd without separators failed")

# print(pandas_date_parser("201205"))

# print(pandas_date_parser("01-12-2023", region="EMEAI") == datetime(2023, 12, 1), "EMEAI format failed")
# print(pandas_date_parser("02-07-2024",region="EMEAI") == datetime(2024, 7, 2), "EMEAI format failed")
# print(pandas_date_parser("12-31-2023",region='EMEAI') == datetime(2023, 12, 31), "EMEAI format failed")
# print(pandas_date_parser("01/11/2024", region="EMEAI") == datetime(2024, 11, 1), "EMEAI format with slashes failed")
# print(pandas_date_parser("03.05.2025", region="EMEAI") == datetime(2025, 5, 3), "EMEAI format with dots failed")


# # mm-dd-yy, mm/dd/yy , mm.dd.yy cases test
# print(pandas_date_parser("07-12-23") == datetime(2023, 7, 12), "US format with 2-digit year failed")
# print(pandas_date_parser("11/2/24") == datetime(2024, 11, 2), "US format with slashes failed")
# print(pandas_date_parser("04.15.25") == datetime(2025, 4, 15), "US format with dots failed")

# print(pandas_date_parser("01-12-2023") == datetime(2023, 1, 12), "US format failed")
# print(pandas_date_parser("January 15, 2025") == datetime(2025, 1, 15), "Text date failed")
# print(pandas_date_parser("15 Jan 2025", region="EMEAI") == datetime(2025, 1, 15), "Text date with EMEAI failed")
# print(pandas_date_parser("invalid-date") is None, "Invalid date should return None")
# print(pandas_date_parser("") is None, "Empty string should return None")
# print(pandas_date_parser('None') is None, "None input should return None")
# print(pandas_date_parser("12th February,2024") == datetime(2024, 2, 12), "Text date with ordinal failed")

# print("All test cases passed!")
# print(pandas_date_parser("2025 Jan 16"))  # Example usage