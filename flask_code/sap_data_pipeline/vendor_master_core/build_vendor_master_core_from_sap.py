# from typing import Optional
# import pandas as pd


# def merge_lfa1_lfb1(lfa1:pd.DataFrame,
#                       lfb1:pd.DataFrame) -> pd.DataFrame:
#     """Merge LFA1 and LFB1 DataFrames on key supplier identifiers.
#     Args:
#         lfa1 (pd.DataFrame): LFA1 table DataFrame with general supplier data.
#         lfb1 (pd.DataFrame): LFB1 table DataFrame with company code-specific supplier data.
        
#     Returns:
#         pd.DataFrame: Merged DataFrame containing supplier data enriched with company code info.
#         """
#     if lfa1 is None or lfb1 is None:
#         raise ValueError("LFA1 and LFB1 DataFrames must be provided for merging.")
#     if lfa1.empty or lfb1.empty:
#         raise ValueError("LFA1 and LFB1 DataFrames cannot be empty.")
    
#     print('Shape of LFA1:', lfa1.shape)
#     print('Shape of LFB1:', lfb1.shape)

#     keys = ['Client','Supplier']
#     print("\n=== [STEP 2] LFA1+LFB1 Merge Start ===")
#     for k in keys:
#         if k not in lfa1.columns or k not in lfb1.columns:
#             raise KeyError(f"Missing join key {k} in either LFA1 or LFB1.")

#     print("Shape of LFA1 before dropping duplicates:", lfa1.shape)
#     lfa1 = lfa1.drop_duplicates(subset=keys).copy()
#     print("Shape of LFA1 after dropping duplicates:", lfa1.shape)

#     dup_count = lfb1.duplicated(subset=['Client','Supplier','Company Code']).sum()
#     if dup_count > 0:
#         raise ValueError(f"LFB1 has {dup_count} duplicate (Client, Supplier, Company Code) records — fix source data.")


#     merged = pd.merge(lfb1, lfa1, how='left', on=keys, suffixes=('_LFB1', '_LFA1'))
#     print("LFA1 and LFB1 merged. Shape:", merged.shape)
#     missing_vendors = merged['Supplier'].isna().sum()
#     if missing_vendors > 0:
#         print(f"⚠️ {missing_vendors} records in LFB1 have no matching supplier in LFA1.")
#     else:
#         print("All suppliers in LFB1 have matching records in LFA1.")
#     return merged


# def merge_lfbk(vendor_financial:pd.DataFrame,
#                lfbk:pd.DataFrame)-> pd.DataFrame :
#     """Merge vendor financial data with LFBK DataFrame on key supplier identifiers.
#     Args:
#         vendor_financial (pd.DataFrame): Vendor financial DataFrame.
#         lfbk (pd.DataFrame): LFBK table DataFrame with bank details.    
#     Returns:
#         pd.DataFrame: Merged DataFrame containing vendor financial data enriched with bank details.
#     """
#     return pd.DataFrame()  # Placeholder implementation



# def build_vendor_master_core(lfa1:pd.DataFrame,
#                               lfb1:pd.DataFrame,
#                               lfbk:Optional[pd.DataFrame]=None,
#                               lfm1:Optional[pd.DataFrame]=None) -> pd.DataFrame:
    
#     """Build vendor master core DataFrame by merging LFA1 and LFB1 tables.
#     Args:
#         lfa1 (pd.DataFrame): LFA1 table DataFrame with general supplier data.
#         lfb1 (pd.DataFrame): LFB1 table DataFrame with company code-specific supplier data.
#     Returns:
#         pd.DataFrame: Merged DataFrame containing vendor master core data.
#     """
#     print("\n=== Building Vendor Master Core from LFA1 and LFB1 ===")
#     vendor_master_core_df = merge_lfa1_lfb1(lfa1, lfb1)
#     print("=== Vendor Master Core Build Complete ===\n")
#     return vendor_master_core_df








