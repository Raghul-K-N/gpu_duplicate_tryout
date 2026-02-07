# filepath=r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\merged_data\sap_data_pipeline_test_output_20251119_164356.csv"
import pandas as pd
import numpy as np


def clean_amount_column(series: pd.Series) -> pd.Series:
    """
    Cleans SAP-style amount fields:
    - Removes commas as thousand separators.
    - Moves trailing '-' (negative sign) to the front.
    - Removes leading '/' if present.
    - Converts cleaned string to float.

    Args:
        series (pd.Series): Raw amount column (string or mixed type).

    Returns:
        pd.Series: Cleaned numeric column (float64), NaN for invalid values.
    """
    try:
        if not isinstance(series, pd.Series):
            raise TypeError("Input must be a pandas Series.")

        # Convert all to string for uniform cleaning
        cleaned = series.astype(str)

        # Remove leading "/" (SAP export artifacts)
        cleaned = cleaned.str.lstrip('/')

        # Remove commas
        cleaned = cleaned.str.replace(',', '', regex=False)

        # Move trailing '-' to the front (e.g., "123.45-" → "-123.45")
        cleaned = cleaned.str.replace(r'^([0-9.]+)-$', r'-\1', regex=True)

        # Strip any remaining spaces
        cleaned = cleaned.str.strip()

        # Convert to float, coercing invalid to NaN
        cleaned = pd.to_numeric(cleaned, errors='coerce')

        return cleaned
    except Exception as e:
        print(f"Error in clean_amount_column: {e}")
        raise ValueError(f"Failed to clean amount column. Error: {e}") 




# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\merged_data\sap_data_pipeline_test_output_20251119_171322.csv"
# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\merged_data\sap_data_pipeline_test_output_20251121_072738.csv"
# filepath=r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-no-23\merged_data\sap_data_pipeline_test_output_20251123_095800.csv"
# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-no-23\merged_data\sap_data_pipeline_test_output_20251125_123404.csv"
# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\Z_sap_data_pipeline_test_output_20251201_120626.csv"
# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\Z_sap_data_pipeline_test_output_20251202_141203.csv"
# filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\Z_sap_data_pipeline_test_output_20251209_073707.csv"
filepath = r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\Z_sap_data_pipeline_test_output_20251217_061321.csv"


import os
import pandas as pd
import numpy as np

df = pd.read_csv(filepath)
print(df.shape)

# print(df[df['DOCUMENT_NUMBER']==1700000021][['DOCUMENT_NUMBER','COMPANY_CODE',]].value_counts())
print(df['ACCOUNT_DOC_ID'].nunique(),df['ACCOUNT_DOC_ID'].max(),df['ACCOUNT_DOC_ID'].min())
df['REGION_BSEG'].fillna('NAA', inplace=True)
print(df['REGION_BSEG'].value_counts())
print('NO OF ACC DOCS PER REGION',df.groupby('REGION_BSEG')['ACCOUNT_DOC_ID'].nunique())
regions_to_consider = ['NAA','EMEA']
df_filtered = df[df['REGION_BSEG'].isin(regions_to_consider)]
print(df_filtered.shape)

print(df_filtered['ACCOUNT_DOC_ID'].nunique())
df_filtered.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\all_acc_docs_dec17.csv", index=False)

# for client in df_filtered['CLIENT'].unique().tolist():
#     temp_df = df_filtered[df_filtered['CLIENT']==client]
#     print(f"Client: {client}, Shape: {temp_df.shape}, Unique ACCOUNT_DOC_IDs: {temp_df['ACCOUNT_DOC_ID'].nunique()}, Min max of acc doc ids: {temp_df['ACCOUNT_DOC_ID'].min()} - {temp_df['ACCOUNT_DOC_ID'].max()}")
# # print(df_filtered['DOCUMENT_TYPE'].value_counts())

# other_than_403_df = df_filtered[df_filtered['CLIENT']!=403]
# print("Other than 403 data shape:", other_than_403_df.shape)
# print("Other than 403 unique ACCOUNT_DOC_IDs:", other_than_403_df['ACCOUNT_DOC_ID'].nunique())
# print(other_than_403_df['CLIENT'].value_counts(),other_than_403_df['REGION_BSEG'].value_counts())

# other_than_403_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\other_than_403.csv", index=False)


# # docs_detaisl_to_filter = [ ('1700000026','4006'),('1700000059','921'),('1700000041','8'),('1700000001','4004')]
# docs_detaisl_to_filter = [ ('1700000011','25')]
# dfs = []
# for doc_num, comp_code in docs_detaisl_to_filter:
#     temp_df = df_filtered[(df_filtered['DOCUMENT_NUMBER']==int(doc_num)) & (df_filtered['COMPANY_CODE']==int(comp_code))]
#     if temp_df.shape[0]>0:
#         dfs.append(temp_df)
#         print('Found document:', doc_num, 'company code:', comp_code, 'shape:', temp_df.shape)
#     else:
#         print(f"Document {doc_num} with company code {comp_code} not found in data.")

# final_df = pd.concat(dfs)
# print(final_df.shape)
# print(final_df['ACCOUNT_DOC_ID'].nunique())
# final_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\dec14_test.csv", index=False)
# docs_with_attachments = pd.read_excel(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\attachment_validation_output_dec07.xlsx")
# print(docs_with_attachments.shape)
# docs_with_attachments  = docs_with_attachments[docs_with_attachments['attachment_count']>0]
# print(docs_with_attachments.shape)
# print(docs_with_attachments.head(3))
# print(docs_with_attachments['file_format'].nunique() ,docs_with_attachments['Document Number'].nunique())


# df_filtered['file_format'] = df_filtered['COMPANY_CODE'].astype(str).str.strip().str.zfill(4) +\
#       "_" + df_filtered['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper() \
#     + "_" + df_filtered['FISCAL_YEAR'].astype(str).str.strip().str.upper()
# print(df_filtered.shape)
# final_df = df_filtered[df_filtered['file_format'].isin(docs_with_attachments['file_format'].unique().tolist())]
# print(final_df.shape)
# print(final_df['ACCOUNT_DOC_ID'].nunique())
# doc_types = final_df['DOCUMENT_TYPE'].unique().tolist()
# doc_ids_to_filter = []
# print(len(doc_types))
# for doc_type in doc_types:
#     temp_df = final_df[final_df['DOCUMENT_TYPE']==doc_type]
#     sampled_ids = np.random.choice(temp_df['ACCOUNT_DOC_ID'].unique(), size=min(10, temp_df['ACCOUNT_DOC_ID'].nunique()), replace=False)
#     doc_ids_to_filter.extend(sampled_ids.tolist())

# print(len(doc_ids_to_filter),doc_ids_to_filter)

# final_df = final_df[final_df['ACCOUNT_DOC_ID'].isin(doc_ids_to_filter)]
# print(final_df.shape)

# final_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\filtered_docs_dec12.csv", index=False)


# final_df = df_filtered[df_filtered['ACCOUNT_DOC_ID'].isin([7,11,77,78,80,81,82,103,121,122]) ]

# print(final_df.shape,final_df['ACCOUNT_DOC_ID'].nunique())


# final_df=df_filtered[(df_filtered['ACCOUNT_DOC_ID']>100) & (df_filtered['ACCOUNT_DOC_ID']<151) ].copy()
# print(final_df.shape, final_df['ACCOUNT_DOC_ID'].nunique())

# final_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\first_101_150_acc_docs_dec9.csv", index=False)

# existing_docs = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\new_150_sampled_sap_data_dec08.csv")
# existing_docs2 = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\final_80_sampled_sap_data_dec08.csv")
# existing_docs = pd.concat([existing_docs, existing_docs2])
# print("Existing  docs shape:", existing_docs.shape)
# print("Existing  docs unique ACCOUNT_DOC_IDs:", existing_docs['ACCOUNT_DOC_ID'].nunique())


# other_data = df_filtered[~df_filtered['ACCOUNT_DOC_ID'].isin(existing_docs['ACCOUNT_DOC_ID'].unique().tolist())]
# print("Other data shape:", other_data.shape)
# print("Other data unique ACCOUNT_DOC_IDs:", other_data['ACCOUNT_DOC_ID'].nunique())


# other_data.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\other_sap_data_163_dec08.csv", index=False)
# # Sample 100 acc_doc ids from other_data
# sampled_acc_doc_ids = np.random.choice(other_data['ACCOUNT_DOC_ID'].unique(), size=150, replace=False)
# sampled_df = other_data[other_data['ACCOUNT_DOC_ID'].isin(sampled_acc_doc_ids)]
# print("Sampled df shape:", sampled_df.shape)
# print("Sampled df unique ACCOUNT_DOC_IDs:", sampled_df['ACCOUNT_DOC_ID'].nunique())

# # final_merged_df = pd.concat([existing_docs, sampled_df])
# final_merged_df = sampled_df.copy()
# print("Final merged df shape:", final_merged_df.shape)
# print("Final merged df unique ACCOUNT_DOC_IDs:", final_merged_df['ACCOUNT_DOC_ID'].nunique())
# final_merged_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\new_150_sampled_sap_data_dec08.csv", index=False)

# # Sample df based on distribution of DOCUMENT_TYPE, sampling by ACCOUNT_DOC_ID
# document_type_counts = df_filtered['DOCUMENT_TYPE'].value_counts()
# print("Document Type Counts:\n", document_type_counts)

# # Get unique ACCOUNT_DOC_IDs per DOCUMENT_TYPE
# unique_acc_docs_per_type = df_filtered.groupby('DOCUMENT_TYPE')['ACCOUNT_DOC_ID'].nunique()
# print("Unique Account Doc IDs per Document Type:\n", unique_acc_docs_per_type)

# # Calculate the proportion of each DOCUMENT_TYPE based on unique account doc IDs
# document_type_proportions = unique_acc_docs_per_type / unique_acc_docs_per_type.sum()
# print("Document Type Proportions:\n", document_type_proportions)

# # Desired sample size (number of unique ACCOUNT_DOC_IDs)
# desired_sample_size = 30

# # Determine the number of ACCOUNT_DOC_IDs to sample from each DOCUMENT_TYPE
# samples_per_type = (document_type_proportions * desired_sample_size).round().astype(int)
# print("Samples per Document Type:\n", samples_per_type)

# # Perform stratified sampling by ACCOUNT_DOC_ID
# sampled_dfs = []
# for doc_type, sample_size in samples_per_type.items():
#     # Get unique ACCOUNT_DOC_IDs for this document type
#     doc_type_acc_ids = df_filtered[df_filtered['DOCUMENT_TYPE'] == doc_type]['ACCOUNT_DOC_ID'].unique()
    
#     # Sample the required number of ACCOUNT_DOC_IDs
#     sampled_acc_ids = np.random.choice(doc_type_acc_ids, size=min(sample_size, len(doc_type_acc_ids)), replace=False)
    
#     # Get all rows for the sampled ACCOUNT_DOC_IDs
#     sampled_df = df_filtered[(df_filtered['DOCUMENT_TYPE'] == doc_type) & 
#                              (df_filtered['ACCOUNT_DOC_ID'].isin(sampled_acc_ids))]
#     sampled_dfs.append(sampled_df)

# df_stratified_sampled = pd.concat(sampled_dfs)
# print("Stratified Sampled DataFrame Shape:", df_stratified_sampled.shape)
# print("Stratified Sampled DataFrame Document Type Counts:\n", df_stratified_sampled['DOCUMENT_TYPE'].value_counts())
# print("Unique ACCOUNT_DOC_IDs in sample:", df_stratified_sampled['ACCOUNT_DOC_ID'].nunique())

# df_stratified_sampled.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\sap_stratified_sampled_data_dec08.csv", index=False)

# filtered_data = df_filtered[df_filtered['ACCOUNT_DOC_ID']<=125]
# print(filtered_data.shape)
# print(filtered_data['ACCOUNT_DOC_ID'].nunique())

# filtered_data.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\zblock_sap_data_dec2_filtered_125_accdocs.csv",index=False)
# df_filtered['NET_PRICE'] = clean_amount_column(df_filtered['NET_PRICE'])
# df_filtered['GROSS_VALUE'] = clean_amount_column(df_filtered['GROSS_VALUE'])
# df_filtered['PO_QUANTITY'] = clean_amount_column(df_filtered['PO_QUANTITY'])
# # CREATED_ON column to datetime
# df_filtered['CREATED_ON'] = pd.to_datetime(df_filtered['CREATED_ON'], errors='coerce')
# df_filtered['PURCHASING_DOCUMENT_DATE'] = pd.to_datetime(df_filtered['PURCHASING_DOCUMENT_DATE'], errors='coerce')

# already_processed_df = pd.read_csv(r"c:\Users\ShriramSrinivasan\Documents\zblock_acc_docflat_processed_150.csv")
# print('already processed:',already_processed_df.shape)
# acc_docs_processed = already_processed_df['COMPANY_CODE'].astype(str)+'_'+already_processed_df['ENTRY_ID'].astype(str)
# print(already_processed_df['COMPANY_CODE'].value_counts())
# print('Before processing:',df_filtered.shape)
# df_filtered['unique_id'] = df_filtered['COMPANY_CODE'].astype(str)+'_'+df_filtered['DOCUMENT_NUMBER'].astype(str)
# df_filtered = df_filtered[df_filtered['unique_id'].isin(acc_docs_processed)]
# print(df_filtered.shape)
# print(df_filtered['ACCOUNT_DOC_ID'].nunique())

# uat_df = pd.read_excel(r"C:\Users\ShriramSrinivasan\Downloads\ThinkRisk Invoice Audit - Test Scenario Phase 1 Updated.xlsx",sheet_name='24-11-2025',skiprows=1)
# print(uat_df.columns)
# print(uat_df.shape)
# print(uat_df.head(3))

# # Document #, Co code, Year
# val1 = uat_df['Document # '].astype(str).str.strip().str.upper()
# val2 = uat_df['Co code'].astype(str).str.strip().str.upper().str.zfill(4)
# val3 = uat_df['Year '].astype(str).str.strip().str.upper()
# uat_df['unique_id'] = val1 + "_" + val2 + "_" + val3


# va1 = df_filtered['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper()
# va2 = df_filtered['COMPANY_CODE'].astype(str).str.strip().str.upper().str.zfill(4)
# va3 = df_filtered['FISCAL_YEAR'].astype(str).str.strip().str.upper()
# df_filtered['unique_id'] = va1 + "_" + va2 + "_" + va3

# print(uat_df['unique_id'].value_counts())
# acc_docs_to_consider = uat_df['unique_id'].unique().tolist()
# print(f"Total account documents to consider from UAT: {(len(acc_docs_to_consider))}")

# final_df = df_filtered[df_filtered['unique_id'].isin(acc_docs_to_consider)]
# print(final_df.shape)
# print(final_df['ACCOUNT_DOC_ID'].nunique())

# print('Missing account docs from UAT:',set(acc_docs_to_consider) - set(final_df['unique_id'].unique().tolist()))

# print(final_df.shape)

# final_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\sap_data_nov_25__accdocs_28_rerun.csv",index=False)

# acc_docs_to_filter = ['1700000385','1700000380','1700000405','1700000041','1700000021']

# filtered_df = df_filtered[df_filtered['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper().isin(acc_docs_to_filter)]
# print(filtered_df.shape)



# print(filtered_df['INVOICE_DATE'].dtype)
# filtered_df.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\sap_test_nov25.csv",index=False)

# df_filtered.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\sap_data_nov23.csv",index=False)
# print(df.columns)
# # company_code_string = str(self.company_code).strip().upper().zfill(4)
# #         acc_doc_string = str(self.account_document_number).strip().upper()
# #         year_string = str(self.fiscal_year).strip().upper()
# #         format = f"{company_code_string}_{acc_doc_string}_{year_string}"
# val1 = df_filtered['COMPANY_CODE'].astype(str).str.strip().str.upper().str.zfill(4)
# val2 = df_filtered['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper()
# val3 = df_filtered['FISCAL_YEAR'].astype(str).str.strip().str.upper()

# df_filtered['file_name_format'] = val1 + "_" + val2 + "_" + val3

# print(df_filtered.shape)
# # for each in (df_filtered.isna().sum()[df_filtered.isna().sum()>0]).items():
# #     print(each)

# print([each for each in df_filtered.columns if 'amount' in each.lower() or 'rate' in each.lower()])



# amount_cols = ['LINEITEM_AMOUNT_IN_LOCAL_CURRENCY', 'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY',
#                 'TOTAL_AMOUNT_LC', 'TOTAL_AMOUNT', 'EXCHANGE_RATE',
#                 'EXCHANGE_RATE_PO','WITHHOLD_TAX_BASE_LC','WITHHOLD_TAX_BASE_FC']

# for col in amount_cols:
#     if col in df_filtered.columns:
#         df_filtered[col] = clean_amount_column(df_filtered[col])
#     else:
#         print(f"Column {col} not found in DataFrame.")

# # print(df['VENDOR_NAME'].value_counts(dropna=False))
# already_processed_df = pd.read_csv(r"c:\Users\ShriramSrinivasan\Documents\zblock_acc_docflat_processed_150.csv")
# # print(already_processed_df.head(3))

# print(df_filtered['unique_id'].nunique())
# # for each in df_filtered.columns:
# #     print(f"{each}: {df_filtered[each].isna().sum()}")

# df_filtered.to_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\inputdata\first_150_filtered_sap_data.csv",index=False)
# print(df_filtered.shape)
# # available_file_details = r"C:\Users\ShriramSrinivasan\Desktop\Dow POC duplicates\codebase\DOW UAT Attachment details.xlsx"
# # if os.path.exists(available_file_details):
# #     file_details_df = pd.read_excel(available_file_details)
# #     print(file_details_df.shape)
# #     file_details_df = file_details_df[file_details_df['count']>0]
# #     print(file_details_df.shape)
# #     print(file_details_df.head(5))
# #     docs_to_filter = file_details_df.head(5)['prefix'].unique().tolist()
# #     df_final = df_filtered[df_filtered['file_name_format'].isin(docs_to_filter)]
# #     print(df_final.shape)
# #     print(df_final['file_name_format'].nunique())
# #     df_final.to_csv('uat_sample_data.csv',index=False)


# # else:
# #     print("File does not exist.")

# # docs_to_consider = ['1700000277', '1700000147', '1700000038', '1700000039', '1700000037',
# #        '1700000036', '1700000030', '1700000027', '1700000000', '1700000059',
# #        '1700000001', '1700000225', '1700000187', '1700000009', '1700000021',
# #        '1700000005', '1700000007', '1700000118', '1700000348', '1700000000',
# #        '1700000352', '1700000353', '1700000026', '1700000132', '1700000324',
# #        '1700000065', '1700000325', '1700000070', '1700000073', '1700000002',
# #        '1700000003', '1700000004', '1700000005', '1700000017', '1700000006',
# #        '1700000008', '1700001134', '1700000049', '1700000018', '1700000019',
# #        '1700000020', '1700000019', '1700000359', '1700000360',
# #        '1700000361', '1700000364', '1700000365', '1700000366', '1700000214',
# #        '1700001308', '1700000060', '1700000370', '1700000145', '1700000369',
# #        '1700000371', '1700000368', '1700000052', '1700000053']


# # final_df = df_filtered[df_filtered['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper().isin(docs_to_consider)]
# # print(final_df.shape)

# # # check how many docs are present in final_df that matches with the above list
# # set_final_docs = set(final_df['DOCUMENT_NUMBER'].astype(str).str.strip().str.upper().tolist())
# # missing_docs = set(docs_to_consider) - set_final_docs
# # print(f"Total documents to consider: {len(docs_to_consider)}")
# # print(f"Documents found in data: {len(set_final_docs)}")
# # print(f"Missing documents: {len(missing_docs)}")
# # final_df.to_csv('uat_sample_data_47_docs_filtered.csv',index=False)


# # file_details_df = pd.read_excel(available_file_details)
# # df = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\uat_sample_data.csv")
# # print(df.shape)
# # print(df['COMPANY_CODE'].value_counts())
# # print(df['DOCUMENT_NUMBER'].value_counts())