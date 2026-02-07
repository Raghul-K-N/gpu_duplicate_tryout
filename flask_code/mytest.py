import pandas as pd


def get_duplicate_label(group):
    """Extract MONTH_LABEL from current data rows, fallback to any row if none exist"""
    current_labels = group.loc[group['is_current_data'], 'MONTH_LABEL']
    return current_labels.iloc[0] if not current_labels.empty else group['MONTH_LABEL'].iloc[0]

# Create a dataframe with columns DUPLICATES_ID , is_current_data and MONTH_LABEL,
# each DUPLICATES_ID  should have atleast two rows with different MONTH_LABEL values and is_current_data values
df_output = pd.DataFrame({
    'DUPLICATES_ID': [1, 1, 2, 2, 3, 3,4,4,4],
    'is_current_data': [True, False, False, True, False, False, True, False, True],
    'MONTH_LABEL': ['m5-2024', 'm4-2024', 'm3-2024', 'm6-2024', 'm1-2024', 'm2-2024', 'm7-2024', 'm8-2024', 'm9-2024']
})

# Create dict mapping DUPLICATES_ID to duplicate_label
duplicate_label_dict = df_output.groupby('DUPLICATES_ID').apply(get_duplicate_label).to_dict()
df_output['duplicate_label'] = df_output['DUPLICATES_ID'].map(duplicate_label_dict)


print(df_output)