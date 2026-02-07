import pandas as pd
import numpy as np
from duplicate_invoices.pipeline import pipe
import time
start_time = time.time()

filepath = '/home/whirldata/Downloads/Thinkrisk ACL - Copy(Sheet1).csv'

def make_prediction(*, input_data: pd.DataFrame):
    validated_data = input_data #validate_inputs(input_data=data)
    # _pipe = load_pipeline(file_name='./duplicate_invoices/trained_models/duplicate_detector_output_v0.0.1.pkl')
    output = pipe.predict(validated_data)
    results = {"output": output}
    return results

def handle_duplicate_invoice_results(df):
            """
            manually remove duplicate invoice results based on invoice amount 
            """
            main_df = df.copy()
            print('No. of rows returned by duplicate invoice module:{}'.format(df.shape))
            # main_df.to_csv('duplicate_res_debugging.csv',index=False)
            ids_to_be_dropped = []
            rows_to_drop = []  # Global list to collect all rows to be dropped
            grouped_df = main_df.groupby('DUPLICATE_ID')
            
            for id, mydf in grouped_df:
                # Check if 'is_current_data' column exists and filter groups with all False values
                if 'is_current_data' in mydf.columns:
                    type_of_data = mydf['is_current_data'].unique()    
                    if not any(type_of_data):
                        ids_to_be_dropped.append(id)
                        continue
                
                # Get invoice amounts for the group - vectorized check
                invoice_amounts = mydf['INVOICE_AMOUNT'].values
                positive_mask = invoice_amounts > 0
                negative_mask = invoice_amounts < 0
                
                # Check if all amounts are positive or all negative
                all_positive = positive_mask.all()
                all_negative = negative_mask.all()
                
                # If all positive or all negative, keep the group as is
                if all_positive or all_negative:
                    continue
                
                # Mixed positive and negative amounts - apply matching logic
                positive_rows = mydf[positive_mask].copy()
                negative_rows = mydf[negative_mask].copy()
                
                # Create matching pairs based on SUPPLIER_NAME and INVOICE_AMOUNT_ABS
                group_rows_to_drop = []
                matched_negative_indices = set()  # Track matched negatives more efficiently
                
                for _, pos_row in positive_rows.iterrows():
                    pos_amount = pos_row['INVOICE_AMOUNT_ABS']
                    pos_supplier = pos_row['SUPPLIER_NAME']
                    
                    # Find matching negative row with same supplier and amount (excluding already matched)
                    available_negatives = negative_rows[
                        ~negative_rows.index.isin(matched_negative_indices)
                    ]
                    
                    matching_neg = available_negatives[
                        (available_negatives['SUPPLIER_NAME'] == pos_supplier) & 
                        (available_negatives['INVOICE_AMOUNT_ABS'] == pos_amount)
                    ]
                    
                    if not matching_neg.empty:
                        # Add both positive and negative rows to drop list
                        group_rows_to_drop.append(pos_row.name)
                        matched_neg_idx = matching_neg.index[0]
                        group_rows_to_drop.append(matched_neg_idx)
                        
                        # Track the matched negative row
                        matched_negative_indices.add(matched_neg_idx)
                
                # Calculate remaining rows after dropping matched pairs
                remaining_rows = len(mydf) - len(group_rows_to_drop)
                
                # If only 1 row remains after matching, drop the entire group
                if remaining_rows < 2:
                    ids_to_be_dropped.append(id)
                else:
                    # Add matched pairs to global drop list
                    rows_to_drop.extend(group_rows_to_drop)
            
            # Drop all matched pairs globally
            if rows_to_drop:
                print(f"Total matching rows to be dropped: {len(rows_to_drop)}")
                print(f"Shape of main_df before dropping matching rows: {main_df.shape}")
                main_df = main_df.drop(rows_to_drop)
                print(f"Shape of main_df after dropping matching rows: {main_df.shape}")
                    
            # Drop groups that were marked for complete removal
            ids_to_be_dropped = list(set(ids_to_be_dropped))
            print(f"Total duplicate groups to be dropped: {len(ids_to_be_dropped)}")
            dropped_df = main_df[main_df['DUPLICATE_ID'].isin(ids_to_be_dropped)].copy()
            print(f"No of rows to be dropped:{dropped_df.shape}")
            print('No. of rows before dropping rows:{}'.format(main_df.shape))
            final_df = main_df[~main_df['DUPLICATE_ID'].isin(ids_to_be_dropped)].copy()
            print('No. of rows after dropping rows:{}'.format(final_df.shape))
            
            # Re-factorize DUPLICATE_ID to ensure consecutive numbering
            final_df['DUPLICATE_ID'] = pd.factorize(final_df['DUPLICATE_ID'])[0]+1
            
            return final_df


df  =pd.read_csv(filepath)
print('Actual dataframe shape:', df.shape)
df.rename(columns={"VENDOR_NAME":'SUPPLIER_NAME',"INVOICE_NUM":'INVOICE_NUMBER',
                           "INVOICE_DATE":'INVOICE_DATE',"LINE_AMOUNT":'INVOICE_AMOUNT',}, inplace=True)
        
df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
df['INVOICE_AMOUNT'] = df['INVOICE_AMOUNT'].astype(float)
df['INVOICE_NUMBER'] = df['INVOICE_NUMBER'].astype(str)

print("Check for null values...")
df[['SUPPLIER_NAME','INVOICE_NUMBER','INVOICE_DATE','INVOICE_AMOUNT']].isnull().sum()

print('Drop rows where amount is less than zero')
print('Shape of data before filtering rows with negative amount: ', df.shape)
# df = df[df['INVOICE_AMOUNT']<=0]
print('Shape of data after filtering rows with negative amount: ', df.shape)

print('Shape of data before grouping by: ', df.shape)
doc_df = df.groupby(['ID','INVOICE_NUMBER']).first().reset_index()
print('Shape of data after grouping by: ', doc_df.shape)
doc_df['is_current_data'] = True

output = make_prediction(input_data=doc_df.copy())

if output['output'] is not None:
    df_output = output['output'][0]
    print('Shape of output dataframe:', df_output.shape)
    print('Time taken',time.time()-start_time)
    new_df = handle_duplicate_invoice_results(df=df_output)
    print("Shape of filtered result data",new_df.shape)
    missing_rows = pd.concat([df_output, new_df, new_df]).drop_duplicates(keep=False)
    missing_rows.to_excel('missing_rows.xlsx',index=False)
    new_df.to_excel('Pearson_output.xlsx', index=False)