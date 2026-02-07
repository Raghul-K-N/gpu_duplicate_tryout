'''
Main file for data onboarding
'''
from datetime import datetime, timezone
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import os,requests,time
from ast import literal_eval
from code1 import src_load
#import init
from flask import g
from code1 import preprocess
from code1 import startvm
from code1 import rules_col_map
from code1.logger import capture_log_message, process_data_for_sending_internal_mail, update_data_time_period_for_audit_id
from datetime import datetime, timezone
import utils
import re
import uuid
from typing import Optional

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)
transaction_id_filter_df = pd.read_csv(os.path.join(base_directory,'transaction_codes_to_filter.csv'))

failed_checks_mapping_dict = {"dataTypeCheck": "Pattern Validation Failed",
                             "dateCheck": "Date Validation Failed",
                             "DateFormatCheck": "Date Format Validation Failed",
                             "nullCheck": "NULL Values Found",
                             "DocTypeCheck": "Document type Validation Failed",
                             "dueDateCheck": "Due Date Validadtion Failed",
                             "manualEntryFlag": "Manual Entry Validation Failed",
                             "ConditionalNullCheck": "Conditional Null Check Validation Failed",
                             "uniqueIdentifier": "Unique Identifier Validation Failed",
                             "creditDebitIndicator": "Invalid values present!",
                             "creditDebitBalance": "Credit-Debit Balance Failed",
                             "creditPeriodConsistency":"Credit period values have discrepancy"}

def filter_dat_strings(input_list):
    """
    Process an input list so that if it contains at least 2 strings starting with "dat" (ignoring case)
    and the list has more than one element, then only one of the "dat" strings is kept (based on priority):
    
      1. Keep any string starting with "datecheck" (if found).
      2. Else, keep any string starting with "datatypecheck" (if found).
      3. Otherwise, leave the dat strings as they are.
    
    Non-dat string values remain untouched.
    """
    dat_entries = [(i, s) for i, s in enumerate(input_list) if s.lower().startswith("dat")]

    if len(input_list) > 1 and len(dat_entries) >= 2:
        candidate = None

        for idx, s in dat_entries:
            if s.lower().startswith("datecheck"):
                candidate = s
                break

        if candidate is None:
            for idx, s in dat_entries:
                if s.lower().startswith("datatypecheck"):
                    candidate = s
                    break

        if candidate is not None:
            result = []
            candidate_used = False
            for s in input_list:
                if s.lower().startswith("dat"):
                    if not candidate_used:
                        result.append(candidate)
                        candidate_used = True
                    continue
                else:
                    result.append(s)
            return result

    return input_list


def get_failed_check_details(response):
    capture_log_message(log_message='Getting failed check details:{}'.format(response),store_in_db=False)
    columns_list=[]
    failed_checks_list=[]
    affected_rows_list=[]
    
    try:
        for each in response['data']:
            check_result = each.get('checks',[])
            if check_result!=[]:
                failed_check =dict(check_result[0])
                if 'status' in failed_check.keys() and failed_check['status']=='Fail':
                    failed_checks_list.append(failed_check['check']+str('Check'))
                    affected_rows_list.append('NA')
                    column_name = failed_check.get('data', []) and failed_check['data'][0] or 'LINE_ITEM_IDENTIFIER'
                    columns_list.append(column_name)
                else:
                    # failed_keys = [key for key in failed_check.keys() if 'fail' in key.lower()]
                    failed_keys =  [key   for each_check in check_result for key in each_check.keys() if 'fail' in key.lower()]
                    capture_log_message(log_message='Failed keys found:{}'.format(failed_keys),store_in_db=False)
                    failed_keys = filter_dat_strings(failed_keys)
                    capture_log_message(log_message='Filtered failed keys:{}'.format(failed_keys),store_in_db=False)
                    failed_check = [dict for f in failed_keys for dict in check_result if f in dict.keys()]
                    if len(failed_keys)>0:
                        for i, each_key in enumerate(failed_keys):
                            failed_checks_list.append(failed_keys[i].split('_FAIL')[0])
                            columns_list.append(each.get('trName','NA'))
                            import numpy as np
                            import ast
                            rows_count = failed_check[i].get('rows',[])
                            if rows_count!=[]:
                                all_rows = np.array(ast.literal_eval((rows_count)))
                                no_of_rows_affected = all_rows.shape[0]
                                affected_rows_list.append(no_of_rows_affected)
                            else:
                                affected_rows_list.append('NA')
        
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message='Error in getting failed health check details:{}'.format(e))
        
    if len(failed_checks_list)==0:
            failed_checks_list.append('Multiple Health Checks')
            
    if len(affected_rows_list)==0:
        affected_rows_list.append('NA')
    
    if len(columns_list)==0:
        columns_list.append('Multiple Columns')
    failed_checks_list = [failed_checks_mapping_dict.get(item, item) for item in failed_checks_list]
        
    return columns_list,failed_checks_list,affected_rows_list

def get_credit_discount_columns(input_df, created_cols):
    """
    This function checks if the CREDIT_PERIOD and DISCOUNT_PERIOD AND DISCOUNT_PERCENT column exists in the input_df.
    If CREDIT_DISCOUNT is not present, it returns the input_df unchanged.
    
    Args:
    input_df (pd.DataFrame): The input DataFrame to check for CREDIT_DISCOUNT column.
    
    Returns:
    pd.DataFrame: The modified DataFrame with CREDIT_PERIOD and DISCOUNT_PERIOD AND DISCOUNT_PERCENT.
    """
    capture_log_message(log_message='Inside get_credit_discount_columns Function')
    discount_period_1 = True if 'DISCOUNT_PERIOD' not in input_df.columns else False
    discount_percent_1 = True if 'DISCOUNT_PERCENTAGE' not in input_df.columns else False

    df = input_df.copy()
    results = np.array([src_load.payment_terms_extraction(x) for x in df['PAYMENT_TERMS_DESCRIPTION'].astype(str)])
    if discount_percent_1:
        capture_log_message(log_message='Adding DISCOUNT_PERCENTAGE column via payment_terms_extraction')
        df['DISCOUNT_PERCENTAGE_1'] = results[:, 0]
        created_cols.append('DISCOUNT_PERCENTAGE_1')
        capture_log_message(log_message='DISCOUNT_PERCENTAGE column added successfully and its values are: {}'.format(df['DISCOUNT_PERCENTAGE_1'].unique()))
    else:
        df['DISCOUNT_PERCENTAGE_1'] = df['DISCOUNT_PERCENTAGE']
    # if discount_percent_2:
    capture_log_message(log_message='Adding DISCOUNT_PERIOD column via payment_terms_extraction')
    df['DISCOUNT_PERCENTAGE_2'] = results[:, 1]
    created_cols.append('DISCOUNT_PERCENTAGE_2')
    capture_log_message(log_message='DISCOUNT_PERIOD column added successfully and its values are: {}'.format(df['DISCOUNT_PERCENTAGE_2'].unique()))
    if discount_period_1:
        capture_log_message(log_message='Adding DISCOUNT_PERCENTAGE column via payment_terms_extraction')
        df['DISCOUNT_PERIOD_1'] = results[:, 2]
        created_cols.append('DISCOUNT_PERIOD_1')
        capture_log_message(log_message='DISCOUNT_PERCENTAGE column added successfully and its values are: {}'.format(df['DISCOUNT_PERIOD_1'].unique()))
    else:
        df['DISCOUNT_PERIOD_1'] = df['DISCOUNT_PERIOD']
    # if discount_period_2:
    capture_log_message(log_message='Adding DISCOUNT_PERIOD column via payment_terms_extraction')
    df['DISCOUNT_PERIOD_2'] = results[:, 3]
    created_cols.append('DISCOUNT_PERIOD_2')
    capture_log_message(log_message='DISCOUNT_PERIOD column added successfully and its values are: {}'.format(df['DISCOUNT_PERIOD_2'].unique()))
    
    capture_log_message(log_message='Adding CREDIT_PERIOD_PTD column via payment_terms_extraction')
    df['CREDIT_PERIOD_PTD'] = results[:, 4]
    capture_log_message(log_message='CREDIT_PERIOD_PTD column added successfully and its values are: {}'.format(df['CREDIT_PERIOD_PTD'].unique()))

    return df
    
    
def convert_time_format(df_col:pd.Series):
    # Check the length, add a zero at the front if the length is not 6
    df_col = df_col.astype(str).str.replace(':', '')
    df_col = df_col.astype(str).apply(lambda x:x if len(x)>=6 else '0'*(6-len(x))+x)
    # Add colons after every two characters
    df_col = df_col.apply(lambda x:':'.join([x[i:i+2] for i in range(0,len(x),2)]))
    return df_col

def convert_year_to_yyyy(date_str):
    parts = date_str.split(' ')
    date_parts = parts[0].split('/')
    if len(date_parts)==3:
        year_part = date_parts[2]
        if len(year_part)==2:
            date_parts[2] = '20'+year_part
            
    modified_date_str = '/'.join(date_parts)+(' '+parts[1] if len(parts)>1 else "")
    modified_date_str = modified_date_str.strip()
    return modified_date_str
    

def convert_str_format(format_str):
    if format_str is None or format_str.strip() == "":
        return None
    format_str = format_str.replace('dd', '%d')
    format_str = format_str.replace('mm', '%m')
    format_str = format_str.replace('yyyy', '%Y')
    return format_str

def convert_date_format(df_col,initial_format:str|None,final_date_format:str):
    """
    Convert the date format from initial_format to final_date_format
    
    Args:
    df_col: pd.Series
        The date column to be converted
    initial_format: str
        The initial format of the date
    final_date_format: str
        The final format of the date
        
    Returns:
    pd.Series
        The date column after conversion
    """
    if pd.api.types.is_datetime64_any_dtype(df_col):
        # Already datetime → no need for initial_format
        return df_col.dt.strftime(final_date_format)
    
    if initial_format is None or initial_format.strip() == "":
        return pd.to_datetime(df_col).dt.strftime(final_date_format).astype(str)
    else:
        return pd.to_datetime(df_col,format=initial_format).dt.strftime(final_date_format).astype(str)

def transform_df_ap(input_df):
    # input_df['original'] = True
    # positive_mask = input_df['AMOUNT']>0
    # negative_mask = input_df['AMOUNT']<0
    # zero_mask = input_df['AMOUNT']==0
     
    # positive_mask_df = input_df[positive_mask].copy()
    # negative_mask_df = input_df[negative_mask].copy()
    # zero_mask_df = input_df[zero_mask].copy()
    
    # positive_mask_df['AMOUNT'] *= -1
    # negative_mask_df['AMOUNT'] *= -1
    # zero_mask_df['AMOUNT'] *= -1
    
    # positive_mask_df['original'] = False
    # negative_mask_df['original'] = False
    # zero_mask_df['original'] = False
    
    # result_df = pd.concat([input_df,positive_mask_df,negative_mask_df,zero_mask_df],ignore_index=True)
    
    # date_cols = ['INVOICE_DATE' ,'POSTED_DATE', 'DUE_DATE', 'PAYMENT_DATE' ,'ENTERED_DATE' ]
    date_cols = ['INVOICE_DATE' ,'POSTED_DATE', 'DUE_DATE', 'PAYMENT_DATE' ,'ENTERED_DATE' ,'GRN_DATE', 'PURCHASE_ORDER_DATE']
    error_col = ''
    error_dict = {}
    error_message = ""
    date_check_msg = []
    g.date_formats_dict = {}
    transform_ap_created_colms = []
    count = int(len(input_df))
    rows = list(range(count))

    expected_date_format = 'yyyy-mm-dd'

    # try:
    for date in date_cols:
        if date in list(input_df.columns):
            error_col = date
            try:
                print(date,input_df[date].dtype)
                print(input_df[date].value_counts(dropna=False).head(10))
                # g.date_formats_dict[date] = g.date_formats_dict[date].replace('-','/')
                # if g.excel_flag and pd.api.types.is_datetime64_any_dtype(input_df[date]):
                #     g.date_formats_dict[date] = "yyyy-mm-dd"
                # else:
                #     g.date_formats_dict[date] = "yyyy-mm-dd"
                    # g.date_formats_dict[date] = g.date_formats_dict[date].replace('-','/')
                g.date_formats_dict[date] = expected_date_format
                capture_log_message(log_message=f"g.date format_dict is {date} :  {g.date_formats_dict[date]}")
                input_df[date] = input_df[date].astype(str).str.replace('-','/')
                
                # Check if the date format exists in g.date_formats_dict and throw keyerror manually
                if date not in g.date_formats_dict:
                    raise KeyError(f"KeyError: Date format for column '{date}' is not provided in g.date_formats_dict")
                
                
                if date == 'POSTED_DATE':
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    if dt_format:
                        dt_format = dt_format.replace('-', '/')  # Match the actual format after str.replace
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                            initial_format=dt_format,
                                                            final_date_format="%d-%m-%Y")
                    input_df[date].replace('nan',np.nan)
                    min_date = pd.to_datetime(input_df[date],dayfirst=True).min()
                    max_date = pd.to_datetime(input_df[date],dayfirst=True).max()
                    min_date_str = min_date.strftime('%b %Y')
                    max_date_str = max_date.strftime('%b %Y')
                    update_data_time_period_for_audit_id(min_date_str+'-'+max_date_str)

                elif date== 'ENTERED_DATE':
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    if dt_format:
                        dt_format = dt_format.replace('-', '/')  # Match the actual format after str.replace
                    input_df[date] = pd.to_datetime(input_df[date], format=dt_format, errors='coerce')
                    input_df[date] = input_df[date].astype(str) + ' 00:00:00'
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                            initial_format= None,
                                                            final_date_format="%d-%m-%Y %H:%M")
                    input_df[date].replace('nan',np.nan)

                else:
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    if dt_format:
                        dt_format = dt_format.replace('-', '/')  # Match the actual format after str.replace
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                    initial_format=dt_format,
                                                    final_date_format="%d-%m-%Y")
                    input_df[date].replace('nan',np.nan)
                    
            except ValueError as ve:
                error_dict[error_col]= "Incompatible Date Format Found, upload dates in correct format"
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"Error converting column {error_col} to datetime: {str(ve)}")
                dict_msg = {'column': [error_col], 'count': count, 'rows': rows}
                date_check_msg.append(dict_msg)
                # return None,error_dict
            
            except KeyError as ke:
                error_dict[error_col]= "Date format does not exist for column"
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"{str(ke)}")
                dict_msg = {'column': [error_col], 'count': count, 'rows': rows}
                date_check_msg.append(dict_msg)
                # return None,error_dict
            
            except Exception as e:
                error_dict[error_col]= "Incompatible Date Format Found, check the data and upload dates in correct format"
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"Error converting column {error_col} to datetime: {str(e)}")
                dict_msg = {'column': [error_col], 'count': count, 'rows': rows}
                date_check_msg.append(dict_msg)
                # return None,error_dict 
        
    if error_dict:
        error_message = "Invalid date format for column(s)"
        preprocess.date_format_check(flag=False, date_check_msg=date_check_msg, error_message=error_message)
    else:
        preprocess.date_format_check(flag=True)
    
    # if 'PAYMENT_TERMS_DESCRIPTION' in list(input_df.columns):
    #     input_df = get_credit_discount_columns(input_df,transform_ap_created_colms)
    # else:
    #     capture_log_message(log_message="PAYMENT_TERMS_DESCRIPTION column not found in input_df, skipping credit discount columns extraction")

    capture_log_message(log_message=f"Columns after getting credit discount columns: {input_df.columns}")
    if ('CREDIT_PERIOD' not in input_df.columns and 'INVOICE_DATE' in input_df.columns 
        and 'DUE_DATE' in input_df.columns and {"INVOICE_DATE", "DUE_DATE"}.isdisjoint(error_dict)):
            input_df['CREDIT_PERIOD'] = ( pd.to_datetime(input_df['DUE_DATE'],dayfirst=True) -\
                pd.to_datetime(input_df['INVOICE_DATE'],dayfirst=True) ).dt.days
            transform_ap_created_colms.append("CREDIT_PERIOD")
            capture_log_message("Initiating credit period check!!")
            preprocess.credit_period_check(input_df, error_dict)
    # elif 'CREDIT_PERIOD' in input_df.columns:
    #     preprocess.credit_period_check(input_df, error_dict)
    else:
        # input_df['CREDIT_PERIOD'].fillna(0, inplace = True)
        g.credit_period_flag = False
    
    capture_log_message(f"Raw data DEBIT_CREDIT_INDICATOR value counts:{input_df['DEBIT_CREDIT_INDICATOR'].value_counts()}")
    input_df['DEBIT_CREDIT_INDICATOR'] = input_df['DEBIT_CREDIT_INDICATOR'].astype(str).str.strip().str.upper()
    
    # Map various formats to standard "H" (Credit) or "S" (Debit)
    debit_credit_mapping = {
        'H': 'H',       
        'S': 'S'
    }
    
    # Apply the mapping
    input_df['DEBIT_CREDIT_INDICATOR'] = input_df['DEBIT_CREDIT_INDICATOR'].apply(
                                                            lambda x: debit_credit_mapping.get(x, x) )

    capture_log_message(f"DEBIT_CREDIT_INDICATOR value counts after mapping:{input_df['DEBIT_CREDIT_INDICATOR'].value_counts()}")
    
    # UPDATED LOGIC: Calculate CREDIT_AMOUNT and DEBIT_AMOUNT using DEBIT_CREDIT_INDICATOR

    # For DOW, H is Debit and S is Credit
    if 'CREDIT_AMOUNT' not in list(input_df.columns):
        input_df['CREDIT_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'S', 
                                           input_df['AMOUNT'], 0)
        transform_ap_created_colms.append("CREDIT_AMOUNT")

    if 'DEBIT_AMOUNT' not in list(input_df.columns):
        input_df['DEBIT_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'H', 
                                          input_df['AMOUNT'], 0)
        transform_ap_created_colms.append("DEBIT_AMOUNT")
    
    if 'INVOICE_AMOUNT' not in list(input_df.columns):
        input_df['INVOICE_AMOUNT'] = input_df['AMOUNT']
        
    columns_to_check_int_dtype = ["CLEARING_DOC","PURCHASE_ORDER_NUMBER","ACCOUNTING_DOC"]
    for column in columns_to_check_int_dtype:
        if column not in list(input_df.columns):
            continue
        if not pd.api.types.is_integer_dtype(input_df[column]):
            # If the column is of float type, convert to integers after handling missing values
            if pd.api.types.is_float_dtype(input_df[column]):
                input_df[column] = input_df[column].fillna(0).astype(int)
            else:
                # For non-numeric types (e.g., strings), attempt to convert to numeric
                input_df[column] = pd.to_numeric(input_df[column], errors='coerce').fillna(0).astype(int)

    
    if 'MONTH_LABEL' not in list(input_df.columns):
        # Create month label like M1_2025, M2_2025 etc from POSTED_DATE
        posted_dates = pd.to_datetime(input_df['POSTED_DATE'], format='%d-%m-%Y', errors='coerce')
        input_df['MONTH_LABEL'] = 'm' + posted_dates.dt.month.astype(str) + '_' + posted_dates.dt.year.astype(str)
        capture_log_message(f"MONTH_LABEL column created from POSTED_DATE, value counts:{input_df['MONTH_LABEL'].value_counts(dropna=False)}")
        transform_ap_created_colms.append("MONTH_LABEL")
    
    if 'POSTED_BY' not in list(input_df.columns):
        input_df['POSTED_BY'] = 'USER123'
    
    # def debit_credit_indicator(row):
    #     if row['CREDIT_AMOUNT'] > 0:
    #         return 'H'
    #     elif row['DEBIT_AMOUNT'] > 0:
    #         return 'S'
    #     else: return 'S'

    # if 'DEBIT_CREDIT_INDICATOR' not in list(input_df.columns):
    #     input_df['DEBIT_CREDIT_INDICATOR'] = input_df.apply(debit_credit_indicator, axis = 1)
    #     transform_ap_created_colms.append("DEBIT_CREDIT_INDICATOR")
    
    input_df['LINE_ITEM_IDENTIFIER'] = input_df.groupby('ACCOUNTING_DOC').cumcount()+1
    input_df['LINE_ITEM_IDENTIFIER'] = input_df['ACCOUNTING_DOC'].astype(str)+'-'+input_df['LINE_ITEM_IDENTIFIER'].astype(str)
    

    if 'PAYMENT_AMOUNT' in list(input_df.columns):
        input_df['PAYMENT_AMOUNT'] = np.abs(input_df['PAYMENT_AMOUNT'])

    # input_df['INV_ACCOUNT_DOC']= input_df['ACCOUNTING_DOC']

    #Replacing * in invoice number with #
    input_df['INVOICE_NUMBER'] = input_df['INVOICE_NUMBER'].astype(str).str.replace("*","#")
    input_df['SUPPLIER_NAME'] = input_df['SUPPLIER_NAME'].astype(str).str.replace("*","") # removing * from supplier name
    
    # CHANGE 
    # adding missing columns (no mapping defined)
    if 'ACCOUNT_DESCRIPTION' not in input_df.columns:
        input_df['ACCOUNT_DESCRIPTION'] = "No description"
        transform_ap_created_colms.append("ACCOUNT_DESCRIPTION")

    if 'GL_ACCOUNT_TYPE' not in input_df.columns:
        input_df['GL_ACCOUNT_TYPE'] = "NA" 
        transform_ap_created_colms.append("GL_ACCOUNT_TYPE")

    input_df['AMOUNT'] = np.abs(input_df['AMOUNT'])
    input_df['CREDIT_AMOUNT'] = np.abs(input_df['CREDIT_AMOUNT'])
    input_df['DEBIT_AMOUNT'] = np.abs(input_df['DEBIT_AMOUNT'])
    
    # if (input_df['INVOICE_AMOUNT'] >= 0).all():
    #     capture_log_message(f"All INVOICE_AMOUNT values are non-negative")
    #     input_df['INVOICE_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'H',
    #                                           -input_df['INVOICE_AMOUNT'], input_df['INVOICE_AMOUNT'])
    # elif (input_df['INVOICE_AMOUNT'] <= 0).all():
    #     input_df['INVOICE_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'H',
    #                                           input_df['INVOICE_AMOUNT'], -input_df['INVOICE_AMOUNT'])
    # else:
    #     input_df['INVOICE_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'H',
    #                                           -np.abs(input_df['INVOICE_AMOUNT']),
    #                                           np.abs(input_df['INVOICE_AMOUNT']))
    
    # Calculate `group_count` and `unique_id` for each accounting_doc based on unique combination of values
    columns_for_uuid = src_load.get_ap_columns_to_create_uuid()
    
    input_df['group_count'] = input_df.groupby(columns_for_uuid).ngroup()+1
    input_df['unique_id'] = input_df.groupby('ACCOUNTING_DOC')['group_count'].rank(method='dense').astype(int)
    
    # Generate a consistent UUID for each unique `ACCOUNTING_DOC` + `unique_id` combination
    input_df['uuid_suffix'] = input_df.groupby(['ACCOUNTING_DOC', 'unique_id'])['group_count'].transform(lambda _: str(uuid.uuid4())[:8])

    # Step 3: Combine everything into the final unique `ACCOUNTING_DOC`
    input_df['ACCOUNTING_DOC'] = input_df['ACCOUNTING_DOC'].astype(str) + '-' + input_df['uuid_suffix'].astype(str)

    # Drop the specified columns from the DataFrame
    input_df = input_df.drop(columns=['group_count', 'unique_id', 'uuid_suffix'])    
    
    transform_ap_created_colms  = [(tr_col,'') for tr_col in transform_ap_created_colms] 
    preprocess.pass_mapping_cols(transform_ap_created_colms)
    capture_log_message(f"Created Columns list:{transform_ap_created_colms} added to mapping")

    return input_df, None
    

# def transform_df_gl(input_df):

    input_df.columns = input_df.columns.str.replace('ï»¿', '')
    date_cols = ['ENTRY_TIME','ENTERED_DATE' ,'POSTED_DATE', 'DUE_DATE', 'PAYMENT_DATE', 'INVOICE_DATE' ]
    error_col = ''
    error_dict = {}
    capture_log_message(f"Inside transform df gl : {input_df.columns}")
    try:
        for date in date_cols:
            if date in list(input_df.columns):
                error_col = date
                capture_log_message(f"Processing Date col:{date}")
                capture_log_message(f"date dtype: {input_df[date].dtype}")
                if date=='ENTRY_TIME':
                    input_df['ENTRY_TIME'] = input_df['ENTRY_TIME'].astype(str).apply(lambda x: x.split('.')[0])
                    input_df['ENTRY_TIME'] = convert_time_format(input_df['ENTRY_TIME'])
                    input_df['ENTRY_TIME'] = pd.to_datetime(input_df['ENTRY_TIME'],
                                                            format= "%H:%M:%S").dt.time
                    continue

                if g.excel_flag and pd.api.types.is_datetime64_any_dtype(input_df[date]):
                    capture_log_message("Excel datetime format")
                    g.date_formats_dict[date] = "yyyy/mm/dd"
                else:
                    g.date_formats_dict[date] = g.date_formats_dict[date].replace('-','/')

                input_df[date] = input_df[date].astype(str).str.replace('-','/')
               
                if date not in g.date_formats_dict:
                    raise KeyError(f"KeyError: Date format for column '{date}' is not provided in g.date_formats_dict")

                if date == 'POSTED_DATE':
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                            initial_format=dt_format,
                                                            final_date_format="%d-%m-%Y")
                    input_df[date].replace('nan',np.nan)
                    min_date = pd.to_datetime(input_df[date],dayfirst=True).min()
                    max_date = pd.to_datetime(input_df[date],dayfirst=True).max()
                    min_date_str = min_date.strftime('%b %Y')
                    max_date_str = max_date.strftime('%b %Y')
                    update_data_time_period_for_audit_id(min_date_str+'-'+max_date_str)

                # elif date=='ENTRY_TIME':
                #     capture_log_message(f"Inside entry time format: {input_df['ENTRY_TIME'].head()}")
                #     input_df['ENTRY_TIME'] = input_df['ENTRY_TIME'].apply(lambda x:x.split('.')[0])
                #     input_df['ENTRY_TIME'] = convert_time_format(input_df['ENTRY_TIME'])
                #     capture_log_message(f"After entry time format:{input_df['ENTRY_TIME'].head()}")
                #     input_df['ENTRY_TIME'] = pd.to_datetime(input_df['ENTRY_TIME'],
                #                                             format= "%H:%M:%S").dt.time
                    
                elif date== 'ENTERED_DATE':
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    input_df[date] = input_df[date].astype(str)+' '+input_df['ENTRY_TIME'].astype(str)
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                            initial_format= dt_format+" %H:%M:%S",
                                                            final_date_format="%d-%m-%Y %H:%M")
                    input_df[date].replace('nan',np.nan)

                else:
                    dt_format = convert_str_format(g.date_formats_dict[date])
                    input_df[date] = convert_date_format(df_col=input_df[date],
                                                    initial_format=dt_format,
                                                    final_date_format="%d-%m-%Y")
                    input_df[date].replace('nan',np.nan)
                    
    except ValueError as ve:
        error_dict[error_col]= "Incompatible Date Format Found, upload dates in correct format"
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error converting column {error_col} to datetime: {str(ve)}")
        return None,error_dict
    
    except KeyError as ke:
        error_dict[error_col]= "Date format does not exist for column"
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"{str(ke)}")
        return None,error_dict
    
    except Exception as e:
        error_dict[error_col]= "Incompatible Date Format Found, check the data and upload dates in correct format"
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error converting column {error_col} to datetime: {str(e)}")
        return None,error_dict 
    

    capture_log_message(f"Raw data DEBIT_CREDIT_INDICATOR value counts:{input_df['DEBIT_CREDIT_INDICATOR'].value_counts()}")
    input_df['DEBIT_CREDIT_INDICATOR'] = input_df['DEBIT_CREDIT_INDICATOR'].astype(str).str.strip().str.upper()
    
    # Map various formats to standard "H" (Credit) or "S" (Debit)
    debit_credit_mapping = {
        'C': 'H',  
        'CREDIT': 'H',
        'H': 'H',       
        'D': 'S',   
        'DEBIT': 'S',
        'S': 'S'
    }
    
    # Apply the mapping
    input_df['DEBIT_CREDIT_INDICATOR'] = input_df['DEBIT_CREDIT_INDICATOR'].apply(
                                                            lambda x: debit_credit_mapping.get(x, x) )
    
    capture_log_message(f"DEBIT_CREDIT_INDICATOR value counts after mapping:{input_df['DEBIT_CREDIT_INDICATOR'].value_counts()}")
    
    # UPDATED LOGIC: Calculate CREDIT_AMOUNT and DEBIT_AMOUNT using DEBIT_CREDIT_INDICATOR
    if 'CREDIT_AMOUNT' not in list(input_df.columns):
        input_df['CREDIT_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'H', 
                                           input_df['AMOUNT'], 0)
    

    if 'DEBIT_AMOUNT' not in list(input_df.columns):
        input_df['DEBIT_AMOUNT'] = np.where(input_df['DEBIT_CREDIT_INDICATOR'] == 'S', 
                                          input_df['AMOUNT'], 0)
    # # Keep only the rows where at least one of the amounts is non-zero
    # input_df = input_df[(input_df['DEBIT_AMOUNT'] > 0) | (input_df['CREDIT_AMOUNT'] > 0)]

    columns_to_check_int_dtype = ["CLEARING_DOC","PURCHASE_ORDER_NUMBER","ACCOUNTING_DOC"]
    for column in columns_to_check_int_dtype:
        if column not in list(input_df.columns):
            continue
        if not pd.api.types.is_integer_dtype(input_df[column]):
            # If the column is of float type, convert to integers after handling missing values
            if pd.api.types.is_float_dtype(input_df[column]):
                input_df[column] = input_df[column].fillna(0).astype(int)
            else:
                # For non-numeric types (e.g., strings), attempt to convert to numeric
                input_df[column] = pd.to_numeric(input_df[column]).astype(int)

    # def debit_credit_indicator(row):
    #     if row['CREDIT_AMOUNT'] > 0:
    #         return 'H'
    #     elif row['DEBIT_AMOUNT'] > 0:
    #         return 'S'
    #     else:
    #         return 'S'
    
    # if 'DEBIT_CREDIT_INDICATOR' not in list(input_df.columns):
    #     input_df['DEBIT_CREDIT_INDICATOR'] = input_df.apply(debit_credit_indicator, axis = 1)
    
    if 'REVERSE_DOCUMENT_NUMBER' in list(input_df.columns):
        input_df['LEDGER'] = input_df['REVERSE_DOCUMENT_NUMBER']
        input_df['LEDGER'] = input_df['LEDGER'].fillna("")
        input_df['IS_REVERSED'] = np.where(input_df['LEDGER']=="",0,1)
        input_df.drop(['REVERSE_DOCUMENT_NUMBER'],axis=1,inplace=True)
    
    
    # Adding POSTED_BY values manually
    if 'POSTED_BY' not in list(input_df.columns):
        input_df['POSTED_BY'] = "TestUser"

    input_df['LINE_ITEM_IDENTIFIER'] = input_df.groupby('ACCOUNTING_DOC').cumcount()+1
    input_df['LINE_ITEM_IDENTIFIER'] = input_df['ACCOUNTING_DOC'].astype(str)+'-'\
                                                +input_df['LINE_ITEM_IDENTIFIER'].astype(str)
    
    if 'AMOUNT' in list(input_df.columns):
        input_df['AMOUNT'] = np.abs(input_df['AMOUNT'])
        
    if 'INVOICE_AMOUNT' in list(input_df.columns):
        input_df['INVOICE_AMOUNT'] = np.abs(input_df['INVOICE_AMOUNT'])

    input_df['CREDIT_AMOUNT'] = np.abs(input_df['CREDIT_AMOUNT'])
    
    input_df['DEBIT_AMOUNT'] = np.abs(input_df['DEBIT_AMOUNT'])

    if 'ACCOUNT_DESCRIPTION' not in input_df.columns:
        input_df['ACCOUNT_DESCRIPTION'] = "No description"
    
    # Calculate `group_count` and `unique_id` for each accounting_doc based on unique combination of values
    columns_for_uuid = src_load.get_gl_columns_to_create_uuid()

    input_df['group_count'] = input_df.groupby(columns_for_uuid).ngroup()+1
    input_df['unique_id'] = input_df.groupby('ACCOUNTING_DOC')['group_count'].rank(method='dense').astype(int)
    
    # Generate a consistent UUID for each unique `ACCOUNTING_DOC` + `unique_id` combination
    input_df['uuid_suffix'] = input_df.groupby(['ACCOUNTING_DOC', 'unique_id'])['group_count'].transform(lambda _: str(uuid.uuid4())[:8])

    # Step 3: Combine everything into the final unique `ACCOUNTING_DOC`
    input_df['ACCOUNTING_DOC'] = input_df['ACCOUNTING_DOC'].astype(str) + '-' + input_df['uuid_suffix'].astype(str)

    # Drop the specified columns from the DataFrame
    input_df = input_df.drop(columns=['group_count', 'unique_id', 'uuid_suffix'])  
    return input_df, None
    


    
def do_preprocess_ap(ap_input_df: pd.DataFrame, src_id: Optional[int], audit_id: int, client_id: int):
    """
    Preprocess AP transaction data without requiring mapping files
    """
    # Initialize
    g.temp_table_names = []
    function_start_time = datetime.now(timezone.utc)
    capture_log_message(log_message=f"Inside do_preprocess_ap_ {audit_id}")

    capture_log_message(f"AP Input DataFrame columns: {ap_input_df.columns.tolist()}", store_in_db=False)


    # Simple one-line rename
    ap_input_df = ap_input_df.rename(columns={
    'DEBIT_CREDIT_INDICATOR': 'DEBIT_CREDIT_INDICATOR_LINE_ITEM',
    'DEBIT_CREDIT_INDICATOR_HEADER_LEVEL': 'DEBIT_CREDIT_INDICATOR' 
    })

    capture_log_message(f"After rename, new header level value counts: {ap_input_df['DEBIT_CREDIT_INDICATOR'].value_counts(dropna=False)}", store_in_db=False)
    capture_log_message(f"After rename, new line item level value counts: {ap_input_df['DEBIT_CREDIT_INDICATOR_LINE_ITEM'].value_counts(dropna=False)}", store_in_db=False)

    # Create new columns, dont rename , create a new copy
    for key, value in utils.AP_RAW_DATA_RENAME_MAPPING.items():
        if value not in ap_input_df.columns and key in ap_input_df.columns:
            ap_input_df[value] = ap_input_df[key]
    
    # ap_input_df.rename(columns=utils.AP_RAW_DATA_RENAME_MAPPING, inplace=True)
    capture_log_message(f"AP DataFrame columns after renaming: {ap_input_df.columns.tolist()}", store_in_db=False)
    # Define mandatory columns
    if g.module_nm =='AP':
        MANDATORY_COLUMNS_AP = [
            'ACCOUNTING_DOC', 'TRANSACTION_DESCRIPTION', 'DOC_TYPE',
            'AMOUNT', 'ENTERED_BY', 'POSTED_BY', 'PAYMENT_DATE', 
            'COMPANY_CODE', 'INVOICE_DATE', 'INVOICE_AMOUNT', 'ENTERED_DATE',
            'DUE_DATE', 'SUPPLIER_ID', 'SUPPLIER_NAME', 'INVOICE_NUMBER',
            'PAYMENT_TERMS', 'POSTED_DATE', 'DEBIT_CREDIT_INDICATOR','CLIENT','REGION','FISCAL_YEAR'
            #'TRANSACTION_ID','ACCOUNT_DOC_ID'
        ]
        MANDATORY_COLUMNS = MANDATORY_COLUMNS_AP
    else:
        MANDATORY_COLUMNS_ZBLOCK = [
            'ACCOUNTING_DOC', 'TRANSACTION_DESCRIPTION', 'DOC_TYPE',
            'AMOUNT', 'ENTERED_BY', 'POSTED_BY', 'PAYMENT_DATE', 
            'COMPANY_CODE', 'INVOICE_DATE', 'INVOICE_AMOUNT', 'ENTERED_DATE',
            'DUE_DATE', 'SUPPLIER_ID', 'SUPPLIER_NAME', 'INVOICE_NUMBER',
            'PAYMENT_TERMS', 'POSTED_DATE', 'DEBIT_CREDIT_INDICATOR','CLIENT','REGION','FISCAL_YEAR'
        ]
        MANDATORY_COLUMNS = MANDATORY_COLUMNS_ZBLOCK

    preprocess.load_config(ap_input_df, src_id, audit_id, client_id)

    

    #tranformations done on the input_df
    ap_input_df, error_dict = transform_df_ap(ap_input_df)

    if error_dict:
        process_details = "Data Health Check Failed"
        error_col = next(iter(error_dict))
        description_value = error_dict[error_col]
        # error_col ,description_value=error_dict.popitem()
        key_resp=[]

        src_load.update_health_status(process_details,src_id, audit_id)
        capture_log_message(log_message='Updated the health status in src_status table for health status Fail Scenario')

        resp_obj = {"trName": error_col, "srcName": "", 
                    "checks": [{"DATE_FORMAT_FAIL":"Invalid date format for column: {}".format(error_col)}] }
        capture_log_message(current_logger=g.error_logger,
                            log_message=f'Failed date format for column:{error_col}',
                            error_name=utils.ISSUE_WITH_DATE_FORMAT) 
        
        key_resp.append(resp_obj)
        formatted_key_resp = {"Result":"FailedPreCheck","data":key_resp}
        columns_list,failed_checks_list,affected_rows_list=get_failed_check_details(formatted_key_resp)

        capture_log_message(current_logger=g.error_logger,
                            log_message='Failed checks:{} for columns:{} and affected rows:{}'.format(failed_checks_list,columns_list,affected_rows_list),
                            error_name=utils.ISSUE_WITH_DATE_FORMAT
                            ) 
        
        # process_data_for_sending_internal_mail(subject='Health Check Status',stage=utils.DATA_HEALTH_CHECK_STAGE,
        #                                     is_success=False,date_list=[function_start_time],failed_column_list=columns_list,
        #                                     failed_count_list=affected_rows_list,failed_reason_list=failed_checks_list,
        #                                     time_taken_list=[datetime.now(timezone.utc)-function_start_time],
        #                                     description_list=[description_value],historical_flow=True)
        
        err_resp = [{"INVALID DATE FORMAT COLUMN": error_col}]
        formatted_resp = {"Result":"FailedPreCheck","data":err_resp}
        return formatted_resp
    

    # Check mandatory columns
    missing_columns = [col for col in MANDATORY_COLUMNS if col not in ap_input_df.columns]
    if missing_columns:
        error_msg = f"Mandatory columns missing: {missing_columns}"
        capture_log_message(log_message=error_msg, current_logger=g.error_logger)
        
        resp_obj = {"trName": missing_columns[0], "srcName": "", 
                   "checks": [{"MANDATORY_COLUMN_FAIL": error_msg}]}
        
        formatted_resp = {
            "Result": "FailedPreCheck",
            "data": [resp_obj]
        }
        return formatted_resp


 
    gltimestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')    
    glinit_table_name = 'dummy_table' +'_' + gltimestamp+'_'+str(audit_id)
    capture_log_message(log_message=f"new temp table name {glinit_table_name}")
    g.temp_table_names.append(glinit_table_name)
    gl_csv_path = "navistar_client_data"
    filenm= os.path.basename(gl_csv_path)
    filename = filenm.replace('.csv','')
    filename = filename.replace('-','_')
    filename = "ap_"+filename
    if len(filename)>29:
        filename = filename[:29] 
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    init_table_name = 'temp_' + filename + '_' + timestamp +'_'+str(audit_id)
    capture_log_message(log_message=f'new temp table name {init_table_name}')
    g.temp_table_names.append(init_table_name)
    # Writing the data to new temp table
    engine = create_engine("mysql+pymysql://"+src_load.DB_USERNAME+":"+src_load.DB_PASSWORD +
                        "@"+src_load.DB_HOST+":"+src_load.DB_PORT+"/"+src_load.DB_NAME, connect_args = src_load.connect_args)
    
    ap_input_df.columns = ap_input_df.columns.str.strip()
    capture_log_message(log_message=f"columns from file {ap_input_df.columns}",store_in_db=False) 
    try:
        ap_input_df.insert(loc=0,column='ROW_NUM',value = np.arange(len(ap_input_df)))
        capture_log_message(log_message=f"Inserted ROW_NUM column for {len(ap_input_df)} rows")
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error while inserting row number column {e}",
                            error_name=utils.OTHER_ERRORS)
        
        return {"Result":"FailedPreCheck","data":[{'message':f"Error while inserting row number column {e}"}]}
    try:
        with engine.connect().execution_options(stream_results=True) as connection:
            ap_input_df['errorlist']=None
            ap_input_df['ERRORFLAG']=0
            ap_input_df.to_sql(name=init_table_name, con=connection,
                        if_exists='replace', index=False,chunksize=50000)
            pd.DataFrame({'col1':[1]}).to_sql(name = glinit_table_name,con=engine.connect().execution_options(stream_results=True),
                        if_exists='replace',index=False,chunksize=1000)
        capture_log_message(log_message=f"Wrote the data to new temp table {init_table_name}")
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error while writing to temp table {e}",
                            error_name=utils.DB_CONNECTION_ISSUE)
        
        return {"Result":"FailedPreCheck","data":[{'message':f"Error while writing to temp table{e}"}]}

    
    load_data_into_temp_time = datetime.now(timezone.utc)
    capture_log_message(log_message=' time taken to load data into temp table:{}'.format(load_data_into_temp_time-function_start_time))
    
    
    capture_log_message(log_message="Adding file names to src status table")
    #save the temp filename to SRC_STATUS
    filenames = ','.join([glinit_table_name,init_table_name])
    src_load.save_temp_filename(src_id,filenames, audit_id)
    capture_log_message(log_message="Added file names to src status table")
    

    # Read the data from the db
    capture_log_message(log_message="Reading the data from the db - invoices")
    with engine.connect().execution_options(stream_results=True) as connection:
        ap_data = pd.read_sql(f'SELECT * FROM {init_table_name}', con=connection)
    capture_log_message(log_message='Read the data from the db')
    
    capture_log_message(log_message=' Shape of data from {0} table :{1}'.format(init_table_name,ap_data.shape))
    #Rename data columns 
    # ap_data = preprocess.ap_rename(ap_data)
    # capture_log_message(log_message='Renamed the data columns')
    

    ap_data['ACCOUNTING_DOC'] = ap_data['ACCOUNTING_DOC'].apply(str)
    capture_log_message(log_message='Converted the account doc columns to string')

    try:
        # ap_gl_data = pd.merge(gl_data,ap_data,right_on='ACCOUNTING_DOC',left_on='INV_ACCOUNT_DOC',how='inner',suffixes=('', '_y'))
        # ap_gl_data['INVOICE_DATE'] = pd.to_datetime(ap_gl_data['INVOICE_DATE'],format = '%m/%d/%Y').dt.strftime("%d-%m-%Y")
        # ap_gl_data['DUE_DATE'] = pd.to_datetime(ap_gl_data['DUE_DATE'],format = '%m/%d/%Y').dt.strftime("%d-%m-%Y")
        # ap_gl_data['ENTERED_DATE'] = pd.to_datetime(ap_gl_data['ENTERED_DATE'], format = "%d-%m-%Y %H:%M:%S").dt.strftime("%d-%m-%Y %H:%M")
        #ap_gl_data = pd.merge(gl_data.assign(x=gl_data['ACCOUNTING_DOC'].astype(str)),ap_data.assign(x=ap_data['INV_ACCOUNT_DOC'].astype(str)),how='inner',on='x',suffixes=('', '_y'))
        ap_gl_data = ap_data
        if ap_gl_data.shape[0]==0:
            capture_log_message(current_logger=g.error_logger,
                                log_message='No common rows found after merging the data')
            return {"Result":"FailedPreCheck","data":"No common rows found after merging the data"}
    except KeyError as e:
        capture_log_message(current_logger=g.error_logger,log_message=f"KeyError: {e}")
        Key = e.args[0]
        Key = re.findall(r"'([A-Za-z0-9_]+)'", Key)  
        Key = Key[0] if len(Key)>0 else 'Mandatory Column'
        key_resp = [{"trName": Key,"srcName": "",
                     "checks": [{"Column_missing_FAIL":"{} column is missing".format(Key)}]}]
        formatted_key_resp = {"Result":"FailedPreCheck","data":key_resp}
        columns_list,failed_checks_list,affected_rows_list=get_failed_check_details(formatted_key_resp)
        capture_log_message(current_logger=g.error_logger,
                            log_message='Failed checks:{} for columns:{} and affected rows:{}'.format(failed_checks_list,columns_list,affected_rows_list),
                            error_name=utils.MISSING_MANDATORY_COLUMNS) 
        
        
        # process_data_for_sending_internal_mail(subject='Health Check Status',stage=utils.DATA_HEALTH_CHECK_STAGE,
        #                                        is_success=False,date_list=[function_start_time],failed_column_list=columns_list,
        #                                        failed_count_list=affected_rows_list,failed_reason_list=failed_checks_list,
        #                                        time_taken_list=[datetime.now(timezone.utc)-function_start_time],)
        
        return formatted_key_resp
    capture_log_message(log_message=f' done merging two files with shape of {ap_gl_data.shape}')
    
    #data type check
    try:
        ap_gl_data['ROW_NUM_y'] = ap_gl_data['ROW_NUM']

        # mandatory_pattern_cols = ['ACCOUNTING_DOC','DOC_TYPE','AMOUNT','POSTED_BY','ACCOUNT_DESCRIPTION',                   
        # 'COMPANY_CODE','INVOICE_DATE','INVOICE_NUMBER','INVOICE_AMOUNT','DUE_DATE','GL_ACCOUNT_TYPE',
        # 'SUPPLIER_ID','SUPPLIER_NAME','PAYMENT_TERMS','POSTED_DATE','CREDIT_PERIOD',
        # 'DEBIT_CREDIT_INDICATOR','ENTERED_DATE','ROW_NUM','ROW_NUM_y',"LINE_ITEM_IDENTIFIER",
        # "INVOICE_CURRENCY","PAYMENT_METHOD",'VAT_ID',"LEGAL_ENTITY_NAME_AND_ADDRESS","DISCOUNT_PERCENTAGE_1"
        # "DISCOUNT_PERCENTAGE_2", "DISCOUNT_TAKEN","DISCOUNT_PERIOD_1","PURCHASE_ORDER_DATE"
        # ,"DISCOUNT_PERIOD_2","PURCHASE_ORDER_NUMBER","GRN_NUMBER","GRN_DATE"]

        # pattern_cols = list(set(mandatory_pattern_cols)  & set(ap_gl_data.columns))
        # apdf =ap_gl_data[pattern_cols]
        
        # finalise the list of mandatory pattern columns based on the columns present in the dataframe

        apdf = ap_gl_data.copy()

        # CHANGE
        # ,'DOC_TYPE_DESCRIPTION' 'PAYMENT_AMOUNT','INVOICE_STATUS',
        
        # already commented
        #LINE_ITEM_IDENTIFIER ,'DISCOUNT_PERCENTAGE'
        
        # if "original" in ap_gl_data.columns:
        #     capture_log_message(log_message='Original column added to dataframe')
        #     apdf['original'] = ap_gl_data['original']
        
        capture_log_message(log_message='Data type check columns filtered')
    except KeyError as e:
        Key = e.args[0]
        Key = re.findall(r"'([A-Za-z0-9_]+)'", Key)  
        Key = Key[0] if len(Key)>0 else 'Mandatory Column'
        key_resp = [{"trName": Key,"srcName": "",
                     "checks": [{"Column_missing_FAIL":"{} column is missing".format(Key)}]}]
        capture_log_message(current_logger=g.error_logger,log_message=f"KeyError on data type column filter: {e}")
        formatted_key_resp = {"Result":"FailedPreCheck","data":key_resp}
        columns_list,failed_checks_list,affected_rows_list=get_failed_check_details(formatted_key_resp)
        capture_log_message(current_logger=g.error_logger,
                            log_message='Failed checks:{} for columns:{} and affected rows:{}'.format(failed_checks_list,columns_list,affected_rows_list),
                            error_name=utils.MISSING_MANDATORY_COLUMNS) 
        
        # process_data_for_sending_internal_mail(subject='Health Check Status',stage=utils.DATA_HEALTH_CHECK_STAGE,
        #                                        is_success=False,date_list=[function_start_time],failed_column_list=columns_list,
        #                                        failed_count_list=affected_rows_list,failed_reason_list=failed_checks_list,
        #                                        time_taken_list=[datetime.now(timezone.utc)-function_start_time],historical_flow=True)
        return formatted_key_resp


    
    
    
    #data_type_output       
    # schema_out = preprocess.ap_schema(apdf)
    capture_log_message(log_message='Pattern Matching Started')
    pattern_match_start_time = datetime.now(timezone.utc)
    pattern = preprocess.ap_pattern_match(apdf)
    pattern_match_end_time = datetime.now(timezone.utc)
    capture_log_message(log_message=f"{pattern}")
    capture_log_message(log_message='Pattern Matching Completed')
    capture_log_message(log_message=' Time taken for pattern match:{}'.format(pattern_match_end_time-pattern_match_start_time))
    PATTERN_FLAG = bool(len(pattern[0]['Pattern_Validation']))
    capture_log_message(log_message='Preprocessing Function Initiated')
    data_check_start_time = datetime.now(timezone.utc)
    preprocessed_df,FINAL_FLAG = preprocess.get_db_data(ap_gl_data,preprocess.MODE_KEY)
    capture_log_message(log_message='Preprocessing Function Completed')
    data_check_end_time = datetime.now(timezone.utc)
    time_taken_for_health_check = data_check_end_time-function_start_time
    capture_log_message(log_message=' Time taken for data checks:{}'.format(time_taken_for_health_check))
    capture_log_message(log_message=' Shape of data after completing data checks:{}'.format(preprocessed_df.shape))
    
    
    # CHANGE
    # if 'original' in preprocessed_df.columns:
    #     original_data_mask = preprocessed_df['original']==True
    #     preprocessed_df = preprocessed_df[original_data_mask].copy()
    #     capture_log_message(log_message='Shape of data after filtering original data:{}'.format(preprocessed_df.shape))
    # else:
    #     capture_log_message(log_message='Original column not found in the dataframe',store_in_db=False)
    
    ErrorObject_Output = preprocess.errorOutObj.finalOutput()
    capture_log_message(log_message='Started Generating Column wise Error')
    GenerateColError_Output = preprocess.errorOutObj.generateColumnErrors()
    capture_log_message(log_message=f'Completed Generating Column wise Error')
    capture_log_message(log_message=f'Filtering the GenerateColError_Output based on condition {GenerateColError_Output}',store_in_db=False)
    result = [[item['srcName'], item['trName'], check_key,literal_eval(check['rows'])] for item in GenerateColError_Output for check in item['checks'] for check_key in check.keys() if '_FAIL' in check_key and len(check) > 0 and 'rows' in check and len(check['rows']) > 2]
    capture_log_message(log_message='Completed Filtering the GenerateColError_Output based on condition: {}'.format(result),store_in_db=False)
    # for i,tup in enumerate(result):
    #     if 'creditDebitBalance_FAIL' in tup:
    #         for item in tup[3]:
    #             indices = ap_gl_data[ap_gl_data['ACCOUNTING_DOC']==item].index.to_list()
    #             tup[3] = indices
    #         result[i]=tup


    # rowslist = [tup[3] for tup in result ]
    # uniq_rows = set(element for array in rowslist for element in array)
    # uniq_rows_str = ','.join(str(e) for e in uniq_rows)

    # src_load.errorrows_to_src(src_id,uniq_rows_str)

    gl_temp,ap_temp = src_load.get_temp_tablename_AP(src_id, audit_id)
    # capture_log_message(log_message='Fetching the temp table names')
    # src_load.create_errorlist_col_ap(gl_temp)
    # capture_log_message(log_message='Created the errorlist table for gl')
    # src_load.create_errorlist_col_ap(ap_temp)
    # capture_log_message(log_message='Created the errorlist table for ap')
    with engine.connect() as connection:
        gl_column = pd.read_sql(f"show COLUMNS from {gl_temp};",con=connection)
        capture_log_message(log_message='Fetching the gl temp table columns')
        ap_column = pd.read_sql(f"show COLUMNS from {ap_temp};",con=connection)
        capture_log_message(log_message='Fetching the ap temp table columns')
    
    engine.dispose()
    glcols =gl_column['Field'].to_list()
    apcols =ap_column['Field'].to_list()
    capture_log_message(log_message='Converting the gl and ap temp table columns to list')
    for i,res in enumerate(result):
        row_replace_list = []
        if res[0] in glcols:
            for row in res[3]:
                row_replace_list.append(row[0])
            res[3] = row_replace_list
        elif res[0] in apcols:
            for row in res[3]:
                row_replace_list.append(row[1])
            res[3] = row_replace_list
    capture_log_message(log_message='Completed the row replacement')
    for items in result:
        if items[0] in glcols:
            column_values = ','.join([f"'{val}'" for val in items[:3]])
            row_nums = list(set(items[3]))
            rownums = tuple(row_nums)
            src_load.errorlist_to_temp_ap(gl_temp,column_values,rownums)
            #for row_num in row_nums:
                #src_load.errorlist_to_temp_ap(gl_temp,column_values,row_num)
        elif items[0] in apcols:
            column_values = ','.join([f"'{val}'" for val in items[:3]])
            row_nums = list(set(items[3]))
            if len(row_nums) == 1:
                rownums = '({})'.format(row_nums[0])
            else:
                rownums = tuple(row_nums)
            src_load.errorlist_to_temp_ap(ap_temp,column_values,rownums)
            #for row_num in row_nums:
                #src_load.errorlist_to_temp_ap(ap_temp,column_values,row_num)
             #   pass
    capture_log_message(log_message='Completed the errorlist updation')
    # ap_rules =  rules_col_map.rules(preprocessed_df,preprocess.MODE_KEY)
    # ap_rules = ','.join(ap_rules)
    ap_rules = "LATE_PAYMENT,IMMEDIATE PAYMENTS,POSTING PERIOD"
    capture_log_message(log_message='AP health check: {}'.format(GenerateColError_Output))
    # Check overall health status
    HEALTH_STATUS = FINAL_FLAG and not PATTERN_FLAG
    capture_log_message(log_message='Health Status : {}'.format(HEALTH_STATUS))
    if HEALTH_STATUS:
        capture_log_message(current_logger=g.stage_logger,log_message='Data Health Check and Preprocessing Passed',
                            start_time=function_start_time,end_time=data_check_end_time,
                            time_taken=time_taken_for_health_check,
                        data_shape=preprocessed_df.shape)
        final_data = preprocessed_df
        capture_log_message(log_message='Started Loading the data to src_ap_data table')
        load_data_start = datetime.now(timezone.utc)
        final_df = src_load.get_src_ap_data(final_data,ap_rules,src_id, audit_id)
        load_data_end = datetime.now(timezone.utc)
        capture_log_message(log_message=' Time taken to load data into src_ap_data table:{}'.format(load_data_end-load_data_start))
        capture_log_message(log_message='Completed Loading the data to src_ap_data table')
        process_details = "Data Health Check Passed" 
        src_load.update_health_status(process_details,src_id,audit_id)
        capture_log_message(log_message='Updated the health status in src_status table')
        resp_msg = {"Result":"Success",
                    "data":GenerateColError_Output,
                    "dataframe":final_df}
        response = resp_msg
        # process_data_for_sending_internal_mail(subject='Health Check Status',stage=utils.DATA_HEALTH_CHECK_STAGE,
        #                                        is_success=True,date_list=[data_check_start_time],volume_list=[preprocessed_df.shape],
        #                                        time_taken_list=[time_taken_for_health_check],
        #                                        description_list=['Data Health Check Completed'],historical_flow=True)
        g.hist_health_check_status = True
    else:
        process_details = "Data Health Check Failed"
        src_load.update_health_status(process_details,src_id, audit_id)
        capture_log_message(log_message='Updated the health status in src_status table for health status Fail Scenario')
        resp_msg = {"Result":"Failed",
                    "data":GenerateColError_Output,
                    "dataframe":pd.DataFrame()}
        response = resp_msg
        columns_list,failed_checks_list,affected_rows_list=get_failed_check_details(response)
        capture_log_message(current_logger=g.error_logger,
                            log_message='Failed checks:{} for columns:{} and affected rows:{}'.format(failed_checks_list,columns_list,affected_rows_list),
                            start_time=data_check_start_time,end_time=data_check_end_time,time_taken=time_taken_for_health_check)   
        
        capture_log_message(current_logger=g.error_logger,
                            log_message='Data Preprocessing & Health Check Failed',
                            start_time=data_check_start_time,end_time=data_check_end_time,
                            time_taken=time_taken_for_health_check,
                            data_shape=preprocessed_df.shape)
        
        # process_data_for_sending_internal_mail(subject='Health Check Status',stage=utils.DATA_HEALTH_CHECK_STAGE,
        #                                        is_success=False,date_list=[data_check_start_time],
        #                                        failed_column_list=columns_list,
        #                                        failed_count_list=affected_rows_list,
        #                                        failed_reason_list=failed_checks_list,
        #                                        total_count_list=[preprocessed_df.shape],
        #                                        time_taken_list=[time_taken_for_health_check],historical_flow=True)
        
        
    return response

def start_mlvm(src_id,TYPE, audit_id):
    engine = create_engine("mysql+pymysql://"+src_load.DB_USERNAME+":"+src_load.DB_PASSWORD +
                        "@"+src_load.DB_HOST+":"+src_load.DB_PORT+"/"+src_load.DB_NAME, connect_args = src_load.connect_args) 
    # curr_src = pd.read_sql("""select id, audit_id from SRC_STATUS ORDER BY id desc;""",con=engine)
    # curr_audit_id = int(curr_src['audit_id'][0])
    exec_table = pd.read_sql(f"""select STATUS from SRC_STATUS where RUN_JOB = 1 and id ={src_id} and audit_id = {audit_id};""",con=engine)     
    if exec_table.shape[0]!=0:
        exe_id = str(exec_table['STATUS'][0])       
        if exe_id == '1' or exe_id == '2':
            if TYPE == "CLOUD":       
                startvm.post_turnON()
                msg = "ML VM Starting now"
            elif TYPE == "LAPTOP":
                time.sleep(100)
                response = requests.get("http://mlvm:5001/trigger")
                msg = "ML VM Starting now"
        else:
            msg = "No process found for Ingestion or Scoring"
    else:
        msg = "No process found for Ingestion or Scoring"
    return msg
    
