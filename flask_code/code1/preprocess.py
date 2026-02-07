'''
Python Script for the preprocessing checks
'''
import configparser
from pandas_schema import Column, Schema
from pandas_schema.validation import IsDtypeValidation,MatchesPatternValidation
from datetime import datetime, timezone
from flask import g
import os
import pandas as pd
import numpy as np
from code1 import errorOutput
from code1 import src_load
from code1 import mainflow
from code1.logger import capture_log_message
import utils



def load_config(ap_input_df_cols, src_id, audit_id, client_id):
    capture_log_message(log_message='Inside Load Config Function src_id:{}'.format(src_id),store_in_db=False)
    global source_name_mapping,target_name_mapping, audit_Id,client_Id, mapping_cols,ap_checks_list,gl_checks_list, MODE_KEY, MODE_CHECK, ACC_DOC_GL,TRANS_DESC_GL, DOC_TYPE_GL, DOC_TYPE_DESC_GL, AMOUNT_GL, ENTERED_BY_GL, POSTED_BY_GL, SAP_ACCOUNT_GL, ACCOUNT_DESC_GL, SAP_COMPANY_GL, POSTED_LOCATION_GL, ENTERED_LOCATION_GL, POSTING_DATE_GL, LINE_ITEM_IDENTIFIER_GL, DEBIT_AMOUNT_GL, CREDIT_AMOUNT_GL, ENTRY_DATE_GL, ENTRY_TIME_GL,MANUAL_ENTRY_GL,DEBIT_CREDIT_INDICATOR_GL,ACC_DOC,TRANS_DESC ,DOC_TYPE,DOC_TYPE_DESC,AMOUNT,ENTERED_BY,POSTED_BY,GL_ACCOUNT_TYPE,ACCOUNT_DESC,COMPANY_CODE,INVOICE_DATE,INVOICE_AMOUNT,DUE_DATE,PAYMENT_DATE,SUPPLIER_ID,SUPPLIER_NAME
    global INVOICE_NUMBER,PAYMENT_TERMS,DISCOUNT_PERCENTAGE, DISCOUNT_TAKEN, PAYMENT_AMOUNT,CREDIT_PERIOD ,DISCOUNT_PERIOD, POSTED_LOCATION,DEBIT_AMOUNT, CREDIT_AMOUNT ,ENTERED_LOCATION, POSTED_DATE, LINE_ITEM_IDENTIFIER,DEBIT_CREDIT_INDICATOR,INV_ACC_DOC,INVOICE_STATUS,CLEARING_DOC
    global POSTED_POSITION_GL,POSTED_DEPARTMENT_GL,ENTERED_BY_POSITION_GL,ENTERED_BY_DEPARTMENT_GL,REVERSAL_GL,REVERSE_DOCUMENT_NUMBER_GL,LEGAL_ENTITY_NAME_AND_ADDRESS, INVOICE_CURRENCY,VAT_ID
    global BALANCE_AMOUNT,PURCHASE_ORDER_NUMBER,PAYMENT_METHOD,BANK_ACCOUNT_NUMBER,PAYMENT_TERMS_DESCRIPTION,GRN_NUMBER,GRN_DATE,PURCHASE_ORDER_DATE,BASELINE_DATE,REQUISITION_DATE,PURCHASE_REQUEST_NUMBER,ENTERED_DATE,APPROVED_USER_1,APPROVED_USER_2,APPROVED_USER_3,APPROVED_USER_4,APPROVED_USER_5
    global COST_CENTER_GL,WBS_ELEMENT_GL,PARKED_DATE_GL,PARKED_BY_GL,MATERIAL_NUMBER_GL,HEADER_TEXT_GL,LINE_ITEM_TEXT_GL


    MODE_KEY = 'AP' if g.module_nm in ['AP','ZBLOCK'] else g.module_nm

    mapping_cols = [(col, col) for col in ap_input_df_cols]
    pass_mapping_cols(mapping_cols)
    
    g.src_id = src_id
    audit_Id = audit_id
    client_Id = client_id

    gl_checks_list = ["NULL_CHECK", 'UNIQUE_IDENTIFIER', 'CREDIT_DEBIT_INDICATOR_PRESENT', 'DEBIT_CREDIT_BALANCE_CHECK',
                'DATE_CHECK', 'MANUAL_ENTRY_FLAG','DOC_TYPE_CHECK'] 

    # CHANGE
    ap_checks_list = ["NULL_CHECK", 'UNIQUE_IDENTIFIER', 'CREDIT_DEBIT_INDICATOR_PRESENT', 
                'DATE_CHECK', 'MANUAL_ENTRY_FLAG','CONDITIONAL_NULL_CHECK',
                ] #"DUE_DATE_CHECK", 'DOC_TYPE_CHECK','DEBIT_CREDIT_BALANCE_CHECK'


    # MODE_KEY = config['MAPPING_MODULE'][0]
    capture_log_message(log_message='Mode Key is {}'.format(MODE_KEY),store_in_db=False)


    # if MODE_KEY == 'GL':
    #     #Maps columns in the data to the DB columns
    #     ACC_DOC_GL = 'ACCOUNTING_DOC'
    #     TRANS_DESC_GL = 'TRANSACTION_DESCRIPTION'
    #     DOC_TYPE_GL =  'DOC_TYPE'
    #     DOC_TYPE_DESC_GL = 'DOC_TYPE_DESCRIPTION'
    #     AMOUNT_GL = 'AMOUNT'
    #     ENTERED_BY_GL = 'ENTERED_BY'
    #     POSTED_BY_GL = 'POSTED_BY'
    #     SAP_ACCOUNT_GL = 'SAP_ACCOUNT'
    #     ACCOUNT_DESC_GL = 'ACCOUNT_DESCRIPTION'
    #     SAP_COMPANY_GL = 'SAP_COMPANY'
    #     POSTED_LOCATION_GL = 'POSTED_LOCATION'
    #     ENTERED_LOCATION_GL = 'ENTERED_LOCATION'
    #     POSTING_DATE_GL = 'POSTED_DATE'
    #     LINE_ITEM_IDENTIFIER_GL = 'LINE_ITEM_IDENTIFIER'
    #     DEBIT_AMOUNT_GL = 'DEBIT_AMOUNT'
    #     CREDIT_AMOUNT_GL = 'CREDIT_AMOUNT'
    #     ENTRY_DATE_GL = 'ENTERED_DATE'
    #     ENTRY_TIME_GL = 'ENTRY_TIME'
    #     MANUAL_ENTRY_GL = 'MANUAL_ENTRY'
    #     DEBIT_CREDIT_INDICATOR_GL = 'DEBIT_CREDIT_INDICATOR'
    #     POSTED_POSITION_GL = 'POSTED_POSITION'
    #     POSTED_DEPARTMENT_GL = 'POSTED_DEPARTMENT'
    #     ENTERED_BY_POSITION_GL = 'ENTERED_BY_POSITION'
    #     ENTERED_BY_DEPARTMENT_GL = 'ENTERED_BY_DEPARTMENT'
    #     REVERSAL_GL = 'REVERSAL'
    #     REVERSE_DOCUMENT_NUMBER_GL = 'REVERSE_DOCUMENT_NUMBER'
    #     COST_CENTER_GL = "COST_CENTER"
    #     WBS_ELEMENT_GL = "WBS_ELEMENT"
    #     PARKED_DATE_GL = "PARKED_DATE"
    #     PARKED_BY_GL  = "PARKED_BY"
    #     MATERIAL_NUMBER_GL = "MATERIAL_NUMBER"
    #     HEADER_TEXT_GL = "HEADER_TEXT"
    #     LINE_ITEM_TEXT_GL = "LINE_ITEM_TEXT"
    #     #map target cols to variable for renaming
    #     capture_log_message(log_message=f"ACC_DOC {ACC_DOC_GL} TRANS_DESC_GL {TRANS_DESC_GL} DOC_TYPE_GL {DOC_TYPE_GL}",
    #                         store_in_db=False)
        

    #     MODE_CHECK = src_load.get_glcheck()
    #     capture_log_message(log_message=f"Mode Check is {MODE_CHECK}",store_in_db=False)

        

    
    if MODE_KEY == 'AP':
    #Maps columns in the AP data to the DB columns
        # ACC_DOC = 'ACCOUNTING_DOC'
        # TRANS_DESC = 'TRANSACTION_DESCRIPTION'
        # DOC_TYPE =  'DOC_TYPE'
        # DOC_TYPE_DESC = 'DOC_TYPE_DESCRIPTION'
        # AMOUNT = 'AMOUNT'
        # ENTERED_BY = 'ENTERED_BY'
        # POSTED_BY = 'POSTED_BY'
        # GL_ACCOUNT_TYPE = 'GL_ACCOUNT_TYPE'
        # ACCOUNT_DESC = 'ACCOUNT_DESCRIPTION'
        # COMPANY_CODE = 'COMPANY_CODE'
        # INVOICE_DATE = 'INVOICE_DATE'
        # INVOICE_NUMBER = 'INVOICE_NUMBER'
        # INVOICE_AMOUNT = 'INVOICE_AMOUNT'
        # DUE_DATE = 'DUE_DATE'
        # PAYMENT_DATE = 'PAYMENT_DATE'
        # SUPPLIER_ID = 'SUPPLIER_ID'
        # SUPPLIER_NAME = 'SUPPLIER_NAME'
        # PAYMENT_TERMS = 'PAYMENT_TERMS'
        # DISCOUNT_PERCENTAGE = 'DISCOUNT_PERCENTAGE'
        # DISCOUNT_TAKEN = 'DISCOUNT_TAKEN'
        # LINE_ITEM_IDENTIFIER = 'LINE_ITEM_IDENTIFIER'
        # PAYMENT_AMOUNT = 'PAYMENT_AMOUNT'
        # POSTED_LOCATION = 'POSTED_LOCATION'
        # ENTERED_LOCATION = 'ENTERED_LOCATION'
        # POSTED_DATE = 'POSTED_DATE'
        # DISCOUNT_PERIOD = 'DISCOUNT_PERIOD'
        # CREDIT_PERIOD = 'CREDIT_PERIOD'
        # DEBIT_AMOUNT = 'DEBIT_AMOUNT'
        # CREDIT_AMOUNT = 'CREDIT_AMOUNT'
        # DEBIT_CREDIT_INDICATOR = 'DEBIT_CREDIT_INDICATOR'
        # INVOICE_STATUS= 'INVOICE_STATUS'
        # CLEARING_DOC= 'CLEARING_DOC'
        # BALANCE_AMOUNT = 'BALANCE_AMOUNT'
        # PURCHASE_ORDER_NUMBER = 'PURCHASE_ORDER_NUMBER'
        # PAYMENT_METHOD = 'PAYMENT_METHOD'
        # BANK_ACCOUNT_NUMBER = 'BANK_ACCOUNT_NUMBER'
        # PAYMENT_TERMS_DESCRIPTION = 'PAYMENT_TERMS_DESCRIPTION'
        # GRN_NUMBER = 'GRN_NUMBER'
        # GRN_DATE = 'GRN_DATE'
        # PURCHASE_ORDER_DATE = 'PURCHASE_ORDER_DATE'
        # BASELINE_DATE = 'BASELINE_DATE'
        # REQUISITION_DATE = 'REQUISITION_DATE'
        # PURCHASE_REQUEST_NUMBER = 'PURCHASE_REQUEST_NUMBER'
        # ENTERED_DATE = 'ENTERED_DATE'
        # LEGAL_ENTITY_NAME_AND_ADDRESS = 'LEGAL_ENTITY_NAME_AND_ADDRESS'
        # INVOICE_CURRENCY = 'INVOICE_CURRENCY'
        # VAT_ID = 'VAT_ID'
    
        
        MODE_CHECK = src_load.get_apcheck()
        configs = utils.get_config("configs.json")
        configs_df = pd.DataFrame(list(configs.items()), columns=["CONFIG_KEY", "CONFIG_VALUE"])
        MODE_CHECK = pd.concat([MODE_CHECK, configs_df], ignore_index=True)

        capture_log_message(log_message=f"Mode Check is {MODE_CHECK}",store_in_db=False)

errorOutObj = errorOutput.dataErrorOutput()

def pass_mapping_cols(mapping_cols):
    '''
    passing the mapping cols to errorOutputObject
    '''
    errorOutObj.updateMapping(mapping_cols)


def conditional_null_check(data_df):
    
    mode_key = MODE_KEY
    if mode_key == 'AP':
        try:
            null_flag = True
            cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='INVOICE_STATUS_CHECK',"CONFIG_VALUE"])
            cols = [col for column in [c.split(',') for c in cols] for col in column]
            
            # data_df[cols[0]]=data_df[cols[0]].str.upper()
            cols1 = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='CONDITIONAL_CHECK',"CONFIG_VALUE"])
            cols1 = [col for column in [c.split(',') for c in cols1] for col in column]
            cols1 = list(set(cols1))
            capture_log_message(log_message=f"Columns for conditional null check are {cols1}",store_in_db=False)
            data_columns = data_df.columns
            cols1 = [col for col in cols1 if col in data_columns]
            capture_log_message(log_message=f"filtered Columns for conditional null check {cols1}",store_in_db=False)
            for col in cols1:
                data_df[col].replace("",np.nan,inplace=True)
            
            conditional_check = (data_df[cols[0]] == 'PAID') & (data_df[cols1].isnull().any(axis=1))
            conditional_null_check_cols = []
            rows_list=[]
            errors = data_df[conditional_check][cols1].isnull().sum()
            for column,count in errors.items():
                # conditional_log.append(f"{column} is missing for {count} paid invoices.")
                capture_log_message(log_message=f"{column} is missing for {count} paid invoices.")
                if count > 0:
                    null_flag= False
                conditional_null_check_cols.append(column)
                error_indices = data_df[conditional_check].loc[data_df[conditional_check][column].isnull()][['ROW_NUM','ROW_NUM_y']].values.tolist()
                rows_list.append({"column":column,"row":error_indices,"count":count})
            
            if null_flag:
                # conditional_log.append("No errors found.")
                capture_log_message(log_message='No errors found.')
                errorOutObj.updateConditionalNullCheck("Pass","Condtional Null Check Passed",rows_list)
            else:
                errorOutObj.updateConditionalNullCheck("Fail",f"Condtional Null Check Failed",rows_list)
        except Exception as e :
            capture_log_message(current_logger=g.error_logger,log_message=f"Error occured while validating conditional null check,{e}")
        return null_flag

def null_check(data_df):
    '''
    Function used for checking if there are any null values in the mandatory field
    '''
    # cols = config[mode_check]['NULL_CHECK'].split(',')
    capture_log_message(log_message='Inside Null Check Function')
    mode_key = MODE_KEY
    capture_log_message(log_message=f"Mode_Key:{mode_key}")
    try:
        cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='NULL_CHECK',"CONFIG_VALUE"])
        cols = [col for column in [c.split(',') for c in cols] for col in column]
        cols = [col for col in cols if col != "PAYMENT_TERMS"]
        # Remove Invoice status column from Null Check
        cols = [col for col in cols if col!='INVOICE_STATUS']
        cols = list(set(cols))
        cols = [col for col in cols if col in data_df.columns]
        # CHANGE
        columns_to_remove = ['ACCOUNT_DESCRIPTION','DOC_TYPE_DESCRIPTION','PAYMENT_AMOUNT','GL_ACCOUNT_TYPE']
        cols = [col for col in cols if col not in columns_to_remove]
        
        capture_log_message(log_message=f"Columns are {cols}",store_in_db=False)
        data_df = data_df.replace(" ", np.nan)
        capture_log_message(log_message="Replace of single space Done")
        data_df = data_df.replace("", np.nan)
        capture_log_message(log_message="Replace of empty space Done")
        null_flag = True
        # null_log="Null Check Passed"
        null_cols=[]
        rows_list=[]
        for column in cols:
            null_count=0
            capture_log_message(log_message=f"Started checking the null count for column {column}"  )
            length = data_df[column].isnull().sum()
            capture_log_message(log_message=f"Done checking null for column {column},and length is {length}")
            if length > 0:
                capture_log_message(log_message=f"Inside if condition for column {column}")
                null_flag = False
                null_cols.append(f"{column}")
                capture_log_message(log_message=f"Appended {column} to null_cols list")
                if mode_key == 'GL':
                    rows =data_df[data_df[column].isnull()]['ROW_NUM'].values.tolist()
                    capture_log_message(log_message=f"Null Check-Inside if GL condition,obtained rows")                  
                elif mode_key == 'AP':
                    rows =data_df[data_df[column].isnull()][['ROW_NUM','ROW_NUM_y']].values.tolist()
                    capture_log_message(log_message=f"Null Check-Inside if AP condition,obtained rows")
                null_count +=len(rows)
                rows_list.append({"Column":column,"Row":rows,"Count":null_count})
                capture_log_message(log_message=f"Created rowlist for column {column}")
                # null_log=f"Null Column detected on {column}"
            
        null_cols_length = len(null_cols)
        if null_cols_length==0:
            errorOutObj.updateNullCheck("Pass",f"{null_cols_length} Null column Detected",null_cols)
            capture_log_message(log_message="Null Check Passed")
        else:
            capture_log_message(log_message=f"Null Check Failed, Null columns are {null_cols}")
            errorOutObj.updateNullCheck("Fail",f"{null_cols_length} Null column Detected",rows_list)
            capture_log_message(log_message="Null Check Failed",
                                current_logger=g.error_logger,
                                error_name=utils.NULL_CHECK_FAILED)
    except Exception as e:
        null_flag = False
        rows_list.append({"Column":column,"Row":'NA',"Count":0})
        errorOutObj.updateNullCheck("Fail",'Error occured while validating null check',rows_list)
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating null check,{e}",
                            error_name=utils.NULL_CHECK_FAILED) 
        
    return null_flag


def due_date_check(data_df):
    '''
    Function used to check if due date is present
    '''
    mode_key = MODE_KEY
    try:
        if mode_key == 'AP':
            cols = data_df.columns
            # datecheck = config[mode_check]['DUE_DATE_CHECK']
            datecheck = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='DUE_DATE_CHECK',"CONFIG_VALUE"])
            datecheck = [col for column in [c.split(',') for c in datecheck] for col in column]
            datecheck = datecheck[0]
           
            if datecheck in cols:
                null_flag = True
                # due_log="Due Date check passed"
                errorOutObj.updateDueDateCheck("Pass","Due Date Check Column Present",datecheck)

            else:
                # cols = config[mode_check]['DUE_DATE_COLS'].split(",")
                cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='DUE_DATE_COLS',"CONFIG_VALUE"])
                cols = [col for column in [c.split(',') for c in cols] for col in column]
                data_df[cols[0]] = pd.to_datetime(data_df[cols[0]])
                data_df[datecheck] = data_df[cols[0]] + \
                    pd.to_timedelta(data_df[cols[1]], unit='d')
                null_flag = True
                # due_log=f"Due Date column created with {cols[0]} and {cols[1]}"
                
                errorOutObj.updateDueDateCheck("Pass","Due Date Column Created",cols)
        else:
            null_flag = False
    except Exception as e:
        # errorOutObj.updateDueDateCheck("Fail","Error occured while validating due date check ",[])
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating due date check,{e}",
                            error_name=utils.DUE_DATE_CHECK_FAILED)
    return null_flag


def unique_identifier(data_df):
    '''
    Function used to set or create a new unique transaction identifier
    '''
    
    uniq_ide_col_length = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='UNIQ_IDEN',"CONFIG_VALUE"])
    capture_log_message(f"The config value is {uniq_ide_col_length}")
    uniq_ide_col_length = len([col for column in [c.split(',') for c in uniq_ide_col_length] for col in column])
    row_index=[]
    null_flag=False
    mode_key = MODE_KEY
    try:
        if mode_key == 'GL':
            temp_data = src_load.get_temp_data(g.src_id, g.audit_id)
        else:
            temp_data = src_load.get_temp_dataUI_AP(g.src_id, g.audit_id)
        if uniq_ide_col_length == 1:

            # cols = config[mode_check]['UNIQ_IDEN']
            cols =list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='UNIQ_IDEN',"CONFIG_VALUE"])
            cols = [col for column in [c.split(',') for c in cols] for col in column]
            cols = [col for col in cols if col in data_df.columns]
            capture_log_message(log_message=f"Unique Identifier Column is {cols}")
            capture_log_message(log_message=f"Columns in temp data:{temp_data.columns}",store_in_db=False)
            unique = "TRANSACTION_ID_GA"
            data_df[unique] = temp_data[cols[0]]
            unique_key = data_df[unique]
            capture_log_message(log_message='No. of unique entries for TRANSACTION_ID_GA column:{}'.format(len(set(unique_key))))
            capture_log_message(log_message='No. of rows in dataframe:{}'.format(len(data_df)))
            if len(set(unique_key)) != len(data_df):
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"The given column {cols} is not unique identifier",
                                    error_name=utils.UNIQUE_IDENTIFIER_CHECK_FAILED)
                # uni_log= f"The given column {cols} is not unique identifier"
                null_flag = False
                errorOutObj.updateUniqueIdentifier("Fail","Given Column is not unique identifier",cols)
            else:
                capture_log_message(log_message="Unique key identifier is accepted")
                # uni_log = "Unique key identifier is accepted"
                null_flag = True
                errorOutObj.updateUniqueIdentifier("Pass","Given Column is unique identifier",cols)
        else:
            # Limit the length of the unique identifier to be checked.
            # unique = config[mode_check]['UNIQ_IDEN']
            unique = "TRANSACTION_ID_GA"
            # cols = config[mode_check]['NEW_UNIQ_IDEN'].split(',')
            cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='UNIQ_IDEN',"CONFIG_VALUE"])
            cols = [col for column in [c.split(',') for c in cols] for col in column]
            cols = [col for col in cols if col in data_df.columns]
            capture_log_message(log_message='Columns for unique identifier:{}'.format(cols))
            capture_log_message(log_message='Columns in temp data:{}'.format(temp_data.columns),store_in_db=False)
        
            data_df[unique] = temp_data[cols].apply(
                lambda x: '_'.join(x.astype(str)), axis=1)
            unique_key=data_df[unique]
            if len(set(unique_key)) != len(data_df):
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"The given column {cols} is not unique identifier",
                                    error_name=utils.UNIQUE_IDENTIFIER_CHECK_FAILED)
                # uni_log= f"The given column {cols} is not unique identifier"
                null_flag = False
                errorOutObj.updateUniqueIdentifier("Fail","Given Column is not unique identifier",cols)
            else:
                capture_log_message(log_message="Unique key identifier is accepted")
                # uni_log = "Unique key identifier is accepted"
                null_flag = True
                errorOutObj.updateUniqueIdentifier("Pass","Given Column is unique identifier",cols)
    except Exception as e:
        errorOutObj.updateUniqueIdentifier("Fail","Error occured while validating Unique Identifier",[])
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating Unique Identifier,{e}",
                            error_name=utils.UNIQUE_IDENTIFIER_CHECK_FAILED)

    return null_flag


def credit_period_check(df, error_dict):
    """
    Compares CREDIT_PERIOD (calculated) with CREDIT_PERIOD_PTD (extracted from payment terms).
    Comparison is performed only for rows where CREDIT_PERIOD_PTD is not null.
    Assumes CREDIT_PERIOD does not contain null values due to prior validation.
    If there is a discrepancy, adds it to error_dict and updates errorOutObj.

    Parameters:
    - df (pd.DataFrame): The input dataframe containing CREDIT_PERIOD and CREDIT_PERIOD_PTD.
    - error_dict (dict): Dictionary to store errors.
    """

    # Ensure both columns are present
    if 'CREDIT_PERIOD' in df.columns and 'CREDIT_PERIOD_PTD' in df.columns:

        valid_rows = df[df['CREDIT_PERIOD_PTD'].notnull()]
        
        # Find rows where CREDIT_PERIOD and CREDIT_PERIOD_PTD differ
        discrepancy = valid_rows[valid_rows['CREDIT_PERIOD'] != valid_rows['CREDIT_PERIOD_PTD']]
        
        if not discrepancy.empty:
            # Add discrepancy to error_dict
            rows = discrepancy.index.tolist()
            error_dict["CreditPeriodDiscrepancy"] = rows
            capture_log_message(f"Credit period discrepancy found in {len(rows)} rows")
            # Update errorOutObj with a new check
            errorOutObj.updatecreditPeriodConsistency(
                                            "Fail", 
                                            f"Credit period discrepancy found in {len(rows)} rows", 
                                            [{"column": ["CREDIT_PERIOD"], "rows": rows, "count": len(rows)}]
                                            )
            g.credit_period_flag = True
        else:
            # No discrepancy, set to Pass
            errorOutObj.updatecreditPeriodConsistency("Pass", "Credit period consistent", [])
            g.credit_period_flag = False
            capture_log_message("Credit period check passed")
    else:
        # Columns not present, log a warning
        g.credit_period_flag = False
        capture_log_message("Warning: CREDIT_PERIOD or CREDIT_PERIOD_PTD not found in dataframe")


def credit_debit_indicator_present(data_df):
    '''
    Check if there is credit debit indicator column and amount column present or
       debit/credit amount given
    '''
    # Extract config values into a dictionary for easy access
    config = MODE_CHECK.set_index('CONFIG_KEY')['CONFIG_VALUE'].to_dict()

    # Assign individual config values
    deb_cred_col_name = config.get('DEBIT_CRED_INDIC_COL')
    amount_col = config.get('AMOUNT_COL')
    deb_col = config.get('DEBIT_COL')
    cred_col = config.get('CREDIT_COL')

    # Flatten and clean flag lists
    deb_flag = [flag.strip() for val in config.get('DEBIT_FLAGS', '').split(',') for flag in val.split(',')]
    cred_flag = [flag.strip() for val in config.get('CREDIT_FLAGS', '').split(',') for flag in val.split(',')]

    # Initialize flags and accepted values
    null_flag = False
    credit_debit_indicator_present_cols = []
    credit_debit_unique = data_df[deb_cred_col_name].unique()
    credit_debit_unique_accepted = deb_flag + cred_flag

    # Construct messages
    msg_deb_flag = ','.join(deb_flag)
    msg_cred_flag = ','.join(cred_flag)


    
    # Check if the value is present in the columns of the DataFrame
    try:
        if not set(credit_debit_unique).issubset(credit_debit_unique_accepted):
            errorOutObj.updateCreditDebitIndicator("Fail",f"Credit Debit indicator has values other than {msg_cred_flag} and {msg_deb_flag}.",deb_cred_col_name)
            capture_log_message(current_logger=g.error_logger,
                                log_message=f"Credit Debit indicator has values other than {msg_cred_flag} and {msg_deb_flag}."
                               ,error_name=utils.CREDIT_DEBIT_INDICATOR_CHECK_FAILED)
            
        elif deb_cred_col_name in data_df.columns and deb_col in data_df.columns and cred_col in data_df.columns:
            capture_log_message(log_message=f"{deb_cred_col_name}, {deb_col} and {cred_col} present in the Data columns")
            # cre_deb_log=f"{deb_col} and {cred_col} already present in the Data columns"
            null_flag = True
            credit_debit_indicator_present_cols = [deb_cred_col_name,deb_col,cred_col]
            errorOutObj.updateCreditDebitIndicator("Pass",f"{deb_cred_col_name},{deb_col} and {cred_col} present in the Data columns",credit_debit_indicator_present_cols)

        else:
            capture_log_message(current_logger=g.error_logger,
                                log_message=f"Debit and Credit Amount not provided or cannot be created with current data",
                                error_name=utils.CREDIT_DEBIT_INDICATOR_CHECK_FAILED)
            # cre_deb_log= "Debit and Credit Amount not provided or cannot be created with current data"
            errorOutObj.updateCreditDebitIndicator("Fail","Debit and Credit Amount not provided or cannot be created with current data",deb_cred_col_name)

    except Exception as e:
        errorOutObj.updateCreditDebitIndicator("Fail","Error occured while validating Credit and debit columns presence",[deb_cred_col_name])
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating Credit and debit columns presence :{e}",
                            error_name=utils.CREDIT_DEBIT_INDICATOR_CHECK_FAILED)

    return null_flag

def debit_credit_balance_check(data_df):
    '''
    Function used for checking if the debit and credit are balanced
    '''
    # cols = config[mode_check]['DEBIT_CREDIT_BALANCE_CHECK'].split(',')
    null_flag = False
    mode_key = MODE_KEY
    try:
        cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='DEBIT_CREDIT_BALANCE_CHECK',"CONFIG_VALUE"])
        cols = [col for column in [c.split(',') for c in cols] for col in column]
        cols = [col for col in cols if col in data_df.columns]
        data_df[cols[0]] = data_df[cols[0]].fillna(0)
        data_df[cols[0]] = data_df[cols[0]].apply(
            lambda x: str(x).replace(',', ''))
        data_df[cols[0]] = data_df[cols[0]].astype(float).abs()
        data_df[cols[1]] = data_df[cols[1]].fillna(0)
        data_df[cols[1]] = data_df[cols[1]].apply(
            lambda x: str(x).replace(',', ''))
        data_df[cols[1]] = data_df[cols[1]].astype(float).abs()
        data_df['NON_BALANCED'] = data_df.groupby(cols[2])[cols[0]].transform(
            'sum')-data_df.groupby(cols[2])[cols[1]].transform('sum')
        non_balanced = data_df[data_df['NON_BALANCED'].astype(int) > 0]
        imbalanced_records = non_balanced[cols[2]].nunique()
        if mode_key=='GL':
            rows = non_balanced['ROW_NUM'].values.tolist()
        elif mode_key == 'AP':
            rows = non_balanced[['ROW_NUM','ROW_NUM_y']].values.tolist()
        # imbalanced_acc_doc = non_balanced[cols[2]].astype(int).unique().tolist()

        imb_list = []
        
        if imbalanced_records > 0:
            capture_log_message(current_logger=g.error_logger,
                                log_message=f"There are {imbalanced_records} accounting docs with imbalance records",
                                error_name=utils.DEBIT_CREDIT_BALANCE_CHECK_FAILED)
            # deb_imbal=f"There are {imbalanced_records} accounting docs with imbalance records"
            null_flag = False
            # msg = {"Doc":imbalanced_acc_doc}
            msg = {"Doc":rows}
            imb_list.append(msg)
            errorOutObj.updateCreditDebitBalance("Fail",f"There are {imbalanced_records} accounting docs with imbalance records",imb_list)
        else:
            capture_log_message(log_message="The debit and credit amount are balanced")
            # deb_imbal = "The debit and credit amount are balanced"
            null_flag = True
            errorOutObj.updateCreditDebitBalance("Pass","The debit and credit amount are balanced",[])
    
        # The imbalance records are rejected and user needs to
        data_df = data_df[data_df["NON_BALANCED"] == 0]
    except Exception as e:
        
        imb_list = []
        msg = {"Doc":"NA"}
        imb_list.append(msg)
        errorOutObj.updateCreditDebitBalance("Fail","Error occured while validating Credit and debit columns for balance check",imb_list)
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating Credit and debit columns for balance check :{e}",
                            error_name=utils.DEBIT_CREDIT_BALANCE_CHECK_FAILED)

    return null_flag


def manual_entry_flag(data_df):
    '''
    Function used to check the manual entry flag exists and is correct
    '''

    manual_entry_status = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='MANUAL_ENTRY',"CONFIG_VALUE"])
    null_flag = False
    error_list=[]
    try:
        if int(manual_entry_status[0]) == 1:
            # cols = config[mode_check]['MANUAL_ENTRY_COL']
            cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='MANUAL_ENTRY_COL',"CONFIG_VALUE"])
            cols = [col for column in [c.split(',') for c in cols] for col in column]
            
            # data_df[cols[0]]=data_df[cols[0]].astype(str).upper()
            manual_entry_shape = data_df[cols].isnull().sum()
            error_count=manual_entry_shape[0]
            #check if unique value are'YES' or 'NO'
            uniq_value = set(data_df[cols[0]].unique())
            
            if manual_entry_shape[0] > 0:
                if MODE_KEY == 'GL':
                   rows = data_df[data_df[cols[0]].isnull()]['ROW_NUM'].values.tolist()
                elif MODE_KEY == 'AP':
                    rows=data_df[data_df[cols[0]].isnull()][['ROW_NUM','ROW_NUM_y']].values.tolist()
                error_list.append({'column':cols[0],"rows":rows,'count':str(error_count)})
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"There are {manual_entry_shape[0]} records without manual entry flag",
                                    error_name=utils.MANUAL_ENTRY_FLAG_FAILED)
                
                # man_ent=f"There are {manual_entry_shape} records without manual entry flag"
                null_flag = False
                errorOutObj.updateManualEntryFlag("Fail",f"There are {manual_entry_shape[0]} records without manual entry flag",error_list)
            elif not uniq_value.issubset({'YES','NO'}):
                error_list.append({'column':cols[0]})
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"The {cols[0]} contain values other than 'YES' and 'NO'",
                                    error_name=utils.MANUAL_ENTRY_FLAG_FAILED)
                # man_ent = f"The {cols} contain values other than 'YES' and 'NO'"
                null_flag = False
                errorOutObj.updateManualEntryFlag("Fail_Sub",f"The manual entry column contain values other than 'YES' and 'NO'",error_list)

            else:
                error_list.append({'column':cols[0]})
                capture_log_message(log_message="The manual entry flag exists for all records")
                # man_ent = "The manual entry flag exists for all records"
                null_flag = True
                errorOutObj.updateManualEntryFlag("Pass","The manual entry flag exists for all records",error_list)
        else:
            error_list.append({'column':'N/A'})
            capture_log_message(log_message="Manual entry flag is not provided for the data")
            # man_ent="Manual entry flag is not provided for the data"
            null_flag = True
            errorOutObj.updateManualEntryFlag("Pass","Manual entry flag is not provided for the data",error_list)
    except Exception as e:
        # errorOutObj.updateManualEntryFlag("Fail","Error occured while validating manual entry column",[])
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating manual entry column : {e}",
                            error_name=utils.MANUAL_ENTRY_FLAG_FAILED)
     
    return null_flag


def convert_date(date):
    '''
    Convert the given format dates to target date format, part of date_check function
    '''
    if not isinstance(date,str) or not date.strip():
        final_output=""
    else:
        try:
            datetime.strptime(str(date), '%d-%m-%Y %H:%M:%S')
        except:
            final_output = 'Invalid date format'
            date = str(date).replace("/", "-")
            for fmt in ('%d-%m-%Y %H:%M:%S', '%d-%m-%Y', '%d.%m.%Y', '%Y.%m.%d', '%Y%m%d', '%d%m%Y', '%Y-%m-%d',
                        '%Y-%d-%m', '%d-%m-%Y %H:%M'):
                try:
                    diff_dateformat = datetime.strptime(date, fmt)
                    final_output = datetime.strftime(
                        diff_dateformat, "%d-%m-%Y %H:%M:%S")
                    return final_output

                except ValueError:
                    continue
            return final_output

def date_check(data_df):
    '''
    Function to check the date and time in mandatory field is in correct format
    '''
    mode_key = MODE_KEY
    date_check_msg=[]
    null_flag = True
    try:
        cols1 = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY'] == 'DATE_CHECK', 'CONFIG_VALUE'])
        cols1 = [col for column in [c.split(',') for c in cols1] for col in column]
        #data_df[cols1] = data_df[cols1].fillna('0')
        cols1 = list(set(cols1))
        # CHANGE
        columns_to_not_consider = []
        cols1  = [col for col in cols1 if col not in columns_to_not_consider]
        capture_log_message(log_message=f"Columns for date check are {cols1}",store_in_db=False)
        for dateform in cols1:
            new_dt = data_df[dateform].apply(convert_date)
            lst = list(new_dt[new_dt == 'Invalid date format'].index)
            limited_list = lst
            count = len(lst)
            rows = data_df.iloc[limited_list]['ROW_NUM'].values.tolist()
            
            # if mode_key == 'AP':
            #     rows = data_df.iloc[limited_list][['ROW_NUM', 'ROW_NUM_y']].values.tolist()
            #     cols_to_pick = src_load.get_gl_ap_cols(mapping_cols, dateform, g.src_id, g.audit_id)
            #     counts = [len(set([i[j] for i in rows])) for j in range(2)]
            #     if 'APROW' in cols_to_pick:
            #         count = counts[1]
            #     elif 'GLROW' in cols_to_pick:
            #         count = counts[0]
            # elif mode_key == 'GL':
            #     count = len(lst)
            #     rows = data_df.iloc[limited_list]['ROW_NUM'].values.tolist()
            
            if len(lst) :
                null_flag = False
                dict_msg = {'column': dateform, 'count': count, 'rows': rows}
                date_check_msg.append(dict_msg)
                
            if null_flag:
                errorOutObj.updateDateCheck('Pass', 'The date format is correct', date_check_msg)
            else:
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f'Date check failed:{date_check_msg}',
                                    error_name=utils.DATE_CHECK_FAILED)
                errorOutObj.updateDateCheck('Fail', 'The date format is incorrect', date_check_msg)
    except Exception as e:
        null_flag = False
        dict_msg = {'column': dateform, 'count': 0, 'rows': 'NA'}
        date_check_msg.append(dict_msg)
        errorOutObj.updateDateCheck('Fail', 'Error occurred while validating date check', date_check_msg)
        capture_log_message(current_logger=g.error_logger,
                            log_message=f'Failed to validate date check, {e}',
                            error_name=utils.DATE_CHECK_FAILED)
    
    return null_flag

def date_format_check(flag: bool, error_message=None, date_check_msg=[]):
    '''
    Function to store the date format check error message
    '''
    if flag:
        g.date_format_flag = False
        capture_log_message(log_message='Date Format check passed')
        errorOutObj.updateDateFormatCheck('Pass', 'The date format is correct', date_check_msg)
    else:
        g.date_format_flag = True
        capture_log_message(current_logger=g.error_logger,
                            log_message=f'Date Format check failed',
                            error_name=utils.DATE_CHECK_FAILED)

        errorOutObj.updateDateFormatCheck('Fail', error_message, date_check_msg)
    
    capture_log_message(log_message='Date Format check completed')

def doc_type_check(data_df):
    '''
    Function used to check the doc type format
    '''
    null_flag = True
    error_list=[]
    try:
        if MODE_KEY == 'AP':
            # cols = config[mode_check]['MANUAL_ENTRY_COL']
            cols = list(MODE_CHECK.loc[MODE_CHECK['CONFIG_KEY']=='DOC_TYPE_CHECK',"CONFIG_VALUE"])
            cols = [col for column in [c.split(',') for c in cols] for col in column]
            cols = [col for col in cols if col in data_df.columns]
            data_df[cols[0]]=data_df[cols[0]].str.upper()
            doc_type_unique = data_df[cols[0]].unique()
            msdoc_type_data = src_load.read_doctype()
            msdoc_type_unique = msdoc_type_data['DOCUMENT_TYPE_CODE'].unique()
            # msdoc_type_unique = ['KR','RE']
            rows_list = []
            if(all(doc in msdoc_type_unique for doc in doc_type_unique)):
                capture_log_message(log_message="Document types are in the required format.")
                null_flag = True
                rows_list.append({"column":cols[0],"row":[],"count":[]})
                errorOutObj.updateDocTypeFlag("Pass","Document types are in the required format.",rows_list)
            
            else:
                null_flag = False
                error_indices = data_df.loc[~data_df[cols[0]].isin(msdoc_type_unique)][['ROW_NUM','ROW_NUM_y']].values.tolist()
                count = len(error_indices)
                rows_list.append({"column":cols[0],"row":error_indices,"count":count})
                capture_log_message(current_logger=g.error_logger,
                                    log_message="Document types are not in the required format. :{}".format(cols[0]),
                                    error_name=utils.DOC_TYPE_CHECK_FAILED)
                errorOutObj.updateDocTypeFlag("Fail","Document types are not in the required format.",rows_list)

    except Exception as e:
        # errorOutObj.updateDocTypeFlag("Fail","Error occured while validating document type column",[])
        capture_log_message(current_logger=g.error_logger,
                            log_message=f"Error occured while validating document type column : {e}",
                            error_name=utils.DOC_TYPE_CHECK_FAILED)
     
    return null_flag

def Get_Data(check, df):
    '''
    Function to call the check functions
    '''
    capture_log_message(log_message='The shape of data is {}'.format(df.shape))
    Get_Module = {
        "NULL_CHECK": null_check,
        "DUE_DATE_CHECK": due_date_check,
        "DEBIT_CREDIT_BALANCE_CHECK": debit_credit_balance_check,
        "UNIQUE_IDENTIFIER": unique_identifier,
        "CREDIT_DEBIT_INDICATOR_PRESENT": credit_debit_indicator_present,
        "DATE_CHECK": date_check,
        'MANUAL_ENTRY_FLAG': manual_entry_flag,
        "CONDITIONAL_NULL_CHECK": conditional_null_check,
        "DOC_TYPE_CHECK":doc_type_check
    }
    data = Get_Module[check](df)
    return data


def all_check(dict1):
    '''
    Final Flag check
    '''
    date_format_flag = False
    credit_period_flag = False
    if getattr(g, "module_nm", None) in ["AP","ZBLOCK"]:
        date_format_flag = getattr(g, "date_format_flag", False)
        credit_period_flag = getattr(g, "credit_period_flag", False)
        
    if False not in dict1.values() and date_format_flag!= True and credit_period_flag!= True:
        dict1['FINAL_CHECK'] = True
        capture_log_message(log_message='Data is healthy')
        
    else:
        dict1['FINAL_CHECK'] = False
        
        capture_log_message(current_logger=g.error_logger,
                            log_message='Data is not in the correct format! Please load it in correct format')
    return dict1


BASEDIR = os.getcwd()
LOG_FILE_PATH = os.path.join(BASEDIR, "code1","logs")


# def gl_rename(data):
#     capture_log_message(log_message='Inside GL Rename Function, data shape:{}'.format(data.shape),store_in_db=False)
#     data.rename(columns={ACC_DOC_GL: target_name_mapping('ACCOUNTING_DOC'),TRANS_DESC_GL : target_name_mapping('TRANSACTION_DESCRIPTION'),DOC_TYPE_GL : target_name_mapping('DOC_TYPE'),
#     DOC_TYPE_DESC_GL:target_name_mapping('DOC_TYPE_DESCRIPTION'),AMOUNT_GL: target_name_mapping('AMOUNT'),ENTERED_BY_GL: target_name_mapping('ENTERED_BY'),POSTED_BY_GL : target_name_mapping('POSTED_BY'),
#     SAP_ACCOUNT_GL: target_name_mapping('SAP_ACCOUNT'),ACCOUNT_DESC_GL: target_name_mapping('ACCOUNT_DESCRIPTION'),SAP_COMPANY_GL: target_name_mapping('SAP_COMPANY'),POSTED_LOCATION_GL: target_name_mapping('POSTED_LOCATION'),
#     ENTERED_LOCATION_GL: target_name_mapping('ENTERED_LOCATION'),POSTING_DATE_GL: target_name_mapping('POSTED_DATE'),LINE_ITEM_IDENTIFIER_GL:target_name_mapping('LINE_ITEM_IDENTIFIER'),
#     DEBIT_AMOUNT_GL:target_name_mapping('DEBIT_AMOUNT'),CREDIT_AMOUNT_GL:target_name_mapping('CREDIT_AMOUNT'),
#     ENTRY_DATE_GL:target_name_mapping('ENTERED_DATE'),ENTRY_TIME_GL:target_name_mapping('ENTRY_TIME'),MANUAL_ENTRY_GL:target_name_mapping('MANUAL_ENTRY'),DEBIT_CREDIT_INDICATOR_GL:target_name_mapping('DEBIT_CREDIT_INDICATOR'),
#     POSTED_POSITION_GL:target_name_mapping('POSTED_POSITION'),POSTED_DEPARTMENT_GL:target_name_mapping('POSTED_DEPARTMENT'),ENTERED_BY_POSITION_GL:target_name_mapping('ENTERED_BY_POSITION'),ENTERED_BY_DEPARTMENT_GL:target_name_mapping('ENTERED_BY_DEPARTMENT'),
#     REVERSAL_GL:target_name_mapping('REVERSAL'),REVERSE_DOCUMENT_NUMBER_GL:target_name_mapping('REVERSE_DOCUMENT_NUMBER'),
#      COST_CENTER_GL:target_name_mapping('COST_CENTER'),WBS_ELEMENT_GL:target_name_mapping('WBS_ELEMENT'),PARKED_BY_GL:target_name_mapping('PARKED_BY'),
#     PARKED_DATE_GL:target_name_mapping('PARKED_DATE'),MATERIAL_NUMBER_GL:target_name_mapping('MATERIAL_NUMBER'),
#     LINE_ITEM_TEXT_GL:target_name_mapping('LINE_ITEM_TEXT'),HEADER_TEXT_GL:target_name_mapping('HEADER_TEXT')}, inplace=True)
#     capture_log_message(log_message='GL Rename Function Completed, data shape:{}'.format(data.shape),store_in_db=False)
#     return data

# def ap_rename(data)->pd.DataFrame:
#     capture_log_message(log_message='Inside AP Rename Function, data shape:{}'.format(data.shape),store_in_db=False)
#     data.rename(columns={ACC_DOC:target_name_mapping('ACCOUNTING_DOC'),TRANS_DESC: target_name_mapping('TRANSACTION_DESCRIPTION'),DOC_TYPE: target_name_mapping('DOC_TYPE'),
#     DOC_TYPE_DESC: target_name_mapping('DOC_TYPE_DESCRIPTION'),AMOUNT: target_name_mapping('AMOUNT'),ENTERED_BY: target_name_mapping('ENTERED_BY'),POSTED_BY: target_name_mapping('POSTED_BY'),
#     GL_ACCOUNT_TYPE: target_name_mapping('GL_ACCOUNT_TYPE'),ACCOUNT_DESC: target_name_mapping('ACCOUNT_DESCRIPTION'),COMPANY_CODE: target_name_mapping('COMPANY_CODE'),INVOICE_DATE:target_name_mapping('INVOICE_DATE'),INVOICE_NUMBER:target_name_mapping('INVOICE_NUMBER'),
#     INVOICE_AMOUNT:target_name_mapping('INVOICE_AMOUNT'),DUE_DATE:target_name_mapping('DUE_DATE'),PAYMENT_DATE:target_name_mapping('PAYMENT_DATE'),SUPPLIER_ID:target_name_mapping('SUPPLIER_ID'),SUPPLIER_NAME:target_name_mapping('SUPPLIER_NAME'),
#     PAYMENT_TERMS:target_name_mapping('PAYMENT_TERMS'),DISCOUNT_PERCENTAGE:target_name_mapping('DISCOUNT_PERCENTAGE'),DISCOUNT_TAKEN:target_name_mapping('DISCOUNT_TAKEN'),LINE_ITEM_IDENTIFIER:target_name_mapping('LINE_ITEM_IDENTIFIER'),PAYMENT_AMOUNT:target_name_mapping('PAYMENT_AMOUNT'),
#     POSTED_LOCATION: target_name_mapping('POSTED_LOCATION'),ENTERED_LOCATION: target_name_mapping('ENTERED_LOCATION'),POSTED_DATE: target_name_mapping('POSTED_DATE'),DISCOUNT_PERIOD:target_name_mapping('DISCOUNT_PERIOD'),CREDIT_PERIOD:target_name_mapping('CREDIT_PERIOD'),
#     DEBIT_AMOUNT:target_name_mapping('DEBIT_AMOUNT'),CREDIT_AMOUNT:target_name_mapping('CREDIT_AMOUNT'),DEBIT_CREDIT_INDICATOR:target_name_mapping('DEBIT_CREDIT_INDICATOR'),INVOICE_STATUS:target_name_mapping('INVOICE_STATUS'),CLEARING_DOC:target_name_mapping('CLEARING_DOC'),
#     BALANCE_AMOUNT:target_name_mapping('BALANCE_AMOUNT'),PURCHASE_ORDER_NUMBER:target_name_mapping('PURCHASE_ORDER_NUMBER'),PAYMENT_METHOD:target_name_mapping('PAYMENT_METHOD'),BANK_ACCOUNT_NUMBER:target_name_mapping('BANK_ACCOUNT_NUMBER'),PAYMENT_TERMS_DESCRIPTION:target_name_mapping('PAYMENT_TERMS_DESCRIPTION'),
#     GRN_NUMBER:target_name_mapping('GRN_NUMBER'),GRN_DATE:target_name_mapping('GRN_DATE'),PURCHASE_ORDER_DATE:target_name_mapping('PURCHASE_ORDER_DATE'),BASELINE_DATE:target_name_mapping('BASELINE_DATE'),REQUISITION_DATE:target_name_mapping('REQUISITION_DATE'),
#     PURCHASE_REQUEST_NUMBER:target_name_mapping('PURCHASE_REQUEST_NUMBER'),ENTERED_DATE:target_name_mapping('ENTERED_DATE'),
#     LEGAL_ENTITY_NAME_AND_ADDRESS:target_name_mapping('LEGAL_ENTITY_NAME_AND_ADDRESS'), INVOICE_CURRENCY:target_name_mapping('INVOICE_CURRENCY'),
#     VAT_ID:target_name_mapping('VAT_ID')
#     }, inplace=True)
#     #DEBIT_CREDIT_FLAG[1]:DEBIT_CREDIT_FLAG[0]
#     capture_log_message(log_message='AP Rename Function Completed, data shape:{}'.format(data.shape),store_in_db=False)
#     return data
    
def gl_pattern_match(df):
    '''
    Function to check whether the data matches the pattern
    '''
    any_pattern = r"^[^*]*$"
    amount_pattern = r'^\s*$|^\d+(\.\d+)?$'
    date_pattern = r"^(?:(?:(?:0?[1-9]|1\d|2[0-8])-(?:0?[1-9]|1[0-2])|(?:29|30)-(?:0?[13-9]|1[0-2])|31-(?:0?[13578]|1[02]))-(?!0000)\d{4}|29-02-(?!0000)(?:\d{2}(?:04|08|[2468][048]|[13579][26])|(?:16|[2468][048]|[3579][26])00)|\s*)$"
    
    any_cols=['ACCOUNTING_DOC','DOC_TYPE','DOC_TYPE_DESCRIPTION','POSTED_BY',        
            'SAP_ACCOUNT','ACCOUNT_DESCRIPTION','SAP_COMPANY','DEBIT_CREDIT_INDICATOR']
    amount_cols=['AMOUNT']
    date_cols=['POSTED_DATE']
    pattern_rows_list = []
    results = {}
    for col in df.columns:
        if col in any_cols:
            pattern = any_pattern
        elif col in amount_cols:
            pattern = amount_pattern
        elif col in date_cols:
            pattern = date_pattern
        else:
            pattern = ""
        if pattern:
            matches = df[col].astype(str).str.match(pattern) | df[col].apply(pd.isna)
            if not matches.all():
                results[col] = matches
                rows = df.loc[matches[matches.values == False].index]['ROW_NUM'].values.tolist()
                pattern_rows_list.append({"Column":col,"Row":rows})
    dtype_list=[]
    if results:
        combined_list=[]
        for col, matches in results.items():    
            combined_list.append(col)
        dict_msg = {"Pattern_Validation":combined_list}
        errorOutObj.updateDataTypeCheck("Fail","DataType Pattern Validation Issue",pattern_rows_list)
        dtype_list.append(dict_msg)
    else:        
        errorOutObj.updateDataTypeCheck("Pass","Expected data type present",[])
        dict_msg = {"Pattern_Validation":[]}
        dtype_list.append(dict_msg)
    return dtype_list

def ap_pattern_match(df):
    '''
    Function to check whether the data matches the pattern
    '''
    any_pattern = r"^[^*]*$"
    amount_pattern = r'^\s*$|^\d+(\.\d+)?$'
    date_pattern = r"^(?:(?:(?:0?[1-9]|1\d|2[0-8])-(?:0?[1-9]|1[0-2])|(?:29|30)-(?:0?[13-9]|1[0-2])|31-(?:0?[13578]|1[02]))-(?!0000)\d{4}|29-02-(?!0000)(?:\d{2}(?:04|08|[2468][048]|[13579][26])|(?:16|[2468][048]|[3579][26])00)|\s*)$"
    datetime_pattern = r"^(?:(?:(?:0?[1-9]|1\d|2[0-8])-(?:0?[1-9]|1[0-2])|(?:29|30)-(?:0?[13-9]|1[0-2])|31-(?:0?[13578]|1[02]))-(?!0000)\d{4}|29-02-(?!0000)(\d{2}(?:04|08|[2468][048]|[13579][26])|(?:16|[2468][048]|[3579][26])00))\s+([01]\d|2[0-3]):([0-5]\d)$"
    any_cols=['ACCOUNTING_DOC','GL_ACCOUNT_TYPE','COMPANY_CODE','SUPPLIER_ID','SUPPLIER_NAME','DEBIT_CREDIT_INDICATOR',
              'DOC_TYPE','DOC_TYPE_DESCRIPTION','POSTED_BY','ACCOUNT_DESCRIPTION','INVOICE_NUMBER','PAYMENT_TERMS','INVOICE_STATUS']
    # amount_cols=['AMOUNT','INVOICE_AMOUNT','PAYMENT_AMOUNT']
    amount_cols=['AMOUNT','PAYMENT_AMOUNT']
    # date_cols=['INVOICE_DATE','DUE_DATE','PAYMENT_DATE','POSTED_DATE']
    date_cols=['INVOICE_DATE','DUE_DATE','POSTED_DATE']
    datetime_cols = ["ENTERED_DATE"]
    pattern_rows_list = []
    results = {}
    for col in df.columns:
        if col in any_cols:
            pattern = any_pattern
        elif col in amount_cols:
            pattern = amount_pattern
        elif col in date_cols:
            pattern = date_pattern
        elif col in datetime_cols:
            pattern = datetime_pattern
        else:
            pattern = ""
        if pattern:
            matches = df[col].astype(str).str.match(pattern) | df[col].apply(pd.isna)
            if not matches.all():
                issue_rows =df.loc[~matches]
                capture_log_message(log_message=f" PATTERN ERROR :Pattern mismatch found in column: {col}, Row count: {issue_rows.shape[0]}",store_in_db=False)
                capture_log_message(log_message=f"PATTERN ERROR: sample Error values:{issue_rows[col].unique()[:5]}",store_in_db=False)
                results[col] = matches
                rows = df.loc[matches[matches.values == False].index][['ROW_NUM','ROW_NUM_y']].values.tolist()
                pattern_rows_list.append({"Column":col,"Row":rows})
    dtype_list =[]
    if results:
        combined_list=[]
        for col, matches in results.items():
            combined_list.append(col)
        dict_msg = {"Pattern_Validation":combined_list}
        errorOutObj.updateDataTypeCheck("Fail","DataType Pattern Validation Issue",pattern_rows_list)
        dtype_list.append(dict_msg)
    else:        
        errorOutObj.updateDataTypeCheck("Pass","Expected data type present",[])
        dict_msg = {"Pattern_Validation":[]}
        dtype_list.append(dict_msg)
    return dtype_list

def mapped_cols(defined_cols):
    '''
    Function to check if columns are mapped.
    ''' 
    map_cols_out = []
    target_map = []
    dup_col = []
    capture_log_message(log_message=f" inside mapped_cols function Defined Columns:{defined_cols}",store_in_db=False)

    for source_cols,target_cols in defined_cols:
        if len(target_cols) == 0:
            common_cols = g.english_col_dict[source_cols]
            map_cols_out.append(common_cols)
        if target_cols in target_map and target_cols!='' :
            dup_col.append(source_cols)
        target_map.append(target_cols)
    dup_map_response =dup_col
    map_response = map_cols_out
    map_cols_out_length = len(map_cols_out)   
    if map_cols_out_length==0:
        errorOutObj.updateRequiredColumnPresent("Pass",f"Missing {map_cols_out_length} columns",map_cols_out)
    else:
        #change
        errorOutObj.updateRequiredColumnPresent("Pass",f"Missing {map_cols_out_length} columns",map_cols_out)    
    return map_response,dup_map_response

  
def Logging(data,MODE_KEY):
    '''
    Logging File
    '''
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    Date = datetime.today().strftime("%d-%B-%Y")
    File_path = os.path.join(LOG_FILE_PATH,Date+"_LOG.csv")

    if MODE_KEY == 'GL':
        reqd_data = {
            "TIMESTAMP": timestamp, 
            "NULL_CHECK": data['NULL_CHECK'], 
            "UNIQUE_IDENTITY": data["UNIQUE_IDENTIFIER"],
            "DATE_CHECK" : data["DATE_CHECK"], 
            "CREDIT_DEBIT_INDICATOR_PRESENT" : data["CREDIT_DEBIT_INDICATOR_PRESENT"], 
            "DEBIT_CREDIT_BALANCE_CHECK" : data["DEBIT_CREDIT_BALANCE_CHECK"], 
            "MANUAL_ENTRY_FLAG" : data["MANUAL_ENTRY_FLAG"], 
            'FINAL_CHECK': data['FINAL_CHECK']
        }
    else:
        reqd_data = {
        "TIMESTAMP": timestamp, 
        "NULL_CHECK": data['NULL_CHECK'], 
        "UNIQUE_IDENTITY": data["UNIQUE_IDENTIFIER"],
        "DATE_CHECK" : data["DATE_CHECK"], 
        "CREDIT_DEBIT_INDICATOR_PRESENT" : data["CREDIT_DEBIT_INDICATOR_PRESENT"], 
        # "DEBIT_CREDIT_BALANCE_CHECK" : data["DEBIT_CREDIT_BALANCE_CHECK"], 
        "MANUAL_ENTRY_FLAG" : data["MANUAL_ENTRY_FLAG"], 
        # "DUE_DATE_CHECK" : data["DUE_DATE_CHECK"],
        "CONDITIONAL_NULL_CHECK":data['CONDITIONAL_NULL_CHECK'],
        'FINAL_CHECK': data['FINAL_CHECK']
        } 
    log = pd.DataFrame.from_dict(reqd_data,orient='index').T
    if(not os.path.isfile(File_path)):
        log.to_csv(File_path,index=False)
    else:
        log.to_csv(File_path,index=False,header=False,mode='a')


def get_db_data(data,MODE_KEY):
    '''
    Function where data is fetched
    '''
    df = data
    results = {}
    if MODE_KEY=='GL':
        checks_list = gl_checks_list
    else:
        checks_list = ap_checks_list
    capture_log_message(log_message='Initiating Checks')
    for check in checks_list:
        capture_log_message(log_message='Initiating {}'.format(check))
        results[check] = Get_Data(check, df)
        capture_log_message(log_message='Completed {}'.format(check))
    results = all_check(results)
    Logging(results,MODE_KEY)
    final_flag = results['FINAL_CHECK']
    return df,final_flag
