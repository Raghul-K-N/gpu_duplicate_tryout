# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 15:48:52 2021

@author: power_Lap
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timezone
from GL_Module.db_connector import MySQL_DB
# from GL_Module.logger import logger
import json
from code1.logger import capture_log_message


class Preparation:

    def __init__(self):
        """
        Some dummy variable
        """
        self.v = 1
        
    def remap(self,x, oMin, oMax, nMin, nMax ):

        #range check
        if oMin == oMax:
            capture_log_message(log_message="Warning: Zero input range")
            return None

        if nMin == nMax:
            capture_log_message(log_message="Warning: Zero output range")
            return None

        #check reversed input range
        reverseInput = False
        oldMin = min( oMin, oMax )
        oldMax = max( oMin, oMax )
        if not oldMin == oMin:
            reverseInput = True

        #check reversed output range
        reverseOutput = False   
        newMin = min( nMin, nMax )
        newMax = max( nMin, nMax )
        if not newMin == nMin :
            reverseOutput = True

        portion = (x-oldMin)*(newMax-newMin)/(oldMax-oldMin)
        if reverseInput:
            portion = (oldMax-x)*(newMax-newMin)/(oldMax-oldMin)

        result = portion + newMin
        if reverseOutput:
            result = newMax - portion

        return result
    
    
    

    def Data_Prep_for_Rules(self,df):
        """
        Data Preparation used for Rules Framework
        """        
        # # Convert Entered Date From UTC to local timezone based on User Profile TimeZone
        # capture_log_message('Find out the local time zone for each posted user using master data')
        # user_master_data_df['Time Zone'].fillna('UTC',inplace=True)
        # user_to_timezone_mapping = dict(zip(user_master_data_df['User ID'],user_master_data_df['Time Zone']))
        # df['LOCAL_TIMEZONE_USER'] = df['ENTERED_BY'].map(user_to_timezone_mapping)
        # capture_log_message(f"No of users with no timezone data:{df['LOCAL_TIMEZONE_USER'].isna().sum()}")
        # cond1=df['LOCAL_TIMEZONE_USER'].isna()
        # users_with_no_timezone = set(df[cond1]['ENTERED_BY'].values)
        # capture_log_message(f"Users with No timezone data:{users_with_no_timezone}")
        # df['LOCAL_TIMEZONE_USER'].fillna('UTC',inplace=True)
        
        # df['ENTERED_DATE'] = df.apply(lambda x:
        #     convert_datetime_to_local_timezone(date_time_value=x['ENTERED_DATE'],
        #                                date_format="%Y-%m-%d %H:%M:%S",
        #                                local_timezone=x['LOCAL_TIMEZONE_USER']), axis=1)
        
        # df['ENTERED_DATE'] = pd.to_datetime(df['ENTERED_DATE'])
        
        
        def calculate_days_difference(row):
            """This function compares entered date and posted date, select the smallest date,
            find out the quarter end date based on the smallest days and 
            calculate the days between the quarter end date and other date
            
            Args:
                row: each transaction -> pd.Series
                
            Returns:
                difference_days -> int
            """
            # Calculate the smallest date
            min_date = min(row['ENTERED_DATE'],row['POSTED_DATE'])
            # Calculate quarter & quarter end date
            quarter = (min_date.month - 1) // 3 + 1
            quarter_end_month = quarter * 3
            quarter_end_date = pd.Timestamp(year=min_date.year, month=quarter_end_month, day=1) + pd.offsets.MonthEnd(0)

            if min_date == row['ENTERED_DATE']:
                days_diff = (row['POSTED_DATE']-quarter_end_date).days
            if min_date ==row['POSTED_DATE']:
                days_diff = (row['ENTERED_DATE']-quarter_end_date).days
                
            return days_diff
        
        
        # df['DIFF_POSTING_ACCOUNTING']=df['ENTERED_DATE']-df['POSTED_DATE']
        # df['DIFF_POSTING_ACCOUNTING']=df['PARKED_DATE']-df['POSTED_DATE']
        # df['DIFF_POSTING_ACCOUNTING']=df['DIFF_POSTING_ACCOUNTING'].dt.days
        
        
        df['DIFF_POSTING_ACCOUNTING']=df.apply(lambda x: calculate_days_difference(x),axis=1)
        df["DATE_HOLIDAYS"] = df['ENTERED_DATE'].apply(lambda x :x.strftime("%Y-%m-%d"))
        df['DEBIT_CREDIT_INDICATOR'] = np.where(df['DEBIT_AMOUNT']>0,"D","C")
        df['AMOUNT'] = df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']

        df['DEBIT_AMOUNT'] = df['DEBIT_AMOUNT'].abs()
        df['CREDIT_AMOUNT'] = df['CREDIT_AMOUNT'].abs()
        df["ROUND_OFF"] = (df['AMOUNT'].abs())%10
        df['POSTED_QUARTER'] = df['ENTERED_DATE'].apply(lambda x: x.quarter)
        df['ACCOUNTING_QUARTER'] = df['POSTED_DATE'].apply(lambda x: x.quarter)
        df['TRANSACTION_DESC'] = df['TRANSACTION_DESC'].replace('',np.nan)
        df['LINE_ITEM_TEXT'] = df['LINE_ITEM_TEXT'].replace('',np.nan)
        df['SAME_QUARTER']=np.where(df['POSTED_QUARTER'] == df['ACCOUNTING_QUARTER'], 1, 0)
        
        # df['PARKED_QUARTER'] = df['ENTERED_DATE'].apply(lambda x: x.quarter)
        # df['SAME_QUARTER']=np.where(df['PARKED_QUARTER'] == df['ACCOUNTING_QUARTER'], 1, 0)
        
        return df
        
    def Data_Prep_for_AI(self,df):
        capture_log_message(log_message="Data Preparation For AI Model Started")
        
        columns = ['ACCOUNT_DESCRIPTION']

        df['DESCRIPTION'] = df[columns].apply(lambda x: '-'.join(x.values.astype(str)),axis=1)
        df['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: x.lower())

        df['DEBIT_CREDIT_INDICATOR']=np.where(df['DEBIT_AMOUNT']==0,"C","D")
        
        df['ENTERED_DATE'] = pd.to_datetime(df['ENTERED_DATE'])
        df['ENTERED_DATE_WEEKDAY'] = df['ENTERED_DATE'].dt.weekday
        df['ENTERED_DATE_HOUR']=df['ENTERED_DATE'].dt.hour
        df['ENTERED_DATE_DAY']=df['ENTERED_DATE'].dt.day
        df['ENTERED_DATE_MONTH_NUMBER']=df['ENTERED_DATE'].dt.month
        df['ENTERED_DATE_YEAR']=df['ENTERED_DATE'].dt.year
        df['ENTERED_DATE_QUARTER']=df['ENTERED_DATE'].apply(lambda x: x.quarter)

        df['POSTED_DATE'] = pd.to_datetime(df['POSTED_DATE'])
        df['POSTED_DATE_QUARTER']=df['POSTED_DATE'].apply(lambda x: x.quarter)
        df['POSTED_DATE_YEAR']=df['POSTED_DATE'].dt.year
        
        	# Convert Hour to 2 columns - Sin and Cos as given below
        df['ENTERED_DATE_HOUR_SIN'] = np.sin(2 * np.pi * df['ENTERED_DATE_HOUR']/24)
        df['ENTERED_DATE_HOUR_COS'] = np.cos(2 * np.pi * df['ENTERED_DATE_HOUR']/24)
        
        	# Ideally we should check which MONTH has how many days and accordingly divide,but for now, this approximation is ok
        df['ENTERED_DATE_DAY_SIN'] = np.sin((df['ENTERED_DATE_DAY']*2* np.pi/30))
        df['ENTERED_DATE_DAY_COS'] = np.cos((df['ENTERED_DATE_DAY']*2* np.pi/30))
        
        df['MONTH_SIN'] = np.sin(df['ENTERED_DATE_MONTH_NUMBER'] * 2 * np.pi/12)
        df['MONTH_COS'] = np.cos(df['ENTERED_DATE_MONTH_NUMBER'] * 2 * np.pi/12)
        
        # Convert SAP Amount to log amount
        df['AMOUNT']=df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']
        df['LOG_AMOUNT'] = np.log(df['AMOUNT'].abs())
        
        #  mean of each SAP_ACCOUNT for each Year's MONTH by Debit / Credit 
        df['MONTH_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'ENTERED_DATE_MONTH_NUMBER','DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        df['QTR_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'ENTERED_DATE_QUARTER','DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        df['YR_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        
        # Calculate Distance between mean and actual SAP_AMOUNT
        df['MONTH_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['MONTH_MEAN'])
        df['QTR_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['QTR_MEAN'])
        df['YR_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['YR_MEAN'])
        
        df['IS_WEEKEND'] = np.where(df['ENTERED_DATE_WEEKDAY'] < 5, 0, 1)
        
        df['DAY_DIFFERENCE_POSTED_POSTING']=(df['ENTERED_DATE']-df['POSTED_DATE']).dt.days
        
        df['SAME_YEAR'] = np.where(df['ENTERED_DATE_YEAR'] ==df['POSTED_DATE_YEAR'] , 0, 1)
        df['SAME_QUARTER'] = np.where(df['ENTERED_DATE_QUARTER'] ==df['POSTED_DATE_QUARTER'] , 0, 1)
        
        # df['ENTERED_DATE_HOLIDAY']= df['IS_POSTED_HOLIDAY']
        
        # source_cols = ['COMPANY_CODE','DOC_TYPE','POSTED_BY','ENTERED_BY','SAME_QUARTER','SAME_YEAR']
        source_cols = ['POSTED_BY','ENTERED_BY','SAME_QUARTER','SAME_YEAR']
        
        # Removing/Dropping DESCRIPTION, 'MONTH_MEAN','QTR_MEAN','YR_MEAN', Columns
        feature_engineered_cols = ['DEBIT_CREDIT_INDICATOR','DAY_DIFFERENCE_POSTED_POSTING','ENTERED_DATE_HOUR_SIN','ENTERED_DATE_HOUR_COS','ENTERED_DATE_DAY_SIN',
                                    'ENTERED_DATE_DAY_COS','MONTH_SIN','MONTH_COS','LOG_AMOUNT','MONTH_MEAN_DISTANCE',
                                    'QTR_MEAN_DISTANCE','YR_MEAN_DISTANCE','IS_WEEKEND','ENTERED_DATE_WEEKDAY','ENTERED_DATE_MONTH_NUMBER']
        
        
        all_cols = source_cols + feature_engineered_cols
        
        df = df[all_cols]

        #Filling any null values with 0
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.replace(r'^\s*$', np.nan , regex=True)
        df = df.fillna(0)
        
        cols_to_encode = ['DEBIT_CREDIT_INDICATOR','ENTERED_BY','POSTED_BY']

        df = pd.get_dummies(data=df,columns=cols_to_encode)

        return df
        
    def Data_Prep_for_Stat(self,df):
        capture_log_message(log_message="Data Preparation For stat Model Started")
        
        
        
        columns = ['ACCOUNT_DESCRIPTION']

        df['DESCRIPTION'] = df[columns].apply(lambda x: '-'.join(x.values.astype(str)),axis=1)
        df['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: x.lower())

        # df['DEBIT_CREDIT_INDICATOR']=np.where(df['DEBIT_AMOUNT']==0,"C","D")
        df['DEBIT_CREDIT_INDICATOR'] = np.where(df['DEBIT_AMOUNT'] == 0, 0, 1)
        
        df['ENTERED_DATE'] = pd.to_datetime(df['ENTERED_DATE'])
        df['POSTED_DATE'] = pd.to_datetime(df['POSTED_DATE'])
        df['ENTERED_DATE_WEEKDAY'] = df['ENTERED_DATE'].dt.weekday
        df['ENTERED_DATE_HOUR']=df['ENTERED_DATE'].dt.hour
        df['ENTERED_DATE_DAY']=df['ENTERED_DATE'].dt.day
        df['ENTERED_DATE_MONTH_NUMBER']=df['ENTERED_DATE'].dt.month
        df['ENTERED_DATE_YEAR']=df['ENTERED_DATE'].dt.year
        df['ENTERED_DATE_QUARTER']=df['ENTERED_DATE'].apply(lambda x: x.quarter)

        df['POSTED_DATE_QUARTER']=df['POSTED_DATE'].apply(lambda x: x.quarter)
        df['POSTED_DATE_YEAR']=df['POSTED_DATE'].dt.year
        	# Convert Hour to 2 columns - Sin and Cos as given below
        df['ENTERED_DATE_HOUR_SIN'] = np.sin(2 * np.pi * df['ENTERED_DATE_HOUR']/24)
        df['ENTERED_DATE_HOUR_COS'] = np.cos(2 * np.pi * df['ENTERED_DATE_HOUR']/24)
        
        	# Ideally we should check which MONTH has how many days and accordingly divide,but for now, this approximation is ok
        df['ENTERED_DATE_DAY_SIN'] = np.sin((df['ENTERED_DATE_DAY']*2* np.pi/30))
        df['ENTERED_DATE_DAY_COS'] = np.cos((df['ENTERED_DATE_DAY']*2* np.pi/30))
        
        df['MONTH_SIN'] = np.sin(df['ENTERED_DATE_MONTH_NUMBER'] * 2 * np.pi/12)
        df['MONTH_COS'] = np.cos(df['ENTERED_DATE_MONTH_NUMBER'] * 2 * np.pi/12)
        
        # Convert SAP Amount to log amount
        df['AMOUNT']=df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']
        df['LOG_AMOUNT'] = np.log(df['AMOUNT'].abs())
        
        #  mean of each SAP_ACCOUNT for each Year's MONTH by Debit / Credit 
        df['MONTH_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'ENTERED_DATE_MONTH_NUMBER','DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        df['QTR_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'ENTERED_DATE_QUARTER','DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        df['YR_MEAN'] = df.groupby(['ACCOUNT_CODE', 'ENTERED_DATE_YEAR', 'DEBIT_CREDIT_INDICATOR'])['AMOUNT'].transform("mean").round(2).astype(float)
        
        # Calculate Distance between mean and actual SAP_AMOUNT
        df['MONTH_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['MONTH_MEAN'])
        df['QTR_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['QTR_MEAN'])
        df['YR_MEAN_DISTANCE'] = abs(df['AMOUNT'] - df['YR_MEAN'])
        
        df['IS_WEEKEND'] = np.where(df['ENTERED_DATE_WEEKDAY'] < 5, 0, 1)
        
        df['DAY_DIFFERENCE_POSTED_POSTING']=(df['ENTERED_DATE']-df['POSTED_DATE']).dt.days
        
        df['SAME_YEAR'] = np.where(df['ENTERED_DATE_YEAR'] ==df['POSTED_DATE_YEAR'] , 0, 1)
        df['SAME_QUARTER'] = np.where(df['ENTERED_DATE_QUARTER'] ==df['POSTED_DATE_QUARTER'] , 0, 1)
        
        # df['ENTERED_DATE_HOLIDAY']= df['IS_POSTED_HOLIDAY']
        
        # source_cols = ['COMPANY_CODE','DOC_TYPE','POSTED_BY','ENTERED_BY','SAME_QUARTER','SAME_YEAR','ENTERED_DATE_HOLIDAY']
        # source_cols = ['COMPANY_CODE','DOC_TYPE','POSTED_BY','ENTERED_BY','SAME_QUARTER','SAME_YEAR']

        # feature_engineered_cols = ['DEBIT_CREDIT_INDICATOR','DAY_DIFFERENCE_POSTED_POSTING','DESCRIPTION','ENTERED_DATE_HOUR_SIN','ENTERED_DATE_HOUR_COS','ENTERED_DATE_DAY_SIN',
        #                             'ENTERED_DATE_DAY_COS','MONTH_SIN','MONTH_COS','LOG_AMOUNT','MONTH_MEAN','QTR_MEAN','YR_MEAN','MONTH_MEAN_DISTANCE',
        #                             'QTR_MEAN_DISTANCE','YR_MEAN_DISTANCE','IS_WEEKEND','ENTERED_DATE_WEEKDAY','ENTERED_DATE_MONTH_NUMBER']
        
        # cols_to_encode = ['DEBIT_CREDIT_INDICATOR','DESCRIPTION','DOC_TYPE','ENTERED_BY','POSTED_BY',
        #                 'COMPANY_CODE','ENTERED_DATE_WEEKDAY','ENTERED_DATE_MONTH_NUMBER']
       
        selected_columns = [
        'DEBIT_CREDIT_INDICATOR', 'DAY_DIFFERENCE_POSTED_POSTING', 'ENTERED_DATE_HOUR_SIN',
        'ENTERED_DATE_DAY_SIN', 'MONTH_SIN', 'LOG_AMOUNT', 'MONTH_MEAN', 'QTR_MEAN', 
        'YR_MEAN', 'MONTH_MEAN_DISTANCE', 'QTR_MEAN_DISTANCE', 'YR_MEAN_DISTANCE', 'IS_WEEKEND']
        
        df = df[selected_columns]

        #Filling any null values with 0
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.replace(r'^\s*$', np.nan , regex=True)
        df = df.fillna(0)

        return df


def main():
    DB = MySQL_DB('DB.json') 
    connection = DB.connect_to_database()
    start_db_read = datetime.now(timezone.utc)
    capture_log_message(log_message="Started Reading Data")
    df = pd.read_sql("""SELECT tran.TRANSACTIONID,tran.TRANSACTION_DESC,tran.ACCOUNTDOCID,tran.POSTING_TYPE,tran.ENTERED_DATE,tran.ENTERED_BY,
    tran.POSTED_DATE,tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,doc.DOCUMENT_TYPE_CODE as DOC_TYPE,com.COMPANY_CODE,
    acctdoc.ACCOUNTDOC_CODE,acc.ACCOUNT_CODE,acc.ACCOUNT_DESCRIPTION,
    fmuser.FMSUSER_CODE as POSTED_BY,
    fmuser.FMSUSER_CODE from rpttransaction tran 
    left join msfmsuser msfutr on tran.ENTERED_BY = msfutr.FMSUSERID
    left join msdocument doc on tran.DOCTYPEID=doc.DOCUMENTID
    left join rptaccountdocument acctdoc on tran.ACCOUNTDOCID=acctdoc.ACCOUNTDOCID
    left join mschartofaccounts acc on tran.ACCOUNTID=acc.ACCOUNTID
    left join mscompany com on tran.COMPANYID=com.COMPANYID
    left join msfmsuser fmuser on tran.ENTERED_BY=fmuser.FMSUSERID;""",con=connection)
    finish_db_read = datetime.now(timezone.utc)
    capture_log_message(log_message='Time Taken for Reading {shape} Dimensioned Dataframe {time}'.format(shape=df.shape,time=finish_db_read-start_db_read))
    connection.close()
    prep=Preparation()
    df=prep.Data_Prep_for_Stat(df)


if __name__ =="__main__":
    main()
