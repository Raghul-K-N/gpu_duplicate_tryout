from hashlib import new
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timezone
from math import isnan
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder
from pickle import dump
# from code1.logger import logger
from code1.logger import capture_log_message

standard_scaler = StandardScaler()
import re

class Preparation:

    def __init__(self):

        self.date_columns = ['INVOICE_DATE','DUE_DATE','PAYMENT_DATE','SYSTEM_UPDATED_DATE','POSTING_DATE',"ENTRY_DATE"]

    def discount_percent(self,i):
        """Function to Extract Discount Percentage from Axiom Payment Terms Columns """
        try:
            return float(i.replace("_","").split(" ")[0].split("%")[0])
        except:
            return 0

    def entry_type(self,i):
        inv_list = ['AB','KR','ZE','RE','KA','SA']

        if i in inv_list:
            return 'INV'
        elif i=='KG':
            return 'DM'
        else:
            return np.nan

    def discount_days(self,i):
        """Function to Extract Discount avail days from Axiom Payment Terms Columns """
        try:
            return int(i.split("NET")[0].split(" ")[1])
        except:
            return 0
    def due_days(self,i):
        """Function to Extract Due Date from Axiom Payment Terms Columns """
        try:
            return int(i.split("NET")[1].split(" ")[1])
        except:
            return 0

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

        # df[self.date_columns] = df[self.date_columns].apply(pd.to_datetime) #,errors='coerce'
        df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
        df['PAYMENT_DATE'] = pd.to_datetime(df['PAYMENT_DATE'])
        df['POSTING_DATE'] = pd.to_datetime(df['POSTING_DATE'])
        df['DUE_DATE'] = pd.to_datetime(df['DUE_DATE'])

        df["DEBIT_AMOUNT"] = df["DEBIT_AMOUNT"].fillna(0)
        df["CREDIT_AMOUNT"] = df["CREDIT_AMOUNT"].fillna(0)        
        df['AMOUNT'] = df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']
        df["AMOUNT"] = df["AMOUNT"].abs()
        # df['INVOICE_BOOKING_AMOUNT'] = np.where(((df['ACCOUNT_TYPE']=="LI") & (df['CREDIT_AMOUNT']!=0)),df.AMOUNT,np.NaN)# df['INVOICE_BOOKING'] = np.where(((df['VOUCHER_TYPE']=="IN") & (df['ACCOUNT_TYPE']=="LI") & (df['CREDIT_AMOUNT']!=0)),1,0)
        df['STRIP_INVOICE'] = df['INVOICE_NUMBER'].apply(lambda x : "".join([digit for digit in list(str(x)) if digit.isdigit()]))
        # df['INVOICE_BOOKING'] = np.where(((df['ENTRY_TYPE']=="INV") & (df['ACCOUNT_TYPE']=="LI") & (df['CREDIT_AMOUNT']!=0)),1,0)        
        # df['DISCOUNT_PERCENT'] = df['PAYMENT_TERMS'].apply(lambda x: self.discount_percent(x))
        df['DISCOUNT_PERCENT_1'] = df['DISCOUNT_PERCENTAGE_1']
        df['DISCOUNT_PERCENT_2'] = df['DISCOUNT_PERCENTAGE_2']
        df['DUE_DAYS'] = df['CREDIT_PERIOD']
        df["PURCHASE_ORDER_NUMBER"] = df["PURCHASE_ORDER_NUMBER"].replace("",np.nan)
        pattern = re.compile(r"\d{2}") 
        # df['CREDIT_PERIOD'] = df['PAYMENT_TERMS'].fillna("").apply(lambda x: pattern.findall(x)).apply(lambda x: x[1] if len(x)==2 else x[0] if len(x)==1 else 0 )
        # df['DISCOUNT_PERIOD'] = df['PAYMENT_TERMS'].fillna("").apply(lambda x: pattern.findall(x)).apply(lambda x: x[0] if len(x)==2 else 0 )
        # df['DISCOUNT_PERIOD'] = df['DISCOUNT_PERIOD'].replace(0,np.nan)  
        df["DISCOUNT_TAKEN"] = df["DISCOUNT_TAKEN"].astype(str)
        df['DISCOUNT_TAKEN'] = df['DISCOUNT_TAKEN'].apply(lambda x:  re.sub('[$,]', '',x))
        df['DISCOUNT_TAKEN'].replace('None',np.nan,inplace=True)
        #df['DISCOUNT_TAKEN'] =  df['DISCOUNT_TAKEN'].astype(float)
        df['ENTRY_TYPE'] = df['DOC_TYPE'].apply(lambda x: self.entry_type(x))
        df['PAYMENT_INVOICE_DIFFERENCE'] = ((df['PAYMENT_DATE'] - df['INVOICE_DATE'])/np.timedelta64(1, 'D')).fillna(10000).astype('int')
        # df["INVOICE_SUPPLIER_RANGE_MEAN"] = df['INVOICE_AMOUNT'].groupby(df['SUPPLIER_ID']).transform('mean')
        # df["INVOICE_SUPPLIER_RANGE_STD"] = df['INVOICE_AMOUNT'].groupby(df['SUPPLIER_ID']).transform('std')
        # df["ROUND_OFF"] = df['AMOUNT']%10
        df['TRANSACTION_DESCRIPTION'].replace('',np.nan,inplace=True)
        # df['ACCOUNT_CODE'] = df['ACCOUNT_CODE'].astype(int)
        df['INVOICE_QUARTER'] = df['INVOICE_DATE'].dt.quarter
        df['SYSTEM_POSTING_QUARTER'] = df['POSTING_DATE'].dt.quarter
        #df["DISCOUNT_PERIOD"] = df["DISCOUNT_PERIOD"].astype(float)
        df['DUE_PAYMENT_DIFFERENCE'] = ((df['DUE_DATE'] - df['PAYMENT_DATE'])/np.timedelta64(1, 'D')).fillna(0).astype('int')
        df['DISCOUNT_CHECK'] = np.where(df['PAYMENT_DATE'].isna(),0,1)
        df['UNPAID_DAYS'] = ((datetime.now() - df['DUE_DATE'])/np.timedelta64(1, 'D')).fillna(0).astype('int')
        df['MAX_DISCOUNT_PERCENT'] = df[['DISCOUNT_PERCENT_1', 'DISCOUNT_PERCENT_2']].max(axis=1)
        df['MAX_DISCOUNT_PERCENT'] = pd.to_numeric(df['MAX_DISCOUNT_PERCENT'],errors='coerce')
        df['MAX_DISCOUNT_PERIOD'] = df[['DISCOUNT_PERIOD_1', 'DISCOUNT_PERIOD_2']].max(axis=1)
        df['MAX_DISCOUNT_PERIOD'] = pd.to_numeric(df['MAX_DISCOUNT_PERIOD'],errors='coerce')
        
        return df
        
    
    def Data_Prep_for_Stat(self,df):
        capture_log_message(log_message='Data Preparation For stat Model Started')
        new_data = pd.DataFrame() 
        #################################invoice################################
        new_data['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'], format='%Y-%m-%d')
        # new_data['INVOICE_WEEKDAY']=new_data['INVOICE_DATE'].dt.weekday
        # new_data['INVOICE_DAY']=new_data['INVOICE_DATE'].dt.day
        # new_data['INVOICE_MONTH']=new_data['INVOICE_DATE'].dt.month
        # new_data['INVOICE_HOUR']=new_data['INVOICE_DATE'].dt.hour
        # new_data['INVOICE_HOUR_SIN'] = np.sin(2 * np.pi * new_data['INVOICE_HOUR']/24)
        # new_data['INVOICE_HOUR_COS'] = np.cos(2 * np.pi * new_data['INVOICE_HOUR']/24)
        # new_data['INVOICE_DAY_SIN'] = np.sin((new_data['INVOICE_DAY']*2* np.pi/30))
        # new_data['INVOICE_DAY_COS'] = np.cos((new_data['INVOICE_DAY']*2* np.pi/30))
        # new_data['INVOICE_MONTH_SIN'] = np.sin(new_data['INVOICE_MONTH'] * 2 * np.pi/12)
        # new_data['INVOICE_MONTH_COS'] = np.cos(new_data['INVOICE_MONTH'] * 2 * np.pi/12)
        # new_data['INVOICE_WEEKDAY_SIN'] = np.sin(new_data['INVOICE_WEEKDAY'] * 2 * np.pi/7)
        # new_data['INVOICE_WEEKDAY_COS'] = np.cos(new_data['INVOICE_WEEKDAY'] * 2 * np.pi/7)
        new_data['INVOICE_QUARTER'] = pd.PeriodIndex(new_data['INVOICE_DATE'], freq='Q-JAN').strftime('%q')
        new_data['INVOICE_QUARTER'] = new_data['INVOICE_QUARTER'].replace("NaT",None)
        new_data['INVOICE_QTR_SIN'] = np.sin((new_data['INVOICE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['INVOICE_QTR_COS'] = np.cos((new_data['INVOICE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        ##############################systemposting#############################
        new_data['ENTRY_DATE']=pd.to_datetime(df['ENTRY_DATE'], format='%Y-%m-%d')
        new_data['ENTRY_DATE']=new_data['ENTRY_DATE'].dt.date
        new_data['ENTRY_DATE']=pd.to_datetime(new_data['ENTRY_DATE'])
        new_data['ENTRY_WEEKDAY']=new_data['ENTRY_DATE'].dt.weekday
        new_data['ENTRY_DAY']=new_data['ENTRY_DATE'].dt.day
        new_data['ENTRY_MONTH']=new_data['ENTRY_DATE'].dt.month
        new_data['ENTRY_HOUR']=new_data['ENTRY_DATE'].dt.hour
        new_data['ENTRY_HOUR_SIN'] = np.sin(2 * np.pi * new_data['ENTRY_HOUR']/24)
        new_data['ENTRY_HOUR_COS'] = np.cos(2 * np.pi * new_data['ENTRY_HOUR']/24)
        new_data['ENTRY_DAY_SIN'] = np.sin((new_data['ENTRY_DAY']*2* np.pi/30))
        new_data['ENTRY_DAY_COS'] = np.cos((new_data['ENTRY_DAY']*2* np.pi/30))
        new_data['ENTRY_MONTH_SIN'] = np.sin(new_data['ENTRY_MONTH'] * 2 * np.pi/12)
        new_data['ENTRY_MONTH_COS'] = np.cos(new_data['ENTRY_MONTH'] * 2 * np.pi/12)
        new_data['ENTRY_WEEKDAY_SIN'] = np.sin(new_data['ENTRY_WEEKDAY'] * 2 * np.pi/7)
        new_data['ENTRY_WEEKDAY_COS'] = np.cos(new_data['ENTRY_WEEKDAY'] * 2 * np.pi/7)
        new_data['ENTRY_QUARTER'] = pd.PeriodIndex(new_data['ENTRY_DATE'], freq='Q-JAN').strftime('%q')
        new_data['ENTRY_QUARTER'] = new_data['ENTRY_QUARTER'].replace("NaT",None) 
        new_data['ENTRY_QTR_SIN'] = np.sin((new_data['ENTRY_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['ENTRY_QTR_COS'] = np.cos((new_data['ENTRY_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        #################################duedate###############################
        new_data['POSTING_DATE']=pd.to_datetime(df['POSTING_DATE'], format='%Y-%m-%d')
        new_data['POSTING_DATE']=new_data['POSTING_DATE'].dt.date
        new_data['POSTING_DATE']=pd.to_datetime(new_data['POSTING_DATE'])
        new_data['POSTED_WEEKDAY']=new_data['POSTING_DATE'].dt.weekday
        new_data['POSTED_DAY']=new_data['POSTING_DATE'].dt.day
        new_data['POSTED_MONTH']=new_data['POSTING_DATE'].dt.month
        new_data['POSTED_HOUR']=new_data['POSTING_DATE'].dt.hour
        new_data['POSTED_HOUR_SIN'] = np.sin(2 * np.pi * new_data['POSTED_HOUR']/24)
        new_data['POSTED_HOUR_COS'] = np.cos(2 * np.pi * new_data['POSTED_HOUR']/24)
        new_data['POSTED_DAY_SIN'] = np.sin((new_data['POSTED_DAY']*2* np.pi/30))
        new_data['POSTED_DAY_COS'] = np.cos((new_data['POSTED_DAY']*2* np.pi/30))
        new_data['POSTED_MONTH_SIN'] = np.sin(new_data['POSTED_MONTH'] * 2 * np.pi/12)
        new_data['POSTED_MONTH_COS'] = np.cos(new_data['POSTED_MONTH'] * 2 * np.pi/12)
        new_data['POSTED_WEEKDAY_SIN'] = np.sin(new_data['POSTED_WEEKDAY'] * 2 * np.pi/7)
        new_data['POSTED_WEEKDAY_COS'] = np.cos(new_data['POSTED_WEEKDAY'] * 2 * np.pi/7)
        new_data['POSTED_QUARTER'] = pd.PeriodIndex(new_data['POSTING_DATE'], freq='Q-JAN').strftime('%q')
        new_data['POSTED_QUARTER'] = new_data['POSTED_QUARTER'].replace("NaT",None) 
        new_data['POSTED_QTR_SIN'] = np.sin((new_data['POSTED_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['POSTED_QTR_COS'] = np.cos((new_data['POSTED_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        ############################################################################# 
        new_data['DUE_DATE']=pd.to_datetime(df['DUE_DATE'], format='%Y-%m-%d')
        new_data['DUE_DATE']=new_data['DUE_DATE'].dt.date
        new_data['DUE_DATE']=pd.to_datetime(new_data['DUE_DATE'])
        # new_data['DUE_DATE_WEEKDAY']=new_data['DUE_DATE'].dt.weekday
        # new_data['DUE_DATE_DAY']=new_data['DUE_DATE'].dt.day
        # new_data['DUE_DATE_MONTH']=new_data['DUE_DATE'].dt.month
        # new_data['DUE_DATE_HOUR']=new_data['DUE_DATE'].dt.hour
        # new_data['DUE_DATE_HOUR_SIN'] = np.sin(2 * np.pi * new_data['DUE_DATE_HOUR']/24)
        # new_data['DUE_DATE_HOUR_COS'] = np.cos(2 * np.pi * new_data['DUE_DATE_HOUR']/24)
        # new_data['DUE_DATE_DAY_SIN'] = np.sin((new_data['DUE_DATE_DAY']*2* np.pi/30))
        # new_data['DUE_DATE_DAY_COS'] = np.cos((new_data['DUE_DATE_DAY']*2* np.pi/30))
        # new_data['DUE_DATE_MONTH_SIN'] = np.sin(new_data['DUE_DATE_MONTH'] * 2 * np.pi/12)
        # new_data['DUE_DATE_MONTH_COS'] = np.cos(new_data['DUE_DATE_MONTH'] * 2 * np.pi/12)
        # new_data['DUE_DATE_WEEKDAY_SIN'] = np.sin(new_data['DUE_DATE_WEEKDAY'] * 2 * np.pi/7)
        # new_data['DUE_DATE_WEEKDAY_COS'] = np.cos(new_data['DUE_DATE_WEEKDAY'] * 2 * np.pi/7)
        new_data['DUE_DATE_QUARTER'] = pd.PeriodIndex(new_data['DUE_DATE'], freq='Q-JAN').strftime('%q')
        new_data['DUE_DATE_QUARTER'] = new_data['DUE_DATE_QUARTER'].replace("NaT",None) 
        new_data['DUE_DATE_QTR_SIN'] = np.sin((new_data['DUE_DATE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['DUE_DATE_QTR_COS'] = np.cos((new_data['DUE_DATE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        #################################paymentdate###############################
        # new_data['PAYMENT_DATE']=df['PAYMENT_DATE']
        ###################################difference##########################
        # new_data['DIFF_INVOICE_POSTING']=new_data['INVOICE_DATE']-new_data['ENTRY_DATE']
        # new_data['DIFF_INVOICE_POSTING']=new_data['DIFF_INVOICE_POSTING'].astype('timedelta64[D]')
        # new_data['DIFF_INVOICE_DUE']=new_data['DUE_DATE']-new_data['INVOICE_DATE']
        # new_data['DIFF_INVOICE_DUE']=new_data['DIFF_INVOICE_DUE'].astype('timedelta64[D]')
        # new_data['DIFF_DUE_SYSTEMPOSTING']=new_data['DUE_DATE']-new_data['ENTRY_DATE']
        # new_data['DIFF_DUE_SYSTEMPOSTING']=new_data['DIFF_DUE_SYSTEMPOSTING'].astype('timedelta64[D]')
        #######################################amount############################
        # new_data['DEBIT_CREDIT_INDICATOR']=df['DEBIT_CREDIT_INDICATOR']
        new_data['AMOUNT'] = df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']
        new_data["AMOUNT"] = new_data["AMOUNT"].abs()
        new_data['LOG_AMOUNT']=np.log2(new_data['AMOUNT'])
        
        # new_data['DEBIT_AMOUNT']=np.where(new_data.DEBIT_CREDIT_INDICATOR == "D", new_data.AMOUNT, 0)
        # new_data['CREDIT_AMOUNT']=np.where(new_data.DEBIT_CREDIT_INDICATOR == "C", new_data.AMOUNT, 0)
        new_data['DEBIT_AMOUNT']=df['DEBIT_AMOUNT'].abs()
        new_data['CREDIT_AMOUNT']=df['CREDIT_AMOUNT'].abs()
        new_data['DEBIT_CREDIT_INDICATOR']=np.where(new_data.DEBIT_AMOUNT== 0, "C","D")
        new_data['INVOICE_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('INVOICE_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_INVOICE_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['INVOICE_AMOUNT_AVERAGE_PER_DAY'])
        new_data['INVOICE_AMOUNT_SUM_PER_DAY'] = new_data.groupby('INVOICE_DATE')['AMOUNT'].transform('sum')
        new_data['LOG_INVOICE_AMOUNT_SUM_PER_DAY'] = np.log2(new_data['INVOICE_AMOUNT_SUM_PER_DAY'])
        
        new_data['ENTRY_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('ENTRY_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_ENTRY_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['ENTRY_AMOUNT_AVERAGE_PER_DAY'])
        new_data['ENTRY_AMOUNT_SUM_PER_DAY'] = new_data.groupby('ENTRY_DATE')['AMOUNT'].transform('sum')
        new_data['LOG_ENTRY_AMOUNT_SUM_PER_DAY'] = np.log2(new_data['ENTRY_AMOUNT_SUM_PER_DAY'])
        
        new_data['DUE_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('DUE_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_DUE_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['DUE_AMOUNT_AVERAGE_PER_DAY'])
        # new_data['PAYMENT_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('PAYMENT_DATE')['AMOUNT'].transform('mean')
        # new_data['LOG_PAYMENT_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['PAYMENT_AMOUNT_AVERAGE_PER_DAY'])
        new_data['POSTED_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('POSTING_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_POSTED_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['POSTED_AMOUNT_AVERAGE_PER_DAY'])
        
        # new_data['ENTRY_TYPE']=df['ENTRY_TYPE']
        # new_data['ACCOUNT_TYPE']=df['ACCOUNT_TYPE']
        
        all_columns = ['DEBIT_CREDIT_INDICATOR',            
        'INVOICE_QUARTER','INVOICE_QTR_SIN','INVOICE_QTR_COS','ENTRY_HOUR_SIN','ENTRY_HOUR_COS',
        'ENTRY_DAY_SIN','ENTRY_DAY_COS','ENTRY_MONTH_SIN','ENTRY_MONTH_COS',
        'ENTRY_WEEKDAY_SIN','ENTRY_WEEKDAY_COS','ENTRY_QUARTER','ENTRY_QTR_SIN',
        'ENTRY_QTR_COS','DUE_DATE_QUARTER',
        'DUE_DATE_QTR_SIN','DUE_DATE_QTR_COS','LOG_AMOUNT','LOG_INVOICE_AMOUNT_AVERAGE_PER_DAY',
        'LOG_INVOICE_AMOUNT_SUM_PER_DAY','LOG_ENTRY_AMOUNT_AVERAGE_PER_DAY','LOG_ENTRY_AMOUNT_SUM_PER_DAY','LOG_DUE_AMOUNT_AVERAGE_PER_DAY',
        'LOG_POSTED_AMOUNT_AVERAGE_PER_DAY'] #'ENTRY_TYPE',
        
        one_hot_encode_cols = ['DEBIT_CREDIT_INDICATOR'] #'ENTRY_TYPE',
        new_data = new_data[all_columns]
        new_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        new_data =  new_data.replace(r'^\s*$', np.nan , regex=True)
        new_data =  new_data.fillna(0)
        new_data = pd.get_dummies(data=new_data,columns=one_hot_encode_cols)
        
        # def clean_dataset(df):
        #     assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
        #     df.dropna(inplace=True)
        #     indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
        #     return df[indices_to_keep].astype(np.float64)
        # new_data=clean_dataset(new_data) 
        capture_log_message(log_message='Data Preparation For stat Model Finished')      
            
        return new_data

    def Data_Prep_for_AI(self,df):
        capture_log_message(log_message='Data Preparation For AI Model Started')
        new_data = pd.DataFrame() 
        #################################invoice################################
        new_data['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'], format='%Y-%m-%d')
        # new_data['INVOICE_WEEKDAY']=new_data['INVOICE_DATE'].dt.weekday
        # new_data['INVOICE_DAY']=new_data['INVOICE_DATE'].dt.day
        # new_data['INVOICE_MONTH']=new_data['INVOICE_DATE'].dt.month
        # new_data['INVOICE_HOUR']=new_data['INVOICE_DATE'].dt.hour
        # new_data['INVOICE_HOUR_SIN'] = np.sin(2 * np.pi * new_data['INVOICE_HOUR']/24)
        # new_data['INVOICE_HOUR_COS'] = np.cos(2 * np.pi * new_data['INVOICE_HOUR']/24)
        # new_data['INVOICE_DAY_SIN'] = np.sin((new_data['INVOICE_DAY']*2* np.pi/30))
        # new_data['INVOICE_DAY_COS'] = np.cos((new_data['INVOICE_DAY']*2* np.pi/30))
        # new_data['INVOICE_MONTH_SIN'] = np.sin(new_data['INVOICE_MONTH'] * 2 * np.pi/12)
        # new_data['INVOICE_MONTH_COS'] = np.cos(new_data['INVOICE_MONTH'] * 2 * np.pi/12)
        # new_data['INVOICE_WEEKDAY_SIN'] = np.sin(new_data['INVOICE_WEEKDAY'] * 2 * np.pi/7)
        # new_data['INVOICE_WEEKDAY_COS'] = np.cos(new_data['INVOICE_WEEKDAY'] * 2 * np.pi/7)
        new_data['INVOICE_QUARTER'] = pd.PeriodIndex(new_data['INVOICE_DATE'], freq='Q-JAN').strftime('%q')
        new_data['INVOICE_QUARTER'] = new_data['INVOICE_QUARTER'].replace("NaT",None)
        new_data['INVOICE_QTR_SIN'] = np.sin((new_data['INVOICE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['INVOICE_QTR_COS'] = np.cos((new_data['INVOICE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        ##############################systemposting#############################
        new_data['ENTRY_DATE']=pd.to_datetime(df['ENTRY_DATE'], format='%Y-%m-%d')
        new_data['ENTRY_DATE']=new_data['ENTRY_DATE'].dt.date
        new_data['ENTRY_DATE']=pd.to_datetime(new_data['ENTRY_DATE'])
        new_data['ENTRY_WEEKDAY']=new_data['ENTRY_DATE'].dt.weekday
        new_data['ENTRY_DAY']=new_data['ENTRY_DATE'].dt.day
        new_data['ENTRY_MONTH']=new_data['ENTRY_DATE'].dt.month
        new_data['ENTRY_HOUR']=new_data['ENTRY_DATE'].dt.hour
        new_data['ENTRY_HOUR_SIN'] = np.sin(2 * np.pi * new_data['ENTRY_HOUR']/24)
        new_data['ENTRY_HOUR_COS'] = np.cos(2 * np.pi * new_data['ENTRY_HOUR']/24)
        new_data['ENTRY_DAY_SIN'] = np.sin((new_data['ENTRY_DAY']*2* np.pi/30))
        new_data['ENTRY_DAY_COS'] = np.cos((new_data['ENTRY_DAY']*2* np.pi/30))
        new_data['ENTRY_MONTH_SIN'] = np.sin(new_data['ENTRY_MONTH'] * 2 * np.pi/12)
        new_data['ENTRY_MONTH_COS'] = np.cos(new_data['ENTRY_MONTH'] * 2 * np.pi/12)
        new_data['ENTRY_WEEKDAY_SIN'] = np.sin(new_data['ENTRY_WEEKDAY'] * 2 * np.pi/7)
        new_data['ENTRY_WEEKDAY_COS'] = np.cos(new_data['ENTRY_WEEKDAY'] * 2 * np.pi/7)
        new_data['ENTRY_QUARTER'] = pd.PeriodIndex(new_data['ENTRY_DATE'], freq='Q-JAN').strftime('%q')
        new_data['ENTRY_QUARTER'] = new_data['ENTRY_QUARTER'].replace("NaT",None) 
        new_data['ENTRY_QTR_SIN'] = np.sin((new_data['ENTRY_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['ENTRY_QTR_COS'] = np.cos((new_data['ENTRY_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        #################################duedate###############################
        new_data['POSTING_DATE']=pd.to_datetime(df['POSTING_DATE'], format='%Y-%m-%d')
        new_data['POSTING_DATE']=new_data['POSTING_DATE'].dt.date
        new_data['POSTING_DATE']=pd.to_datetime(new_data['POSTING_DATE'])
        new_data['POSTED_WEEKDAY']=new_data['POSTING_DATE'].dt.weekday
        new_data['POSTED_DAY']=new_data['POSTING_DATE'].dt.day
        new_data['POSTED_MONTH']=new_data['POSTING_DATE'].dt.month
        new_data['POSTED_HOUR']=new_data['POSTING_DATE'].dt.hour
        new_data['POSTED_HOUR_SIN'] = np.sin(2 * np.pi * new_data['POSTED_HOUR']/24)
        new_data['POSTED_HOUR_COS'] = np.cos(2 * np.pi * new_data['POSTED_HOUR']/24)
        new_data['POSTED_DAY_SIN'] = np.sin((new_data['POSTED_DAY']*2* np.pi/30))
        new_data['POSTED_DAY_COS'] = np.cos((new_data['POSTED_DAY']*2* np.pi/30))
        new_data['POSTED_MONTH_SIN'] = np.sin(new_data['POSTED_MONTH'] * 2 * np.pi/12)
        new_data['POSTED_MONTH_COS'] = np.cos(new_data['POSTED_MONTH'] * 2 * np.pi/12)
        new_data['POSTED_WEEKDAY_SIN'] = np.sin(new_data['POSTED_WEEKDAY'] * 2 * np.pi/7)
        new_data['POSTED_WEEKDAY_COS'] = np.cos(new_data['POSTED_WEEKDAY'] * 2 * np.pi/7)
        new_data['POSTED_QUARTER'] = pd.PeriodIndex(new_data['POSTING_DATE'], freq='Q-JAN').strftime('%q')
        new_data['POSTED_QUARTER'] = new_data['POSTED_QUARTER'].replace("NaT",None) 
        new_data['POSTED_QTR_SIN'] = np.sin((new_data['POSTED_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['POSTED_QTR_COS'] = np.cos((new_data['POSTED_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        ############################################################################# 
        new_data['DUE_DATE']=pd.to_datetime(df['DUE_DATE'], format='%Y-%m-%d')
        new_data['DUE_DATE']=new_data['DUE_DATE'].dt.date
        new_data['DUE_DATE']=pd.to_datetime(new_data['DUE_DATE'])
        # new_data['DUE_DATE_WEEKDAY']=new_data['DUE_DATE'].dt.weekday
        # new_data['DUE_DATE_DAY']=new_data['DUE_DATE'].dt.day
        # new_data['DUE_DATE_MONTH']=new_data['DUE_DATE'].dt.month
        # new_data['DUE_DATE_HOUR']=new_data['DUE_DATE'].dt.hour
        # new_data['DUE_DATE_HOUR_SIN'] = np.sin(2 * np.pi * new_data['DUE_DATE_HOUR']/24)
        # new_data['DUE_DATE_HOUR_COS'] = np.cos(2 * np.pi * new_data['DUE_DATE_HOUR']/24)
        # new_data['DUE_DATE_DAY_SIN'] = np.sin((new_data['DUE_DATE_DAY']*2* np.pi/30))
        # new_data['DUE_DATE_DAY_COS'] = np.cos((new_data['DUE_DATE_DAY']*2* np.pi/30))
        # new_data['DUE_DATE_MONTH_SIN'] = np.sin(new_data['DUE_DATE_MONTH'] * 2 * np.pi/12)
        # new_data['DUE_DATE_MONTH_COS'] = np.cos(new_data['DUE_DATE_MONTH'] * 2 * np.pi/12)
        # new_data['DUE_DATE_WEEKDAY_SIN'] = np.sin(new_data['DUE_DATE_WEEKDAY'] * 2 * np.pi/7)
        # new_data['DUE_DATE_WEEKDAY_COS'] = np.cos(new_data['DUE_DATE_WEEKDAY'] * 2 * np.pi/7)
        new_data['DUE_DATE_QUARTER'] = pd.PeriodIndex(new_data['DUE_DATE'], freq='Q-JAN').strftime('%q')
        new_data['DUE_DATE_QUARTER'] = new_data['DUE_DATE_QUARTER'].replace("NaT",None) 
        new_data['DUE_DATE_QTR_SIN'] = np.sin((new_data['DUE_DATE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        new_data['DUE_DATE_QTR_COS'] = np.cos((new_data['DUE_DATE_QUARTER'].apply(lambda x: float(x)))* 2 * np.pi/4)
        #################################paymentdate###############################
        # new_data['PAYMENT_DATE']=df['PAYMENT_DATE']
        ###################################difference##########################
        # new_data['DIFF_INVOICE_POSTING']=new_data['INVOICE_DATE']-new_data['ENTRY_DATE']
        # new_data['DIFF_INVOICE_POSTING']=new_data['DIFF_INVOICE_POSTING'].astype('timedelta64[D]')
        # new_data['DIFF_INVOICE_DUE']=new_data['DUE_DATE']-new_data['INVOICE_DATE']
        # new_data['DIFF_INVOICE_DUE']=new_data['DIFF_INVOICE_DUE'].astype('timedelta64[D]')
        # new_data['DIFF_DUE_SYSTEMPOSTING']=new_data['DUE_DATE']-new_data['ENTRY_DATE']
        # new_data['DIFF_DUE_SYSTEMPOSTING']=new_data['DIFF_DUE_SYSTEMPOSTING'].astype('timedelta64[D]')
        #######################################amount############################
        # new_data['DEBIT_CREDIT_INDICATOR']=df['DEBIT_CREDIT_INDICATOR']
        new_data['AMOUNT'] = df['DEBIT_AMOUNT']+df['CREDIT_AMOUNT']
        new_data["AMOUNT"] = new_data["AMOUNT"].abs()
        new_data['LOG_AMOUNT']=np.log2(new_data['AMOUNT'])
        
        # new_data['DEBIT_AMOUNT']=np.where(new_data.DEBIT_CREDIT_INDICATOR == "D", new_data.AMOUNT, 0)
        # new_data['CREDIT_AMOUNT']=np.where(new_data.DEBIT_CREDIT_INDICATOR == "C", new_data.AMOUNT, 0)
        new_data['DEBIT_AMOUNT']=df['DEBIT_AMOUNT'].abs()
        new_data['CREDIT_AMOUNT']=df['CREDIT_AMOUNT'].abs()
        new_data['DEBIT_CREDIT_INDICATOR']=np.where(new_data.DEBIT_AMOUNT== 0, "C","D")
        new_data['INVOICE_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('INVOICE_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_INVOICE_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['INVOICE_AMOUNT_AVERAGE_PER_DAY'])
        new_data['INVOICE_AMOUNT_SUM_PER_DAY'] = new_data.groupby('INVOICE_DATE')['AMOUNT'].transform('sum')
        new_data['LOG_INVOICE_AMOUNT_SUM_PER_DAY'] = np.log2(new_data['INVOICE_AMOUNT_SUM_PER_DAY'])
        
        new_data['ENTRY_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('ENTRY_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_ENTRY_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['ENTRY_AMOUNT_AVERAGE_PER_DAY'])
        new_data['ENTRY_AMOUNT_SUM_PER_DAY'] = new_data.groupby('ENTRY_DATE')['AMOUNT'].transform('sum')
        new_data['LOG_ENTRY_AMOUNT_SUM_PER_DAY'] = np.log2(new_data['ENTRY_AMOUNT_SUM_PER_DAY'])
        
        new_data['DUE_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('DUE_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_DUE_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['DUE_AMOUNT_AVERAGE_PER_DAY'])
        # new_data['PAYMENT_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('PAYMENT_DATE')['AMOUNT'].transform('mean')
        # new_data['LOG_PAYMENT_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['PAYMENT_AMOUNT_AVERAGE_PER_DAY'])
        new_data['POSTED_AMOUNT_AVERAGE_PER_DAY'] = new_data.groupby('POSTING_DATE')['AMOUNT'].transform('mean')
        new_data['LOG_POSTED_AMOUNT_AVERAGE_PER_DAY'] = np.log2(new_data['POSTED_AMOUNT_AVERAGE_PER_DAY'])
        
        # new_data['ENTRY_TYPE']=df['ENTRY_TYPE']
        # new_data['ACCOUNT_TYPE']=df['ACCOUNT_TYPE']
        
        all_columns = ['DEBIT_CREDIT_INDICATOR',            
        'INVOICE_QUARTER','INVOICE_QTR_SIN','INVOICE_QTR_COS','ENTRY_HOUR_SIN','ENTRY_HOUR_COS',
        'ENTRY_DAY_SIN','ENTRY_DAY_COS','ENTRY_MONTH_SIN','ENTRY_MONTH_COS',
        'ENTRY_WEEKDAY_SIN','ENTRY_WEEKDAY_COS','ENTRY_QUARTER','ENTRY_QTR_SIN',
        'ENTRY_QTR_COS','DUE_DATE_QUARTER',
        'DUE_DATE_QTR_SIN','DUE_DATE_QTR_COS','LOG_AMOUNT','LOG_INVOICE_AMOUNT_AVERAGE_PER_DAY',
        'LOG_INVOICE_AMOUNT_SUM_PER_DAY','LOG_ENTRY_AMOUNT_AVERAGE_PER_DAY','LOG_ENTRY_AMOUNT_SUM_PER_DAY','LOG_DUE_AMOUNT_AVERAGE_PER_DAY',
        'LOG_POSTED_AMOUNT_AVERAGE_PER_DAY']  #'ENTRY_TYPE',
        
        one_hot_encode_cols = ['DEBIT_CREDIT_INDICATOR']          #'ENTRY_TYPE', 
        new_data = new_data[all_columns]
        
        
        # def clean_dataset(df):
        #     assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
        #     df.dropna(inplace=True)
        #     indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
        #     return df[indices_to_keep].astype(np.float64)
        # new_data=clean_dataset(new_data)       
        new_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        new_data =  new_data.replace(r'^\s*$', np.nan , regex=True)
        new_data =  new_data.fillna(0)
        new_data = pd.get_dummies(data=new_data,columns=one_hot_encode_cols)
        capture_log_message(log_message='Data Preparation For AI Model Finished')
        
        return new_data

