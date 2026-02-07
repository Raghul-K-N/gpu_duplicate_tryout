import pandas as pd 
import numpy as np
import os
import json
import itertools
import time
# from fuzzywuzzy import fuzz
from rapidfuzz import process, fuzz
from db_connector import MySQL_DB
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from Data_Preparation import Preparation
# from code1.logger import logger
from code1.logger import capture_log_message
class Rules_Framework():
    
    def __init__(self,configurations):
        
        """
        Initializing Variables for class
        """
        self.rule_weights ={'LATE_PAYMENT':5,'UNFAVORABLE_PAYMENT_TERMS':8,'SUSPICIOUS_KEYWORD':3,'IMMEDIATE_PAYMENTS':6,'POSTING_PERIOD':6,'INVOICE_CREATED_AFTER_ACCOUNTING_DATE':6,'INVOICE_VALUE_OF_SUPPLIER':6,'UNUSUAL_VENDOR':6,'MANUAL_DEBIT_OR_CREDIT_NOTES':6,'ROUNDING_OFF':3,'TRANSACTION_TEXT_EMPTY':3,'DUPLICATE_INVOICE_POSTING':8} #'DUPLICATE_INVOICE_POSTING':8,
        
        self.rule_functions = {'LATE_PAYMENT':self.Late_Payment,'UNFAVORABLE_PAYMENT_TERMS':self.Unfavorable_Payment_Terms,'SUSPICIOUS_KEYWORD':self.Suspicious_Keywords,
                               'IMMEDIATE_PAYMENTS':self.Immediate_Payments,'POSTING_PERIOD':self.Posting_Period,'INVOICE_CREATED_AFTER_ACCOUNTING_DATE':self.late_invoice_creation,
                               'INVOICE_VALUE_OF_SUPPLIER':self.invoice_exceeds_range,'UNUSUAL_VENDOR':self.Unusual_Vendor,'MANUAL_DEBIT_OR_CREDIT_NOTES':self.manual_debit_credit_notes,
                               'ROUNDING_OFF':self.rounding_off,'TRANSACTION_TEXT_EMPTY':self.blank_je, 'DUPLICATE_INVOICE_POSTING':self.Duplicate_Invoices} #'DUPLICATE_INVOICE_POSTING':self.Duplicate_Invoices,
        
        self.suspicious_words = None 
        
        self.shorter_credit  = None
        self.immediate_payments  = None 
        self.old_unpaid_invoice = None  
        self.Date_Sequence = ['REQUISITION_DATE','TRANSPORTATION_DATE','DUE_DATE']
        self.configs = configurations
        self.config_initialization()
        self.vendor_list = ['ABC']
        self.rev_pay_types = ['W2','Y1','Y2','Y3','Y4','Y5','KG','AB','ZI','ZW','ZC','ZN','ZA','ZK','ZP','ZE']
        self.strip_list = []

    def config_initialization(self):
        """
        Function to read from Configuraitons
        """
        
        configs = dict(zip(self.configs.KEYNAME,self.configs.KEYVALUE))
        self.rule_weights = {rulename.split("WEIGHT_")[1]:float(weight) for rulename,weight in configs.items()if rulename.startswith("WEIGHT")} 
        self.suspicious_words = json.loads(configs['suspicious_words'])  
        self.date_Sequence = json.loads(configs['date_Sequence'])  
        self.shorter_credit = json.loads(configs['shorter_credit']) 
        self.immediate_payment_percent = json.loads(configs['immediate_payments'])/100
        self.unpaid_threshold_days = json.loads(configs['old_unpaid_invoice'])
        self.Date_Sequence = json.loads(configs['date_Sequence'])
        self.manual_entry_users = json.loads(configs['manual_entry_users'])
        self.vendor_list = json.loads(configs['vendor_list'])
        self.round_off = json.loads(configs['round_off'])

    def Late_Payment(self,data):
        """
        Flag payments done after Due Date
        """   
        cols=['DUE_PAYMENT_DIFFERENCE']
        # data['LATE_PAYMENT'] = np.where(data['DUE_PAYMENT_DIFFERENCE']<0))
        data['LATE_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DUE_PAYMENT_DIFFERENCE']<0),1,0))

            
    def Excess_Payment(self,data):
        """
        Flag if Payment amount exceeds invoice amount
        Done
        """
        cols=['PAYMENT_AMOUNT','INVOICE_AMOUNT','DISCOUNT_PERCENT']
        
        data['EXCESS_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['PAYMENT_AMOUNT']>data['INVOICE_AMOUNT']),1,0)) #& (data['DISCOUNT_PERCENT']<=0)

    def Unfavorable_Payment_Terms(self,data):
        """
        Flag if Cash Payments are done within short span
        Configurable number of days - Is there in Config Settings
        & Amount to be added in the Configuration
        """
        cols=['PAYMENT_TERMS','DUE_DAYS']
        
        data['UNFAVORABLE_PAYMENT_TERMS'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['PAYMENT_TERMS']=="_CASH") | (data['DUE_DAYS']<self.shorter_credit),1,0))

    def Immediate_Payments(self, data):
        """
        Flag transactions which was paid immediately when there was no discount
        The percentage configurable to be taken from configuraiton
        """
        cols=['DISCOUNT_PERCENT','PAYMENT_INVOICE_DIFFERENCE','DUE_DAYS']
        
        data['IMMEDIATE_PAYMENTS'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DISCOUNT_PERCENT']==0) & (data['PAYMENT_INVOICE_DIFFERENCE'] < data['DUE_DAYS']*self.immediate_payment_percent),1,0))

    def WorkFlow_Deviation(self, data):
        """
        Flag transactions with accounts which were only used less than 5 times in a month
        Kind of working
        """
        
        data['WORKFLOW_DEVIATION'] = np.where((~data['INVOICE_DATE'].isna()) & ((data['PURCHASE_ORDER_DATE'].isna()) | (data['REQUISITION_DATE'].isna()) | (data['TRANSPORTATION_DATE'].isna())),1,0)

    def Date_Sequential_Mismatch(self,data):
        """
        Flag transactions with JE Description column blank 
        Passing
        """
        rev_columns = self.Date_Sequence[::-1]

        k=1
        column_check_list =[]
        for i in range(0,len(rev_columns)):
            for j in range(k,len(rev_columns)):
                colname = rev_columns[i]+'_'+rev_columns[j]
                data[colname] = ((data[rev_columns[i]]-data[rev_columns[j]])/np.timedelta64(1, 'D')).fillna(10000).astype('int')
                column_check_list.append(colname)
            k=k+1
        
        data['DATE_SEQUENTIAL_MISMATCH'] = np.where((data[column_check_list]<0).any(1),1,0)
        
    def Suspicious_Keywords(self,data):
        """
        Flag is any of suspicious keywords occur in the transaction description
        Is there in Config settingss
        """
        cols=['TRANSACTION_DESCRIPTION','GL_ACCOUNT_DESCRIPTION']
        
        data['SUSPICIOUS_KEYWORD'] = 0
        for word in self.suspicious_words:
            data['SUSPICIOUS_KEYWORD'] = np.where(data[cols].isnull().any(axis=1),0,np.where(( data.TRANSACTION_DESCRIPTION.str.lower().str.contains(word) | (data.GL_ACCOUNT_DESCRIPTION.str.lower().str.contains(word))),1,0))

    def Posting_Period(self,data):
        """
        Check whether the Posting is done in the same quarter when it generated the invoice 
        Passing
        """
        cols=['INVOICE_QUARTER','SYSTEM_POSTING_QUARTER','DISCOUNT_CHECK']
        
        data['POSTING_PERIOD'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['INVOICE_QUARTER']!=data['SYSTEM_POSTING_QUARTER']) & (data['DISCOUNT_CHECK']==0),1,0))

    def Late_Payment(self,data):
        """
        Flag payments done after Due Date
        """
        cols=['DUE_PAYMENT_DIFFERENCE','DISCOUNT_PERCENT']
        data['LATE_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DUE_PAYMENT_DIFFERENCE']<0),1,0))
        # data['LATE_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DUE_PAYMENT_DIFFERENCE']<0) & (data['DISCOUNT_CHECK']==1),1,0))
        
    def Unusual_Vendor(self,data):
        """
        Flag transactions where an unknown vendor is there 
        Vendor list required from UI
        """
        cols=['SUPPLIER_ID']
        
        data['UNUSUAL_VENDOR'] = np.where(data['SUPPLIER_ID'].isnull(),0,np.where(data['SUPPLIER_ID'].isin(self.vendor_list),1,0))
    
    def blank_je(self,data):
        """
        Flag transactions where transaction text is empty
        """
        cols=['TRANSACTION_DESCRIPTION']
        data['TRANSACTION_TEXT_EMPTY'] = np.where(data[cols].isnull(),1,0)
    

    def rounding_off(self,data):
        """
        Last 3 digits: This entry’s monetary value does not end in “0.00” or “9.99”

        """
        data["ROUNDING_OFF"] = np.where(data['AMOUNT'].isnull(),0,np.where(data["ROUND_OFF"].isin(self.round_off),1,0))
    
    def late_invoice_creation(self,data):
        """
        The Invoice date from the supplier is after the accounting date

        """
        data["INVOICE_CREATED_AFTER_ACCOUNTING_DATE"] = np.where(data['INVOICE_DATE'].isnull(),0,np.where((data['INVOICE_DATE']>data['POSTING_DATE']),1,0))
    
    def invoice_exceeds_range(self,data):
        """
        Invoice amount of supplier exceeds normal range of the supplier

        """
        data["INVOICE_VALUE_OF_SUPPLIER"] = np.where(data['INVOICE_AMOUNT'].isnull(),0,np.where((data['INVOICE_AMOUNT']>(data["INVOICE_SUPPLIER_RANGE_MEAN"]+3*(data["INVOICE_SUPPLIER_RANGE_STD"]))),1,0))

    def manual_debit_credit_notes(self,data):
        """
        Manually posted entry for debit or credit notes then we can flag the transaction
        """
        data["MANUAL_DEBIT_OR_CREDIT_NOTES"] = np.where(data['ENTRY_TYPE'].isnull(),0,np.where((data['ENTRY_TYPE']=="DM") | (data['ENTRY_TYPE']=="CM") &  (data['POSTED_BY'].isin(self.manual_entry_users)),1,0))


    def Old_Unpaid_Invoice(self,data):
        """
        Flag Invoives which are not paid n days after DUE Date
        n-number of days configuraiton is required
        """
        cols = ['UNPAID_DAYS','INVOICE_NUMBER']
        
        total_invoices = list(data['INVOICE_NUMBER'].unique())
        paid_invoices = list(data[~data['PAYMENT_DATE'].isna()]['INVOICE_NUMBER'].unique())
        unpaid_invoices = set(total_invoices).difference(paid_invoices)
    
        data['OLD_UNPAID_INVOICE'] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['UNPAID_DAYS']>self.unpaid_threshold_days) & (data['INVOICE_NUMBER'].isin(unpaid_invoices))),1,0))

    def similarity_check(self,df,check_column,grouping_column,which_one):
        
        col_name = 'DUPLICATES_'+str(which_one)
        
        df[col_name] = ""
        
        subset_df = df[df[check_column]==1]
        
        for distinct_val in set(subset_df[grouping_column]):
            subset = subset_df[subset_df[grouping_column]==distinct_val]
            for pair in itertools.combinations(set(list(subset['INVOICE_NUMBER'])),2):
                match = fuzz.ratio(pair[0],pair[1])
                if match>60:
                    df.loc[df.INVOICE_NUMBER == pair[0], col_name] =  ",".join(df.loc[df.INVOICE_NUMBER==pair[0]]['INVOICE_ID_COPY'].to_list())
                    df.loc[df.INVOICE_NUMBER == pair[1], col_name] =  ",".join(df.loc[df.INVOICE_NUMBER==pair[1]]['INVOICE_ID_COPY'].to_list())

                else:
                    continue

        df['DUPLICATE_INV_'+str(which_one)] = np.where(~(df[col_name]==""),1,0)

    def Duplicate_Invoices(self,df):
        """
        Flag Duplicate Invoices
        """
        cols =['DUPLICATE_INV_1','DUPLICATE_INV_2','DUPLICATE_INV_3','DUPLICATE_INV_4','DUPLICATE_INV_5'] #,'DUPLICATE_INV_7','DUPLICATE_INV_8'
        duplicate_cols = ["DUPLICATES_"+column.strip("_")[-1] for column in cols ]
        
        df['INVOICE_DATE'] = df['INVOICE_DATE'].astype(str)
        df['INVOICE_ID_COPY']=df['INVOICE_ID'].astype(str)
        
        df['DUPLICATE_INV_1'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE','INVOICE_AMOUNT'])['INVOICE_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_1'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_2'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE'])['INVOICE_AMOUNT'].transform('nunique')>1,1,0)
        df['DUPLICATES_2'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_3'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_AMOUNT'])['INVOICE_DATE'].transform('nunique')>1,1,0)
        df['DUPLICATES_3'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_4'] = np.where(df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT','INVOICE_DATE'])['SUPPLIER_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_4'] =df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT','INVOICE_DATE'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_5'] = np.where(df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT'])['SUPPLIER_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_5'] =df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        # print("starting DUPLICATE_INV_6_CHECK")
        # df['DUPLICATE_INV_6_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','INVOICE_DATE','SUPPLIER_ID'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DUPLICATE_INV_6_CHECK','SUPPLIER_ID',6)

        # print("starting DUPLICATE_INV_7_CHECK")
        # df['DIFF_SUPPLIER_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','INVOICE_DATE'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DIFF_SUPPLIER_CHECK','INVOICE_DATE',7)
        
        # print("starting DUPLICATE_INV_8_CHECK")
        # df['DIFF_DATE_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','SUPPLIER_ID'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DIFF_DATE_CHECK','SUPPLIER_ID',8)
        
        df['DUPLICATE_INVOICES_MAPPING'] = df[duplicate_cols].apply(lambda x: ','.join(x),axis=1).apply(lambda x: ','.join(pd.unique(x.split(','))))
        df['DUPLICATE_INVOICES_MAPPING'] = df['DUPLICATE_INVOICES_MAPPING'].apply(lambda x : ",".join(x.split(",")[1:]))
        df['DUPLICATE_INVOICES_MAPPING'] = np.where(df.DUPLICATE_INVOICES_MAPPING==",","",df.DUPLICATE_INVOICES_MAPPING)
        df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
        df['DUPLICATE_INVOICE_POSTING'] = np.where((df[cols].sum(axis=1)>0) & ~(df['STRIP_INVOICE'].isin(self.strip_list)),1,0)

    def Rule_Scores_Calculation(self,data):
        """
        Function to calculate the Rule Score
        """
        data['RULES_RISK_SCORE_RAW'] = 0
        for rulename,weight in self.rule_weights.items():
            data['RULES_RISK_SCORE_RAW']+= data[rulename]*weight

        data['RULES_RISK_SCORE'] = data['RULES_RISK_SCORE_RAW']/data['RULES_RISK_SCORE_RAW'].max()

        rule_name_cols = [rulename+"_NAME" for rulename,weight in self.rule_weights.items()]
        rule_dict = {rule : rule+"_NAME" for rule,weight in self.rule_weights.items()}
        
        for col in rule_name_cols:
         data[col]=""
        for rule,rulename in rule_dict.items():
            data.loc[(data[rule] > 0), rulename] = rule
        
        data['CONTROL_DEVIATION'] = data[rule_name_cols].values.tolist()
        data['CONTROL_DEVIATION'] = data['CONTROL_DEVIATION'].apply(lambda x: [i for i in x if i != ''])
        data['CONTROL_DEVIATION'] = data['CONTROL_DEVIATION'].apply(lambda x: ', '.join(x))

        for col in rule_name_cols:
            del data[col]
        
        return data

    def Rule_Score_Calculation_AccountDOC(self,data):

        index_col = ['ACCOUNT_DOC_ID']
        rule_cols = [rulename for rulename,weight in self.rule_weights.items()]
        agg_cols = [(rulename,['max']) for rulename,weight in self.rule_weights.items()]
        rules_accountdoc = data.groupby('ACCOUNT_DOC_ID',as_index=False).agg(OrderedDict(agg_cols))
        rules_accountdoc.columns = index_col+rule_cols

        return self.Rule_Scores_Calculation(rules_accountdoc)

    def Run_Rules(self,data):
        
        Prep = Preparation()
        data = Prep.Data_Prep_for_Rules(data)

        reversal_pay_types1 = data[(data['ENTRY_TYPE'].isin(self.rev_pay_types)) & (data['INVOICE_AMOUNT']>0)]['STRIP_INVOICE'].to_list()
        reversal_pay_types2 = data[~(data['ENTRY_TYPE'].isin(self.rev_pay_types)) & (data['INVOICE_AMOUNT']>0)]['STRIP_INVOICE'].to_list()
        self.strip_list = reversal_pay_types1+reversal_pay_types2

        data = data[~(data['ENTRY_TYPE'].isin(self.rev_pay_types)) | (data['INVOICE_AMOUNT']<0)]
        for rule in self.rule_weights.keys():
            self.rule_functions[rule](data)
        data = self.Rule_Scores_Calculation(data)
     
        id_column = ['TRANSACTION_ID','ACCOUNT_DOC_ID']
        rule_cols = [rulename for rulename,weight in self.rule_weights.items()]
        scores_column = ['CONTROL_DEVIATION','RULES_RISK_SCORE_RAW','RULES_RISK_SCORE']
        

        data = data[id_column+rule_cols+scores_column]
        
        return data

def main():

    DB = MySQL_DB('DB.json')
    connection = DB.connect_to_database()
    start_db_read = datetime.now(timezone.utc)
    df= pd.read_sql("""select tran.TRANSACTION_ID,tran.ENTERED_BY,doc.ENTRY_ID,tran.ACCOUNT_DOC_ID,tran.INVOICE_ID,tran.TRANSACTION_DESCRIPTION,tran.GL_ACCOUNT_DESCRIPTION,
    tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.PAYMENT_DATE,
    tran.ACCOUNT_TYPE,tran.POSTED_BY,tran.POSTING_DATE,tran.ENTRY_DATE,tran.SYSTEM_UPDATED_DATE,tran.DUE_DATE,tran.PAYMENT_TERMS,  tran.INVOICE_NUMBER,tran.SUPPLIER_ID,tran.INVOICE_AMOUNT,tran.INVOICE_DATE,tran.CREDIT_PERIOD,tran.TRANSACTION_CODE,comp.COMPANY_CODE,vend.VENDORCODE,loc.LOCATION_CODE,entry.ENTRY_TYPE as ENTRY_TYPE
    from ap_transaction tran
    left join msentrytype entry on tran.ENTRY_TYPE_ID=entry.ENTRY_TYPE_ID
    left join mscompany comp  on tran.COMPANY_ID=comp.COMPANYID
    left join ap_vendorlist vend  on tran.VENDORID=vend.VENDORID
    left join mslocation loc on tran.LOCATION_ID=loc.LOCATIONID
    left join ap_accountdocuments doc on tran.ACCOUNT_DOC_ID=doc.ACCOUNT_DOC_ID limit 1000;""",connection);
    configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='apframework' and STATUS=1",con=connection)
    finish_db_read = datetime.now(timezone.utc)
    capture_log_message(log_message="Time Taken for Reading {shape} Dimensioned Dataframe {time}".format(shape=df.shape,time=finish_db_read-start_db_read))
    connection.close()

    Rules = Rules_Framework(configurations)

    Rules_scored = Rules.Run_Rules(df)


if __name__ == "__main__":
    start_time = time.time()
    main()
    capture_log_message(log_message="Time Taken for Running Rules {time}".format(time=time.time() - start_time))
    
