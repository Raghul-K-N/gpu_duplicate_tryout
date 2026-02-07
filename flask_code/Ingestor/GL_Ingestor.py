from datetime import datetime, timezone
import pandas as pd
import numpy as np
import asyncio
from flask import g
from Ingestor.db_connector import MySQL_DB
# from GL_Module.logger import logger
# from code1.logger import logger as main_logger
from code1.logger import capture_log_message, process_data_for_sending_internal_mail, update_current_module
import utils
import traceback
from Ingestor.fetch_data import process_batch_data
from pipeline_data import PipelineData

class GL_Ingestor:
    
    def __init__(self,dbjson, audit_id, client_id):
        """
        Initializing Database Connection
        """        
        capture_log_message(log_message="Initializing GL Ingestor")
        self.DB = MySQL_DB(dbjson)
        self.audit_id = audit_id
        self.client_id = client_id

    def preprocess_ingest_data(self, ingest_data):
       g.src_gl_cols = ingest_data.columns
       capture_log_message(log_message=f"Src_GL_cols:{g.src_gl_cols}")


       extra_columns = ['TRANSACTION_ID_GA', 'ACCOUNTING_DOC', 'TRANSACTION_DESCRIPTION', 'DOC_TYPE', 'DOC_TYPE_DESCRIPTION',
       'AMOUNT', 'DEBIT_CREDIT_INDICATOR', 'SAP_ACCOUNT', 
       'ACCOUNT_DESCRIPTION', 'SAP_P01_ACCOUNT_DESCRIPTION', 'SAP_P02_ACCOUNT_DESCRIPTION',
       'SAP_P03_ACCOUNT_DESCRIPTION', 'SAP_P04_ACCOUNT_DESCRIPTION', 'SAP_P05_ACCOUNT_DESCRIPTION',
       'POSTED_DATE', 'POSTED_BY',
       'POSTED_BY_POSTION', 'POSTED_BY_DEPARTMENT', 'POSTED_LOCATION', 'ENTERED_DATE', 'ENTERED_BY',
       'ENTERED_BY_POSTION',  'ENTERED_BY_DEPARTMENT', 
       'ENTERED_LOCATION', 'SAP_COMPANY', 'IS_REVERSED',
       'POSTED_BY_USER_TYPE', 'POSTED_BY_ORG_CHART_UNIT',
       'POSTED_BY_FUNCTION','HOLIDAY','LEDGER' 
       'COST_CENTER','WBS_ELEMENT','PARKED_DATE','PARKED_BY','MATERIAL_NUMBER','HEADER_TEXT','LINE_ITEM_TEXT']
       #'SRC_SID', 'CREATED_DATE','MODIFIED_DATE','LEDGER'
      
       for col in extra_columns:
           if col not in ingest_data.columns:
               ingest_data[col] = pd.NA

    # #    ingest_data['GL_ACCOUNT_TYPE'] = ingest_data['GL_ACCOUNT_TYPE'].fillna("").astype(str)
    #    ingest_data['ENTERED_BY'] = ingest_data['ENTERED_BY'].fillna("").astype(str)
    #    ingest_data['POSTED_BY'] = ingest_data['POSTED_BY'].fillna("").astype(str)
    # #    ingest_data['COMPANY_NAME'] = ingest_data['COMPANY_NAME'].fillna("").astype(str)
    #    ingest_data['POSTED_LOCATION'] = ingest_data['POSTED_LOCATION'].fillna("").astype(str)
    # #    ingest_data['SUPPLIER_ID'] = ingest_data['SUPPLIER_ID'].fillna("").astype(str)
    #    ingest_data['ACCOUNTING_DOC'] = ingest_data['ACCOUNTING_DOC'].fillna("").astype(str)
    #    ingest_data['INVOICE_NUMBER'] = ingest_data['INVOICE_NUMBER'].fillna("").astype(str)
       ingest_data['SAP_ACCOUNT'] = ingest_data['SAP_ACCOUNT'].astype(object).fillna("").astype(str)
       ingest_data['SAP_COMPANY'] = ingest_data['SAP_COMPANY'].astype(object).fillna("").astype(str)
       ingest_data['ENTERED_BY'] = ingest_data['ENTERED_BY'].astype(object).fillna("NA").astype(str)
       ingest_data['POSTED_BY'] = ingest_data['POSTED_BY'].astype(object).fillna("NA").astype(str)
       ingest_data['ENTERED_LOCATION'] = ingest_data['ENTERED_LOCATION'].astype(object).fillna("").astype(str)
       ingest_data['POSTED_LOCATION'] = ingest_data['POSTED_LOCATION'].astype(object).fillna("").astype(str)
       ingest_data['audit_id'] = g.audit_id
       return ingest_data 

    def mscompany(self,transactions):
        """
        Function to populate mscompany table
        """
        columns = ['COMPANY_CODE']
        capture_log_message(log_message="Ingesting MSCOMPANY")

        current_table = self.DB.read_table('mscompany',columns,client_id=self.client_id)
        new_table     = pd.DataFrame(transactions['SAP_COMPANY'].unique(),columns=['COMPANY_CODE'])

        final_table   = new_table.loc[~new_table['COMPANY_CODE'].isin(current_table['COMPANY_CODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['COMPANY_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message="Number of records to ingest "+str(final_table.shape))

        self.DB.upload_data_to_database(final_table,'mscompany')
    
    def msfmsuser(self,transactions):
        """
        Function to populate msfmsuser table
        """
        columns           = ['FMSUSER_CODE','FMSUSER_DEPARTMENT','FMSUSER_POSITION','FMSUSER_LOCATION']
        entered_by_rename = {"ENTERED_BY":"FMSUSER_CODE","ENTERED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT","ENTERED_BY_POSTION":"FMSUSER_POSITION",'ENTERED_LOCATION':"FMSUSER_LOCATION"}
        posted_by_rename  = {"POSTED_BY":"FMSUSER_CODE","POSTED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT","POSTED_BY_POSTION":"FMSUSER_POSITION",'POSTED_LOCATION':"FMSUSER_LOCATION",'POSTED_BY_FUNCTION':'FMSUSER_FUNCTION','POSTED_BY_ORG_CHART_UNIT':'FMES_ORG_CHART_UNIT','POSTED_BY_USER_TYPE':'FMES_USER_TYPE'}##'POSTED_BY_USER_TYPE'
        capture_log_message(log_message="Ingesting MSFMSUSER")

        current_table     = self.DB.read_table('msfmsuser',columns,client_id=self.client_id)
        transactions_copy = transactions.copy()
        transactions_copy['ENTERED_BY_POSTION'] = transactions_copy['ENTERED_BY_POSTION'].fillna("NA")
        transactions_copy['ENTERED_BY_DEPARTMENT'] = transactions_copy['ENTERED_BY_DEPARTMENT'].fillna("NA")
        transactions_copy['ENTERED_LOCATION'] = transactions_copy['ENTERED_LOCATION'].fillna("NA")
        transactions_copy['POSTED_BY_POSTION'] = transactions_copy['ENTERED_BY_POSTION'].fillna("NA")
        transactions_copy['POSTED_BY_DEPARTMENT'] = transactions_copy['ENTERED_BY_DEPARTMENT'].fillna("NA")
        transactions_copy['POSTED_LOCATION'] = transactions_copy['ENTERED_LOCATION'].fillna("NA")
        transactions_copy['POSTED_BY_FUNCTION'] = transactions_copy['POSTED_BY_FUNCTION'].fillna("NA")
        transactions_copy['POSTED_BY_ORG_CHART_UNIT'] = transactions_copy['POSTED_BY_ORG_CHART_UNIT'].fillna("NA")
        transactions_copy['POSTED_BY_USER_TYPE'] = transactions_copy['POSTED_BY_USER_TYPE'].fillna("NA")
        transactions_copy.fillna("NA", inplace = True)
        new_table_entered = transactions_copy.groupby('ENTERED_BY').agg({'ENTERED_BY_POSTION':'max','ENTERED_BY_DEPARTMENT':'last','ENTERED_LOCATION':'last'}).reset_index().rename(columns=entered_by_rename)
        new_table_posted  = transactions_copy.groupby('POSTED_BY').agg({'POSTED_BY_POSTION':'max','POSTED_BY_DEPARTMENT':'last','POSTED_LOCATION':'last','POSTED_BY_FUNCTION':'last','POSTED_BY_ORG_CHART_UNIT':'last','POSTED_BY_USER_TYPE':'last'}).reset_index().rename(columns=posted_by_rename)
        new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['FMSUSER_CODE']).reset_index().drop(columns=['index'])  

        
        final_table       = new_table.loc[~new_table['FMSUSER_CODE'].isin(current_table['FMSUSER_CODE'])].copy()
        final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table       = final_table.drop_duplicates(subset=['FMSUSER_CODE'])
        final_table.dropna(axis=0,subset=['FMSUSER_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message="Number of records to ingest "+str(final_table.shape)
                            )

        self.DB.upload_data_to_database(final_table,'msfmsuser')

    def mslocation(self,transactions):
        """
        Function to populate mslocation table
        """
        columns = ['LOCATION_CODE']
        capture_log_message(log_message="Ingesting MSLOCATION")
    
        current_table     = self.DB.read_table('mslocation',columns,client_id=self.client_id)
        new_table_entered = pd.DataFrame(transactions['ENTERED_LOCATION'].unique(),columns=['LOCATION_CODE'])
        new_table_posted  = pd.DataFrame(transactions['POSTED_LOCATION'].unique(),columns=['LOCATION_CODE'])
        new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['LOCATION_CODE'])

        final_table       = new_table.loc[~new_table['LOCATION_CODE'].isin(current_table['LOCATION_CODE'])].copy()
        final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['LOCATION_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message="Number of records to ingest "+str(final_table.shape)
                            )

        self.DB.upload_data_to_database(final_table,'mslocation')

    def mschartofaccounts(self,transactions):
        """
        Function to populate mschartofaccounts table
        """
        columns     =['ACCOUNT_CODE','ACCOUNT_DESCRIPTION','P01_ACCOUNT_DESCRIPTION','P02_ACCOUNT_DESCRIPTION','P03_ACCOUNT_DESCRIPTION','P04_ACCOUNT_DESCRIPTION','P05_ACCOUNT_DESCRIPTION']

        rename_cols = {'SAP_ACCOUNT':'ACCOUNT_CODE','ACCOUNT_DESCRIPTION':'ACCOUNT_DESCRIPTION','SAP_P01_ACCOUNT_DESCRIPTION':'P01_ACCOUNT_DESCRIPTION',
        'SAP_P02_ACCOUNT_DESCRIPTION':'P02_ACCOUNT_DESCRIPTION','SAP_P03_ACCOUNT_DESCRIPTION':'P03_ACCOUNT_DESCRIPTION',
        'SAP_P04_ACCOUNT_DESCRIPTION':'P04_ACCOUNT_DESCRIPTION','SAP_P05_ACCOUNT_DESCRIPTION':'P05_ACCOUNT_DESCRIPTION'}

        capture_log_message(log_message="Ingesting MSCHARTOFACCOUNTS")
        current_table = self.DB.read_table('mschartofaccounts',columns,client_id=self.client_id)
        new_table     = transactions.groupby('SAP_ACCOUNT').agg({'ACCOUNT_DESCRIPTION':'last','SAP_P01_ACCOUNT_DESCRIPTION':'last','SAP_P02_ACCOUNT_DESCRIPTION':'last',
        'SAP_P03_ACCOUNT_DESCRIPTION':'last','SAP_P04_ACCOUNT_DESCRIPTION':'last','SAP_P05_ACCOUNT_DESCRIPTION':'last'}).reset_index().rename(columns=rename_cols)

        final_table   = new_table.loc[~new_table['ACCOUNT_CODE'].isin(current_table['ACCOUNT_CODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['ACCOUNT_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message="Number of records to ingest "+str(final_table.shape))

        self.DB.upload_data_to_database(final_table,'mschartofaccounts')

    def msdocumenttype(self,transactions):
        """
        Function to populate msdocumenttype table
        """
        columns = ['DOCUMENT_TYPE_CODE','DOCUMENT_TYPE_DESCRIPTION']
        rename_cols = {"DOC_TYPE":"DOCUMENT_TYPE_CODE","DOC_TYPE_DESCRIPTION":'DOCUMENT_TYPE_DESCRIPTION'}
        
        capture_log_message(log_message="Ingesting MSDOCUMENTYPE")

        current_table = self.DB.read_table('msdocumenttype',columns)
        new_table     = transactions.groupby('DOC_TYPE').agg({"DOC_TYPE_DESCRIPTION":'last'}).reset_index().rename(columns=rename_cols)

        final_table   = new_table.loc[~new_table['DOCUMENT_TYPE_CODE'].isin(current_table['DOCUMENT_TYPE_CODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['DOCUMENT_TYPE_CODE'],inplace=True)

        capture_log_message(log_message="Number of records to ingest "+str(final_table.shape))

        self.DB.upload_data_to_database(final_table,'msdocumenttype')

    def rptaccountdocument(self,transactions):
        """
        Function to populate rptaccountdocument table
        """
        capture_log_message(log_message="Ingesting RPTACCOUNTDOCUMENT")
        
        existing_table = self.DB.read_table(f'rptaccountdocument_{self.audit_id}',columns=['ACCOUNTDOC_CODE'],audit_id=self.audit_id)

        # rptaccdoc      = transactions.groupby('ACCOUNTDOC_CODE').agg({'COMPANYID':'last','DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum',
        #                                           'POSTED_BY':'last','POSTED_DATE':'last','POSTED_LOCATION':'last',
        #                                           'DOCUMENTID':'last', 'audit_id':'last'}).reset_index()
        rptaccdoc      = transactions.groupby('ACCOUNTDOC_CODE').agg({'audit_id':'last','ACCOUNTDOCID':'first'}).reset_index()
        
        rptaccdoc.rename(columns={"COMPANYID":"COMPANY_CODE"},inplace=True)

        capture_log_message(log_message="Number of records to ingest "+str(rptaccdoc.shape))
        final_table    = rptaccdoc.loc[~rptaccdoc['ACCOUNTDOC_CODE'].isin(existing_table['ACCOUNTDOC_CODE'])].copy()
        final_table['ACCOUNTDOC_CODE'] = final_table['ACCOUNTDOC_CODE'].astype(str).str.split('-').str[0]
        capture_log_message(log_message="Number of records to ingest after duplicate removal "+str(final_table.shape))
        self.DB.upload_data_to_database(final_table,f'rptaccountdocument_{self.audit_id}')
        capture_log_message(log_message='Insertion to rptAccountdoc Finished')
    
    def rpttransactions(self,transactions):
    
        doctypes = self.DB.read_table('msdocumenttype',columns=['DOCUMENTID','DOCUMENT_TYPE_CODE'])
        location = self.DB.read_table('mslocation',columns=['LOCATIONID','LOCATION_CODE'],client_id=self.client_id)
        coa      = self.DB.read_table('mschartofaccounts',columns=['ACCOUNTID','ACCOUNT_CODE'],client_id=self.client_id)
        user     = self.DB.read_table('msfmsuser',columns=['FMSUSERID','FMSUSER_CODE'],client_id=self.client_id)
        company  = self.DB.read_table('mscompany',columns=['COMPANYID','COMPANY_CODE'],client_id=self.client_id)

        doctypes = dict(zip(doctypes.DOCUMENT_TYPE_CODE,doctypes.DOCUMENTID))
        location = dict(zip(location.LOCATION_CODE,location.LOCATIONID))
        coa      = dict(zip(coa.ACCOUNT_CODE,coa.ACCOUNTID))
        user     = dict(zip(user.FMSUSER_CODE,user.FMSUSERID))
        company  = dict(zip(company.COMPANY_CODE,company.COMPANYID))

        transactions['DOCUMENTID']        = transactions['DOC_TYPE'].map(doctypes)
        # transactions['DOCTYPEID']        = transactions['DOCUMENTID']
        # transactions['DOCUMENT_TYPE_CODE'] = transactions['DOC_TYPE']
        transactions['DESCRIPTION']        = transactions['ACCOUNT_DESCRIPTION']
        transactions['LOCATION_CODE']     = transactions['POSTED_LOCATION']
        transactions['POSTED_LOCATION']   = transactions['POSTED_LOCATION'].map(location)
        transactions['ENTERED_LOCATION']  = transactions['ENTERED_LOCATION'].map(location)
        transactions['ACCOUNTID']         = transactions['SAP_ACCOUNT'].map(coa)
        transactions['ACCOUNT_CODE']      = transactions['SAP_ACCOUNT']
        transactions['ENTERED_BY_USERID'] = transactions['ENTERED_BY'].map(user)
        transactions['POSTED_BY_USERID']  = transactions['POSTED_BY'].map(user)
        transactions['ENTERED_BY_NAME']   = transactions['ENTERED_BY']
        transactions['POSTED_BY_NAME']    = transactions['POSTED_BY']
        transactions['COMPANYID']         = transactions['SAP_COMPANY'].map(company)
        transactions['COMPANY_CODE']      = transactions['SAP_COMPANY']
        transactions['DEBIT_AMOUNT']      = np.where(transactions.DEBIT_CREDIT_INDICATOR=="D",transactions.AMOUNT,0)
        transactions['CREDIT_AMOUNT']     = np.where(transactions.DEBIT_CREDIT_INDICATOR=="C",transactions.AMOUNT,0)
        
        #renames
        transactions = transactions.rename(columns={"TRANSACTION_ID_GA":"TRANSACTION_CODE","TRANSACTION_DESCRIPTION":"TRANSACTION_DESC",
                                                    "ACCOUNTING_DOC":"ACCOUNTDOC_CODE",'POSTING_DATE':'POSTED_DATE','HOLIDAY':'IS_POSTED_HOLIDAY'})


        # required_cols = ['audit_id','TRANSACTION_CODE','TRANSACTION_DESC','ACCOUNTDOC_CODE','DEBIT_AMOUNT','CREDIT_AMOUNT',
        #                 'ACCOUNTID','DOCUMENTID','COMPANYID','ENTERED_DATE','ENTERED_BY','ENTERED_LOCATION','POSTED_DATE','POSTED_BY','POSTED_LOCATION',
        #                 'IS_REVERSED','IS_REVERSAL','IS_POSTED_HOLIDAY']
        
        

        self.rptaccountdocument(transactions)

        # accountdoc = self.DB.read_table(f'rptaccountdocument_{self.audit_id}',columns=['ACCOUNTDOCID','ACCOUNTDOC_CODE'],audit_id=self.audit_id)
        # accountdoc = dict(zip(accountdoc.ACCOUNTDOC_CODE,accountdoc.ACCOUNTDOCID))

        # transactions['ACCOUNTDOCID'] = transactions['ACCOUNTDOC_CODE'].map(accountdoc)

        # transactions.rename(columns={'DOCUMENTID':'DOCTYPEID'},inplace=True)

        # transactions.drop(['POSTED_LOCATION'],axis=1,inplace=True)
        

        rpttransaction_df = transactions.copy()
        pipe_obj = PipelineData()
        pipe_obj.set_data(f'audit_{self.audit_id}',rpttransaction_df)
        # rpttransaction_df.to_csv(f'/home/whirldata/projects/May6_latest_code_AP_IV_VM/May6_latest_code/notebook/processed_genmab_2023_data.csv',index=False)
        capture_log_message(f"shape of ingestion dataframe : {rpttransaction_df.shape}, columns:{rpttransaction_df.columns}")

        
        required_cols = ['audit_id','TRANSACTIONID','TRANSACTION_CODE','ACCOUNTDOCID']

        transactions = transactions[required_cols]

        capture_log_message(log_message="Ingesting RPTTRANSACTION")
        self.DB.upload_data_to_database(transactions,f'rpttransaction_{self.audit_id}')
        capture_log_message(log_message="Ingestion to rpttransaction finished")

    def run_job(self): 
        """
        Function to Run GL Ingestor Job
        """
        update_current_module('ingestion')
        ingestion_start_time = datetime.now(timezone.utc)
        try:
            capture_log_message(log_message="Reading Source data")
            raw_ingest_data = process_batch_data(self.audit_id, self.client_id)
            if raw_ingest_data is None or raw_ingest_data.empty:
                raise Exception("No Source Data Available to read")
            
            ingest_data = self.preprocess_ingest_data(raw_ingest_data)
            capture_log_message(log_message=' Shape of current data read:{}'.format(ingest_data.shape))
            capture_log_message(log_message="Reading transaction data for duplicate check")
            existing_data = self.DB.read_table(f'rpttransaction_{self.audit_id}',columns=['TRANSACTION_CODE'],audit_id=self.audit_id)
            capture_log_message(log_message='Duplicate check started GL for transaction data')
            transactions = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(existing_data['TRANSACTION_CODE'])].copy()
            capture_log_message(log_message='Shape of Dataframe after Duplicate check '+str(transactions.shape))

            #Populating Master Tables
            self.mscompany(transactions)
            self.msfmsuser(transactions)
            self.mslocation(transactions)
            self.mschartofaccounts(transactions)
            self.msdocumenttype(transactions)

            #Populating Reporting tables
            self.rpttransactions(transactions)

            ingestion_end_time = datetime.now(timezone.utc)
            g.gl_ingestion_end_time = ingestion_end_time
            time_taken_for_ingestion = ingestion_end_time - ingestion_start_time
            capture_log_message(current_logger=g.stage_logger,log_message="GL Data Ingestion Process completed",
                                start_time=ingestion_start_time,end_time=ingestion_end_time,
                                time_taken=time_taken_for_ingestion,data_shape=transactions.shape)
            
            # process_data_for_sending_internal_mail(subject='Ingestion Status',stage=utils.DATA_INGESTION_STAGE,is_success=True,
            #                                        date_list=[ingestion_start_time],volume_list=[transactions.shape],
            #                                        time_taken_list=[time_taken_for_ingestion],
            #                                        description_list=['GL Ingestion Completed'])
            
            return True
        
        except Exception as e:
            ingestion_end_time = datetime.now(timezone.utc)
            time_taken_for_ingestion = ingestion_end_time - ingestion_start_time
            capture_log_message(current_logger=g.error_logger,log_message="Error occurred in GL Data Ingestion:{}".format(e),
                                start_time=ingestion_start_time,end_time=ingestion_end_time,
                                time_taken=time_taken_for_ingestion, error_name=utils.INGESTION_FAILED)
            
            capture_log_message(current_logger=g.error_logger,
                                store_in_db=False,
                                log_message= f"Ingestion Failed:{str(traceback.format_exc())}")
            
            process_data_for_sending_internal_mail(subject='Ingestion Status',stage=utils.DATA_INGESTION_STAGE,is_success=False,
                                                   date_list=[ingestion_start_time],
                                                   time_taken_list=[time_taken_for_ingestion],
                                                   description_list=['GL Ingestion Failed'])
            
            return False

def main():

    GL = GL_Ingestor('DB.json')
    GL.run_job()
    

if __name__ == "__main__":
    
    main()