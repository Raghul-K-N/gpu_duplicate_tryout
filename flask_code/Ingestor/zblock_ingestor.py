from datetime import datetime, timezone
import pandas as pd
import numpy as np
from flask import g
from Ingestor.db_connector import MySQL_DB
# # from AP_Module.logger import logger
# from code1.logger import logger as main_logger
from code1.logger import capture_log_message, process_data_for_sending_internal_mail, update_current_module
import utils
import os
import traceback
from Ingestor.fetch_data import process_batch_data
from pipeline_data import PipelineData

class Zblock_Ingestor:

    def __init__(self, audit_id, client_id):
        """
        Initializing Database Connection
        """
        self.DB = MySQL_DB(config_filename=None)
        self.audit_id = audit_id
        self.client_id = client_id
    
    def preprocess_ingest_data(self, ingest_data):
       
       capture_log_message(log_message=f"Zblock Ingestion data columns:{ingest_data.columns.tolist()}")

       extra_columns = ['TRANSACTION_ID_GA', 'SUPPLIER_NAME', 'SUPPLIER_ID', 'AMOUNT', 'INVOICE_AMOUNT',
       'INVOICE_DATE', 'INVOICE_NUMBER', 'POSTED_DATE', 
       'REQUISITION_DATE', 'TRANSPORTATION_DATE', 'PAYMENT_DATE', 'DUE_DATE', 'PAYMENT_METHOD',
       'PAYMENT_TERMS', 'PURCHASE_ORDER_NUMBER', 'PURCHASE_ORDER_DATE',
       'GL_ACCOUNT_TYPE', 'ACCOUNT_DESCRIPTION',
       'DEBIT_CREDIT_INDICATOR', 'ENTERED_BY', 'POSTED_BY', 'POSTED_LOCATION', 'COMPANY_NAME',
       'ACCOUNT_NUMBER',  'PO_QUANTITY', 
       'PAYMENT_AMOUNT', 'JOURNEL_ENTRY_DESCRIPTION', 'INVOICE_QUANTITY',
       'ENTRY_TYPE', 'ENTERED_DATE', 'DEBIT_AMOUNT',
       'CREDIT_AMOUNT', 'APPROVED_USER_5', 'APPROVED_USER_4', 'APPROVED_USER_3',
       'APPROVED_USER_2', 'APPROVED_USER_1', 'CREDIT_PERIOD', 'ACCOUNTING_DOC',
       'TRANSACTION_ID', 'POSTED_BY_ORG_CHART_UNIT', 'POSTED_BY_USER_TYPE', 'POSTED_BY_DEPARTMENT',
       'TRANSACTION_DESCRIPTION', 'DOC_TYPE', 'ENTERED_LOCATION','PAYMENT_TERMS_DESCRIPTION',
       'DISCOUNT_PERCENTAGE_1','DISCOUNT_PERCENTAGE_2', 'DISCOUNT_PERIOD_1', 'DISCOUNT_PERIOD_2', 'DISCOUNT_TAKEN', 'GRN_DATE',
       'GRN_NUMBER', 'INVOICE_PRICE', 'PO_PRICE','INVOICE_CURRENCY','VAT_ID','LEGAL_ENTITY_NAME_AND_ADDRESS']
      
       for col in extra_columns:
           if col not in ingest_data.columns:
               ingest_data[col] = pd.NA

       ingest_data['GL_ACCOUNT_TYPE'] = ingest_data['GL_ACCOUNT_TYPE'].fillna("").astype(str)
       ingest_data['ENTERED_BY'] = ingest_data['ENTERED_BY'].fillna("").astype(str)
       ingest_data['POSTED_BY'] = ingest_data['POSTED_BY'].fillna("").astype(str)
       ingest_data['COMPANY_NAME'] = ingest_data['COMPANY_NAME'].fillna("").astype(str)
       ingest_data['POSTED_LOCATION'] = ingest_data['POSTED_LOCATION'].fillna("").astype(str)
       ingest_data['SUPPLIER_ID'] = ingest_data['SUPPLIER_ID'].fillna("").astype(str)
       ingest_data['ACCOUNTING_DOC'] = ingest_data['ACCOUNTING_DOC'].fillna("").astype(str)
       ingest_data['INVOICE_NUMBER'] = ingest_data['INVOICE_NUMBER'].fillna("").astype(str)
       ingest_data['ACCOUNT_NUMBER'] = ingest_data['ACCOUNT_NUMBER'].where( ingest_data['ACCOUNT_NUMBER'].isna(),
                                                                        ingest_data['ACCOUNT_NUMBER'].astype(str).str.strip())
       ingest_data['audit_id'] = g.audit_id
       return ingest_data 
    
    
    def msentrytype(self, transactions):
        """
        Function to populate entry_type table
        """
        columns = ['ENTRY_TYPE']
        capture_log_message(log_message='Ingesting MSENTRYTYPE')

        current_table     = self.DB.read_table('msentrytype',columns)
        new_table = pd.DataFrame(transactions['ENTRY_TYPE'].unique(),columns=['ENTRY_TYPE'])
        final_table       = new_table.loc[~new_table['ENTRY_TYPE'].isin(current_table['ENTRY_TYPE'])].copy()
        final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['ENTRY_TYPE'],inplace=True)
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'msentrytype')

    def mslocation(self,transactions):
        """
        Function to populate mslocation table
        """
        columns = ['LOCATION_CODE']
        capture_log_message(log_message='Ingesting MSLOCATION')
    
        current_table     = self.DB.read_table('mslocation',columns,client_id = self.client_id)
        # new_table_entered = pd.DataFrame(transactions['ENTERED_LOCATION'].unique(),columns=['LOCATION_CODE'])
        new_table  = pd.DataFrame(transactions['POSTED_LOCATION'].unique(),columns=['LOCATION_CODE'])
        # new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['LOCATION_CODE'])

        final_table       = new_table.loc[~new_table['LOCATION_CODE'].isin(current_table['LOCATION_CODE'])].copy()
        final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['LOCATION_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'mslocation')

    def mscompany(self,transactions):
        """
        Function to populate mscompany table
        """
        columns = ['COMPANY_CODE']
        capture_log_message(log_message='Ingesting MSCOMPANY')

        current_table = self.DB.read_table('mscompany',columns,client_id=self.client_id)
        new_table     = pd.DataFrame(transactions['COMPANY_NAME'].unique(),columns=['COMPANY_CODE'])

        final_table   = new_table.loc[~new_table['COMPANY_CODE'].isin(current_table['COMPANY_CODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['COMPANY_CODE'],inplace=True)
        
        # Add company codes in hist. data to the table
        if hasattr(g,'unique_company_codes_in_hist_data'):
            values_to_add = [each for each in g.unique_company_codes_in_hist_data if each not in final_table['COMPANY_CODE'].values]
            new_df = pd.DataFrame({'COMPANY_CODE':values_to_add})
        else:
            new_df = pd.DataFrame()
        final_table = pd.concat([final_table,new_df],ignore_index=True)
        
        final_table['client_id'] = self.client_id
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'mscompany')
    
    def ap_vendorlist(self,transactions):
        """
        Function to populate ap_vendorlist table
        """
        columns = ['VENDORCODE','VENDOR_NAME'] 
        rename_cols = {"VENDORCODE":"SUPPLIER_ID","VENDOR_NAME":'SUPPLIER_NAME'}
        capture_log_message(log_message='Ingesting APVENDORLIST')

        current_table = self.DB.read_table('ap_vendorlist',columns,client_id=self.client_id)
        new_table     = transactions.groupby('SUPPLIER_ID').agg({"SUPPLIER_NAME":'last'}).reset_index().rename(columns=rename_cols)
        vendor_code_values  = current_table['VENDORCODE'].values
        existing_vendor_codes = [each.lower() for each in vendor_code_values]
        final_table   = new_table.loc[~new_table['SUPPLIER_ID'].astype(str).str.lower().isin(existing_vendor_codes)].copy()
        # final_table   = new_table.loc[~new_table['SUPPLIER_ID'].isin(current_table['VENDORCODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['SUPPLIER_ID'],inplace=True)
        final_table.drop_duplicates(subset=['SUPPLIER_ID'],inplace=True)
        final_table.rename(columns={'SUPPLIER_ID':'VENDORCODE','SUPPLIER_NAME':'VENDOR_NAME'},inplace=True)
        
        if hasattr(g,'unique_vendor_names_in_hist_data'):
            new_df = g.unique_vendor_names_in_hist_data
            new_df.columns = ['VENDORCODE','VENDOR_NAME']
        else:
            new_df = pd.DataFrame()
            
        final_table = pd.concat([final_table,new_df],ignore_index=True)
        
        final_table = final_table.drop_duplicates(subset=['VENDORCODE','VENDOR_NAME'],keep='first')
        final_table['client_id']=self.client_id
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'ap_vendorlist')


    def msfmsuser(self,transactions):
        """
        Function to populate msfmsuser table
        """
        columns           = ['FMSUSER_CODE','FMSUSER_DEPARTMENT','FMSUSER_POSITION','FMSUSER_LOCATION']
        entered_by_rename = {"ENTERED_BY":"FMSUSER_CODE",'POSTED_LOCATION':"FMSUSER_LOCATION"}
        # posted_by_rename  = {"POSTED_BY":"FMSUSER_CODE","POSTED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT",'POSTED_BY_ORG_CHART_UNIT':'FMES_ORG_CHART_UNIT','POSTED_BY_USER_TYPE':'FMES_USER_TYPE'}##'POSTED_BY_USER_TYPE'
        posted_by_rename = {"POSTED_BY": "FMSUSER_CODE", "POSTED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT",'POSTED_LOCATION': 'FMSUSER_LOCATION', 'POSTED_BY_ORG_CHART_UNIT':'FMES_ORG_CHART_UNIT','POSTED_BY_USER_TYPE': 'FMES_USER_TYPE'}
        capture_log_message(log_message='Ingesting MSFMSUSER')

        current_table     = self.DB.read_table('msfmsuser',columns,client_id=self.client_id)
        new_table_entered = transactions.groupby('ENTERED_BY').agg({'POSTED_LOCATION':'last'}).reset_index().rename(columns=entered_by_rename)
        new_table_posted  = transactions.groupby('POSTED_BY').agg({'POSTED_BY_DEPARTMENT':'last','POSTED_LOCATION':'last','POSTED_BY_ORG_CHART_UNIT':'last','POSTED_BY_USER_TYPE':'last'}).reset_index().rename(columns=posted_by_rename)
        new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['FMSUSER_CODE'])

        
        final_table       = new_table.loc[~new_table['FMSUSER_CODE'].isin(current_table['FMSUSER_CODE'])].copy()
        final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['FMSUSER_CODE'],inplace=True)
        final_table.rename(columns={'POSTED_LOCATION':'FMSUSER_LOCATION'},inplace=True)
        
        if hasattr(g,'unique_user_names_in_hist_data'):
            values_to_add  = [each for each in g.unique_user_names_in_hist_data if each not in final_table['FMSUSER_CODE'].values]
            new_df = pd.DataFrame({'FMSUSER_CODE':values_to_add})
        else:
            new_df = pd.DataFrame()
        final_table = pd.concat([final_table,new_df],ignore_index=True)

        final_table['client_id'] = self.client_id
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'msfmsuser')
    
    def mschartofaccounts(self,transactions):
        """
        Function to populate mschartofaccounts table
        """

        columns     =['ACCOUNT_CODE','ACCOUNT_DESCRIPTION','P01_ACCOUNT_DESCRIPTION','P02_ACCOUNT_DESCRIPTION','P03_ACCOUNT_DESCRIPTION','P04_ACCOUNT_DESCRIPTION','P05_ACCOUNT_DESCRIPTION']
        
        
        
        rename_cols = {'ACCOUNT_NUMBER':'ACCOUNT_CODE','ACCOUNT_DESCRIPTION':'ACCOUNT_DESCRIPTION'}

        capture_log_message(log_message='Ingesting MSCHARTOFACCOUNTS')
        current_table = self.DB.read_table('mschartofaccounts',columns,client_id=self.client_id)
        new_table     = transactions.groupby('ACCOUNT_NUMBER').agg({'ACCOUNT_DESCRIPTION':'last'}).reset_index().rename(columns=rename_cols)


        final_table   = new_table.loc[~new_table['ACCOUNT_CODE'].isin(current_table['ACCOUNT_CODE'])].copy()
        final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
        final_table.dropna(axis=0,subset=['ACCOUNT_CODE'],inplace=True)
        final_table['client_id'] = self.client_id
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))

        self.DB.upload_data_to_database(final_table,'mschartofaccounts')

    def ap_invoiceheader(self, transactions):
        """
        Function to populate APINVOICEHEADER
        """
        columns = ['INVOICE_NUMBER']
        rename_cols = {'COMPANY_NAME': 'SAP_COMPANY'}
        capture_log_message(log_message='Ingesting INVOICEHEADER Started')
        capture_log_message(log_message='Reading Current table for Duplicate check')

        current_table = self.DB.read_table('ap_invoiceheader', columns)

        apheader      = transactions.groupby('INVOICE_NUMBER').agg({'SUPPLIER_ID': 'max', 'SUPPLIER_NAME': 'max', 'INVOICE_AMOUNT': 'sum',
        'INVOICE_DATE': 'last', 'DUE_DATE': 'last'}).reset_index()

        apheader.rename(columns=rename_cols, inplace=True)

        final_table   = apheader.loc[~apheader['INVOICE_NUMBER'].isin(current_table['INVOICE_NUMBER'])].copy()
        
        capture_log_message(log_message='Number of records to ingest:{}'.format(final_table.shape))
        capture_log_message(log_message='Insertion to InvoiceHeader Started')
        self.DB.upload_data_to_database(final_table, 'ap_invoiceheader')
        capture_log_message(log_message='Insertion to InvoiceHeader Finished')

    def zblock_accountdocuments(self,transactions):
       """
       Function to populate zblock_accountdoc
       """
       capture_log_message('Calling zblock_accountdocuments function')
       columns=['ENTRY_ID']
       rename_cols = {'COMPANY_NAME':'SAP_COMPANY','ENTRY_ID':'ACCOUNTING_DOC'}
       capture_log_message(log_message='Ingesting accountdocument Started')
       capture_log_message(log_message='Reading Current table for Duplicate check')

       accountdoc = transactions.groupby('ACCOUNTING_DOC').agg({'audit_id':'last','ACCOUNT_DOC_ID':'first'}).reset_index()

       accountdoc.rename(columns=rename_cols, inplace=True)

       accountdoc.rename(columns={'ACCOUNTING_DOC':'ENTRY_ID','LOCATION_ID':'LOCATIONID','COMPANY_ID':'COMPANYID'},inplace=True)

       g.acc_doc_to_id_mapping = dict(zip(accountdoc['ENTRY_ID'], accountdoc['ACCOUNT_DOC_ID']))
       # Split ENTRY_ID for database storage
       accountdoc['ENTRY_ID'] = accountdoc['ENTRY_ID'].astype(str).str.split('-').str[0]

       # Create reverse mapping: ACCOUNT_DOC_ID -> split ENTRY_ID
       g.swapped_acc_doc_to_id_mapping = dict(zip(accountdoc['ACCOUNT_DOC_ID'], accountdoc['ENTRY_ID']))
      
       capture_log_message(log_message='Number of records to ingest:{}'.format(accountdoc.shape))

    

    def zblock_transaction(self, transactions):
       """
       Function to populate APTRANSACTION table
       """
       location_table = self.DB.read_table('mslocation',columns=['LOCATIONID','LOCATION_CODE'],client_id=self.client_id)
       user_table     = self.DB.read_table('msfmsuser',columns=['FMSUSERID','FMSUSER_CODE'],client_id=self.client_id)
       company_table  = self.DB.read_table('mscompany',columns=['COMPANYID','COMPANY_CODE'],client_id=self.client_id)
       coa_table      = self.DB.read_table('mschartofaccounts',columns=['ACCOUNTID','ACCOUNT_CODE'],client_id=self.client_id)
       # entry    = self.DB.read_table('msentrytype',columns=['ENTRY_TYPE_ID','ENTRY_TYPE'])
       vendor_table   = self.DB.read_table('ap_vendorlist',columns=['VENDORID','VENDORCODE','VENDOR_NAME'],client_id=self.client_id)
       approval_user = self.DB.read_table('user_approval_matrix',columns = ['USERID','USER_CODE'])
       vendor_table['VENDORCODE'] = vendor_table['VENDORCODE'].astype(str).str.upper()
       # entry    = dict(zip(entry.ENTRY_TYPE,entry.ENTRY_TYPE_ID))
       coa      = dict(zip(coa_table.ACCOUNT_CODE,coa_table.ACCOUNTID))
       coa_value = dict(zip(coa_table.ACCOUNTID,coa_table.ACCOUNT_CODE))
       location = dict(zip(location_table.LOCATION_CODE,location_table.LOCATIONID))
       location_value = dict(zip(location_table.LOCATIONID,location_table.LOCATION_CODE))
       user     = dict(zip(user_table.FMSUSER_CODE,user_table.FMSUSERID))
       user_code = dict(zip(user_table.FMSUSERID,user_table.FMSUSER_CODE))
       company  = dict(zip(company_table.COMPANY_CODE,company_table.COMPANYID))
       company_value = dict(zip(company_table.COMPANYID, company_table.COMPANY_CODE))
       vendor   = dict(zip(vendor_table.VENDORCODE,vendor_table.VENDORID))
       vendor_value = dict(zip(vendor_table.VENDORID,vendor_table.VENDORCODE))
       vendor_name = dict(zip(vendor_table.VENDORID, vendor_table.VENDOR_NAME))
       approval_user = dict(zip(approval_user.USER_CODE,approval_user.USERID))
      
       transactions['ENTERED_BY_NAME'] = transactions['ENTERED_BY']
       transactions['POSTED_BY_NAME'] = transactions['POSTED_BY']


       # transactions['ENTRY_TYPE_ID']     = transactions['ENTRY_TYPE'].map(entry)
       transactions['ACCOUNT_ID']         = transactions['GL_ACCOUNT_TYPE'].map(coa)
       transactions['ACCOUNT_CODE']      = transactions['ACCOUNT_ID'].map(coa_value)


       transactions['ENTERED_BY']        = transactions['ENTERED_BY'].map(user)
       transactions['POSTED_BY']         = transactions['POSTED_BY'].map(user)
       transactions['FMSUSER_CODE']      = transactions['POSTED_BY'].map(user_code)


       transactions['COMPANYID']        = transactions['COMPANY_NAME'].map(company)
       transactions['COMPANY_CODE_NAME']      = transactions['COMPANYID'].map(company_value)
       transactions['COMPANY_CODE']      = transactions['COMPANY_CODE_NAME']


       transactions['LOCATION_ID']       = transactions['POSTED_LOCATION'].map(location)
       transactions['POSTED_LOCATION_NAME']     = transactions['LOCATION_ID'].map(location_value)
       transactions['LOCATION_CODE']     = transactions['POSTED_LOCATION_NAME']


       transactions['VENDORID']          = transactions['SUPPLIER_ID'].map(vendor)
       transactions['VENDORCODE']        = transactions['VENDORID'].map(vendor_value)
       transactions['SUPPLIER_NAME']     = transactions['VENDORID'].map(vendor_name)


       transactions['APPROVED_USER_5']   = transactions['APPROVED_USER_5'].map(approval_user).fillna(-1).astype(int)
       transactions['APPROVED_USER_4']   = transactions['APPROVED_USER_4'].map(approval_user).fillna(-1).astype(int)
       transactions['APPROVED_USER_3']   = transactions['APPROVED_USER_3'].map(approval_user).fillna(-1).astype(int)
       transactions['APPROVED_USER_2']   = transactions['APPROVED_USER_2'].map(approval_user).fillna(-1).astype(int)
       transactions['APPROVED_USER_1']   = transactions['APPROVED_USER_1'].map(approval_user).fillna(-1).astype(int)
      
      


       rename_columns = {'ACCOUNT_DESCRIPTION': 'GL_ACCOUNT_DESCRIPTION'}#, 'GL_ACCOUNT_TYPE': 'ACCOUNT_TYPE'
       capture_log_message(f"lenth of data frame : {len(transactions)}")


       transactions.rename(columns=rename_columns, inplace=True)
    #    transactions['TRANSACTION_ID'] = transactions['AXIOM_ID']


       transactions['TRANSACTION_CODE']    = transactions['TRANSACTION_ID_GA']#used in main
       transactions['DEBIT_AMOUNT']        = transactions['DEBIT_AMOUNT'].fillna(0)
       transactions['CREDIT_AMOUNT']       = transactions['CREDIT_AMOUNT'].fillna(0)
              
       # Convert the amount values into absolute values
       transactions['DEBIT_AMOUNT']        = np.abs(transactions['DEBIT_AMOUNT'])#used in main
       transactions['CREDIT_AMOUNT']       = np.abs(transactions['CREDIT_AMOUNT'])#used in main
      
       transactions['ACCOUNT_NAME']        = transactions['GL_ACCOUNT_DESCRIPTION']
       transactions['SYSTEM_POSTING_DATE'] = transactions['ENTERED_DATE']
       transactions['SYSTEM_UPDATED_DATE'] = transactions['ENTERED_DATE']#used in main


       # transactions['TRANSACTION_DESCRIPTION'] = transactions['JOURNEL_ENTRY_DESCRIPTION']


       # Payment Amount
       # transactions['PAYMENT_AMOUNT'] = np.where(((transactions['ENTRY_TYPE'] == "PMNT") & (transactions['ACCOUNT_TYPE'] == "AS") & (transactions['DEBIT_CREDIT_INDICATOR'] == "C")),transactions.AMOUNT, 0)


       # Invoice Amount
       # transactions['INVOICE_AMOUNT'] = np.where(((transactions['ENTRY_TYPE'] == "INV")),transactions.AMOUNT, 0)
       transactions['INVOICE_AMOUNT'] = transactions['INVOICE_AMOUNT']#used in main


       # transactions['audit_id'] = self.audit_id


       self.zblock_accountdocuments(transactions)
       # self.ap_invoiceheader(transactions)


       # doc      = self.DB.read_table(f'ap_accountdocuments_{self.audit_id}',columns=['ACCOUNT_DOC_ID','ENTRY_ID'],audit_id=self.audit_id)
       # doc      = dict(zip(doc.ENTRY_ID,doc.ACCOUNT_DOC_ID))
       invoice  = self.DB.read_table('ap_invoiceheader',columns=['INVOICE_ID','INVOICE_NUMBER'])
       invoice  = dict(zip(invoice.INVOICE_NUMBER,invoice.INVOICE_ID))
      
       # Read table to get Invoice IDs
    #    transactions['ACCOUNT_DOC_ID']    = transactions['ACCOUNTING_DOC'].map(g.acc_doc_to_id_mapping)
       transactions['ENTRY_ID']          = transactions['ACCOUNT_DOC_ID'].map(g.swapped_acc_doc_to_id_mapping)
       transactions['INVOICE_ID']        = transactions['INVOICE_NUMBER'].map(invoice)
 
       # invoices = self.DB.read_table('ap_accountdocuments', columns=['ACCOUNT_ID', 'INVOICE_NUMBER'])
       # invoices = dict(zip(invoices.INVOICE_NUMBER, invoices.ACCOUNT_ID))


       # transactions['ACCOUNT_ID'] = transactions['INVOICE_NUMBER'].map(invoices)


       transactions['POSTING_DATE'] = transactions['POSTED_DATE']
       transactions['ENTRY_DATE']   = transactions['ENTERED_DATE']
       transactions['DISCOUNT_TAKEN'] = np.where(transactions['DISCOUNT_TAKEN'] != 0, 1, 0)

    #    past_required_cols = ['audit_id','AXIOM_ID', 'ENTERED_BY', 'TRANSACTION_DESCRIPTION', 'GL_ACCOUNT_DESCRIPTION','DEBIT_AMOUNT','INVOICE_PRICE','PO_PRICE','INVOICE_QUANTITY','PO_QUANTITY',
    #                     'CREDIT_AMOUNT', 'PAYMENT_DATE', 'ACCOUNT_NUMBER', 'ACCOUNT_NAME','POSTED_BY','DISCOUNT_PERCENTAGE','DISCOUNT_TAKEN','DISCOUNT_PERIOD',
    #                      'SYSTEM_UPDATED_DATE','CREDIT_PERIOD','DUE_DATE','INVOICE_NUMBER','PURCHASE_ORDER_NUMBER','TRANSACTION_CODE','COMPANY_ID','ACCOUNT_ID',
    #                      'SUPPLIER_ID','LOCATION_ID','ACCOUNT_DOC_ID','PAYMENT_TERMS','INVOICE_AMOUNT','INVOICE_DATE','VENDORID','POSTING_DATE','ENTRY_DATE','INVOICE_ID',
    #                      'GRN_NUMBER','GRN_DATE','PURCHASE_ORDER_DATE','DOC_TYPE','AMOUNT','PAYMENT_METHOD','REQUISITION_DATE','TRANSPORTATION_DATE'
    #                    ,'ENTERED_BY_NAME','POSTED_BY_NAME']
      
       aptransaction_df = transactions.copy()
       pipe_obj = PipelineData()
       pipe_obj.set_data(f'zblock_audit_{self.audit_id}',aptransaction_df)


       capture_log_message(f"shape of ingestion dataframe : {aptransaction_df.columns}")
       required_cols = ['audit_id','TRANSACTION_ID','TRANSACTION_CODE','ACCOUNT_DOC_ID']
      
       transactions = transactions[required_cols]


       capture_log_message(log_message='Ingesting APTRANSACTION')
       capture_log_message(log_message='Number of records to ingest:{}'.format(transactions.shape))
        

       self.DB.upload_data_to_database(transactions, f'zblock_transaction_{g.audit_id}')
       capture_log_message(log_message='Insertion to AP Transactions Completed')

    def run_job(self):
        """
        Function to Run ZBLOCK Ingestor Job
        """

        ingestion_start_time = datetime.now(timezone.utc)
        try:
            capture_log_message(log_message='Reading Source Data')
            raw_ingest_data = process_batch_data(self.audit_id, self.client_id)
            if raw_ingest_data is None or raw_ingest_data.empty:
                raise Exception("No Source Data Available to read")
            
            ingest_data = self.preprocess_ingest_data(raw_ingest_data)
            capture_log_message(log_message=' Shape of current data read:{}'.format(ingest_data.shape))

            transactions = ingest_data.copy()
            
            # self.msentrytype(transactions)
            self.mscompany(transactions)
            self.msfmsuser(transactions)
            self.mslocation(transactions)
            # VENDOR_DATA_FLAG = int(os.getenv("VENDOR_DATA_FILE", "0")) == 1
            # if not VENDOR_DATA_FLAG:
            #     self.ap_vendorlist(transactions)
            self.mschartofaccounts(transactions)
            self.zblock_transaction(transactions)
            
            ingestion_end_time = datetime.now(timezone.utc)
            g.zblock_ingestion_end_time = ingestion_end_time
            time_taken_for_ingestion = ingestion_end_time-ingestion_start_time
            capture_log_message(current_logger=g.stage_logger,log_message='Data Ingestion Process completed',
                                start_time=ingestion_start_time,end_time=ingestion_end_time,
                                time_taken=time_taken_for_ingestion,data_shape=transactions.shape)
            g.zblock_ingestion_status = True
            # process_data_for_sending_internal_mail(subject='Ingestion Status',is_success=True,stage = utils.DATA_INGESTION_STAGE,
            #                                        date_list=[ingestion_start_time],volume_list=[transactions.shape],
            #                                        time_taken_list=[time_taken_for_ingestion],
            #                                        description_list=['AP Ingestion Completed'])
            
            return True

        except Exception as e:
            ingestion_end_time = datetime.now(timezone.utc)
            time_taken_for_ingestion = ingestion_end_time-ingestion_start_time
            capture_log_message(current_logger=g.error_logger,
                                start_time=ingestion_start_time,end_time=ingestion_end_time,
                                log_message='Error in AP Ingestion:{}'.format(e),time_taken=time_taken_for_ingestion,
                                error_name=utils.INGESTION_FAILED)
            
            capture_log_message(current_logger=g.error_logger,
                                store_in_db=False,
                                log_message= str(traceback.format_exc()))
            process_data_for_sending_internal_mail(subject='Ingestion Status',is_success=False,stage = utils.DATA_INGESTION_STAGE,
                                                   date_list=[ingestion_start_time],
                                                   time_taken_list=[time_taken_for_ingestion],
                                                   description_list=['AP Ingestion Failed'])
            return False
