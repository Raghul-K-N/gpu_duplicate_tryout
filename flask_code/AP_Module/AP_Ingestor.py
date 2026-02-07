# import pandas as pd
# import numpy as np
# from AP_Module.db_connector import MySQL_DB
# # from code1.logger import logger


# class AP_Ingestor:

#     def __init__(self, dbjson):
#         """
#         Initializing Database Connection
#         """
#         self.DB = MySQL_DB(dbjson)

#     def msentrytype(self, transactions):
#         """
#         Function to populate entry_type table
#         """
#         columns = ['ENTRY_TYPE']

#         logger.info("Ingesting MSENTRYTYPE")

#         current_table     = self.DB.read_table('msentrytype',columns)
#         new_table = pd.DataFrame(transactions['ENTRY_TYPE'].unique(),columns=['ENTRY_TYPE'])
#         final_table       = new_table.loc[~new_table['ENTRY_TYPE'].isin(current_table['ENTRY_TYPE'])].copy()
#         final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['ENTRY_TYPE'],inplace=True)

#         self.DB.upload_data_to_database(final_table,'msentrytype')

#     def mslocation(self,transactions):
#         """
#         Function to populate mslocation table
#         """
#         columns = ['LOCATION_CODE']

#         logger.info("Ingesting MSLOCATION")
    
#         current_table     = self.DB.read_table('mslocation',columns)
#         # new_table_entered = pd.DataFrame(transactions['ENTERED_LOCATION'].unique(),columns=['LOCATION_CODE'])
#         new_table  = pd.DataFrame(transactions['POSTED_LOCATION'].unique(),columns=['LOCATION_CODE'])
#         # new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['LOCATION_CODE'])

#         final_table       = new_table.loc[~new_table['LOCATION_CODE'].isin(current_table['LOCATION_CODE'])].copy()
#         final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['LOCATION_CODE'],inplace=True)
#         self.DB.upload_data_to_database(final_table,'mslocation')

#     def mscompany(self,transactions):
#         """
#         Function to populate mscompany table
#         """
#         columns = ['COMPANY_CODE']

#         logger.info("Ingesting MSCOMPANY")

#         current_table = self.DB.read_table('mscompany',columns)
#         new_table     = pd.DataFrame(transactions['COMPANY_NAME'].unique(),columns=['COMPANY_CODE'])

#         final_table   = new_table.loc[~new_table['COMPANY_CODE'].isin(current_table['COMPANY_CODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['COMPANY_CODE'],inplace=True)
#         self.DB.upload_data_to_database(final_table,'mscompany')
    
#     def ap_vendorlist(self,transactions):
#         """
#         Function to populate ap_vendorlist table
#         """
#         columns = ['VENDORCODE','VENDOR_NAME'] 
#         rename_cols = {"VENDORCODE":"SUPPLIER_ID","VENDOR_NAME":'SUPPLIER_NAME'}

#         logger.info("Ingesting APVENDORLIST")

#         current_table = self.DB.read_table('ap_vendorlist',columns)
#         new_table     = transactions.groupby('SUPPLIER_ID').agg({"SUPPLIER_NAME":'last'}).reset_index().rename(columns=rename_cols)

#         final_table   = new_table.loc[~new_table['SUPPLIER_ID'].isin(current_table['VENDORCODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['SUPPLIER_ID'],inplace=True)
#         final_table.drop_duplicates(subset=['SUPPLIER_ID'],inplace=True)
#         final_table.rename(columns={'SUPPLIER_ID':'VENDORCODE','SUPPLIER_NAME':'VENDOR_NAME'},inplace=True)
#         self.DB.upload_data_to_database(final_table,'ap_vendorlist')


#     def msfmsuser(self,transactions):
#         """
#         Function to populate msfmsuser table
#         """
#         columns           = ['FMSUSER_CODE','FMSUSER_DEPARTMENT','FMSUSER_POSITION','FMSUSER_LOCATION']
#         entered_by_rename = {"ENTERED_BY":"FMSUSER_CODE",'POSTED_LOCATION':"FMSUSER_LOCATION"}
#         posted_by_rename  = {"POSTED_BY":"FMSUSER_CODE","POSTED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT",'POSTED_BY_ORG_CHART_UNIT':'FMES_ORG_CHART_UNIT','POSTED_BY_USER_TYPE':'FMES_USER_TYPE'}##'POSTED_BY_USER_TYPE'

#         logger.info("Ingesting MSFMSUSER")

#         current_table     = self.DB.read_table('msfmsuser',columns)
#         new_table_entered = transactions.groupby('ENTERED_BY').agg({'POSTED_LOCATION':'last'}).reset_index().rename(columns=entered_by_rename)
#         new_table_posted  = transactions.groupby('POSTED_BY').agg({'POSTED_BY_DEPARTMENT':'last','POSTED_LOCATION':'last','POSTED_BY_ORG_CHART_UNIT':'last','POSTED_BY_USER_TYPE':'last'}).reset_index().rename(columns=posted_by_rename)
#         new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['FMSUSER_CODE'])

        
#         final_table       = new_table.loc[~new_table['FMSUSER_CODE'].isin(current_table['FMSUSER_CODE'])].copy()
#         final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['FMSUSER_CODE'],inplace=True)
#         final_table.rename(columns={'POSTED_LOCATION':'FMSUSER_LOCATION'},inplace=True)
#         self.DB.upload_data_to_database(final_table,'msfmsuser')
    
#     def mschartofaccounts(self,transactions):
#         """
#         Function to populate mschartofaccounts table
#         """

#         columns     =['ACCOUNT_CODE','ACCOUNT_DESCRIPTION','P01_ACCOUNT_DESCRIPTION','P02_ACCOUNT_DESCRIPTION','P03_ACCOUNT_DESCRIPTION','P04_ACCOUNT_DESCRIPTION','P05_ACCOUNT_DESCRIPTION']
        
        
        
#         rename_cols = {'ACCOUNT_NUMBER':'ACCOUNT_CODE','ACCOUNT_DESCRIPTION':'ACCOUNT_DESCRIPTION'}


#         logger.info("Ingesting MSCHARTOFACCOUNTS")
#         current_table = self.DB.read_table('mschartofaccounts',columns)
#         new_table     = transactions.groupby('ACCOUNT_NUMBER').agg({'ACCOUNT_DESCRIPTION':'last'}).reset_index().rename(columns=rename_cols)


#         final_table   = new_table.loc[~new_table['ACCOUNT_CODE'].isin(current_table['ACCOUNT_CODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['ACCOUNT_CODE'],inplace=True)

#         self.DB.upload_data_to_database(final_table,'mschartofaccounts')

#     def ap_invoiceheader(self, transactions):
#         """
#         Function to populate APINVOICEHEADER
#         """
#         columns = ['INVOICE_NUMBER']
#         rename_cols = {'COMPANY_NAME': 'SAP_COMPANY'}

#         logger.info("Ingesting INVOICEHEADER Started")
#         logger.info("Reading Current table for Duplicate check")

#         current_table = self.DB.read_table('ap_invoiceheader', columns)

#         apheader      = transactions.groupby('INVOICE_NUMBER').agg({'SUPPLIER_ID': 'max', 'SUPPLIER_NAME': 'max', 'INVOICE_AMOUNT': 'sum',
#         'INVOICE_DATE': 'last', 'DUE_DATE': 'last'}).reset_index()

#         apheader.rename(columns=rename_cols, inplace=True)

#         final_table   = apheader.loc[~apheader['INVOICE_NUMBER'].isin(current_table['INVOICE_NUMBER'])].copy()

        

#         logger.info("Insertion to InvoiceHeader Started")
#         self.DB.upload_data_to_database(final_table, 'ap_invoiceheader')
#         logger.info("Insertion to InvoiceHeader Finished")

#     def ap_accountdocuments(self,transactions):
#         """
#         Function to populate ap_accountdoc
#         """
#         columns=['ENTRY_ID']
#         rename_cols = {'COMPANY_NAME':'SAP_COMPANY','ENTRY_ID':'ACCOUNTING_DOC'}

#         logger.info("Ingesting accountdocument Started")
#         logger.info("Reading Current table for Duplicate check")

#         current_table = self.DB.read_table('ap_accountdocuments', columns)

#         accountdoc = transactions.groupby('ACCOUNTING_DOC').agg({'DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum','ENTERED_BY':'last',
#         'POSTED_BY':'last','COMPANY_ID':'last','LOCATION_ID':'first','ACCOUNT_ID':'first','CREDIT_PERIOD':'first',
#         "ENTERED_DATE":'last','POSTED_DATE':'last','PAYMENT_DATE':'last','DUE_DATE':'last','PAYMENT_TERMS':'last','AMOUNT':'first',
#         'INVOICE_DATE':'last','INVOICE_NUMBER':'last','VENDORID':'last','INVOICE_AMOUNT':'last'}).reset_index()
        
#         accountdoc.rename(columns=rename_cols, inplace=True)
        

#         final_table   = accountdoc.loc[~accountdoc['ACCOUNTING_DOC'].isin(current_table['ENTRY_ID'])].copy()
        
#         final_table.rename(columns={'ACCOUNTING_DOC':'ENTRY_ID','LOCATION_ID':'LOCATIONID','COMPANY_ID':'COMPANYID'},inplace=True)

#         logger.info("Insertion to Accountdoc Started")
#         self.DB.upload_data_to_database(final_table, 'ap_accountdocuments')
#         logger.info("Insertion to Accountdoc Finished")

    

#     def ap_transaction(self, transactions):
#         """
#         Function to populate APTRANSACTION table
#         """

        
        
#         location = self.DB.read_table('mslocation',columns=['LOCATIONID','LOCATION_CODE'])
#         user     = self.DB.read_table('msfmsuser',columns=['FMSUSERID','FMSUSER_CODE'])
#         company  = self.DB.read_table('mscompany',columns=['COMPANYID','COMPANY_CODE'])
#         coa      = self.DB.read_table('mschartofaccounts',columns=['ACCOUNTID','ACCOUNT_CODE'])
#         # entry    = self.DB.read_table('msentrytype',columns=['ENTRY_TYPE_ID','ENTRY_TYPE'])
#         vendor   = self.DB.read_table('ap_vendorlist',columns=['VENDORID','VENDORCODE'])

#         # entry    = dict(zip(entry.ENTRY_TYPE,entry.ENTRY_TYPE_ID))
#         coa      = dict(zip(coa.ACCOUNT_CODE,coa.ACCOUNTID))
#         location = dict(zip(location.LOCATION_CODE,location.LOCATIONID))
#         user     = dict(zip(user.FMSUSER_CODE,user.FMSUSERID))
#         company  = dict(zip(company.COMPANY_CODE,company.COMPANYID))
#         vendor   = dict(zip(vendor.VENDORCODE,vendor.VENDORID))

        

#         # transactions['ENTRY_TYPE_ID']     = transactions['ENTRY_TYPE'].map(entry)
#         transactions['ACCOUNT_ID']         = transactions['GL_ACCOUNT_TYPE'].map(coa)
#         transactions['ENTERED_BY']        = transactions['ENTERED_BY'].map(user)
#         transactions['POSTED_BY']         = transactions['POSTED_BY'].map(user)
#         transactions['COMPANY_ID']        = transactions['COMPANY_NAME'].map(company)
#         transactions['LOCATION_ID']       = transactions['POSTED_LOCATION'].map(location)
#         transactions['VENDORID']          = transactions['SUPPLIER_ID'].map(vendor)
       
        
        

#         rename_columns = {'TRANSACTION_ID': 'AXIOM_ID', 
#                           'ACCOUNT_DESCRIPTION': 'GL_ACCOUNT_DESCRIPTION'}#, 'GL_ACCOUNT_TYPE': 'ACCOUNT_TYPE'

#         transactions.rename(columns=rename_columns, inplace=True)


#         transactions['TRANSACTION_CODE']    = transactions['TRANSACTION_ID_GA']
#         transactions['DEBIT_AMOUNT']        = transactions['DEBIT_AMOUNT'].fillna(0)
#         transactions['CREDIT_AMOUNT']       = transactions['CREDIT_AMOUNT'].fillna(0)
#         transactions['ACCOUNT_NAME']        = transactions['GL_ACCOUNT_DESCRIPTION']
#         transactions['SYSTEM_POSTING_DATE'] = transactions['ENTERED_DATE']
#         transactions['SYSTEM_UPDATED_DATE'] = transactions['ENTERED_DATE']

#         # transactions['TRANSACTION_DESCRIPTION'] = transactions['JOURNEL_ENTRY_DESCRIPTION']

#         # Payment Amount
#         # transactions['PAYMENT_AMOUNT'] = np.where(((transactions['ENTRY_TYPE'] == "PMNT") & (transactions['ACCOUNT_TYPE'] == "AS") & (transactions['DEBIT_CREDIT_INDICATOR'] == "C")),transactions.AMOUNT, 0)

#         # Invoice Amount
#         # transactions['INVOICE_AMOUNT'] = np.where(((transactions['ENTRY_TYPE'] == "INV")),transactions.AMOUNT, 0)
#         transactions['INVOICE_AMOUNT'] = transactions['INVOICE_AMOUNT']


#         self.ap_accountdocuments(transactions)
#         # self.ap_invoiceheader(transactions)

#         doc      = self.DB.read_table('ap_accountdocuments',columns=['ACCOUNT_DOC_ID','ENTRY_ID'])
#         invoice  = self.DB.read_table('ap_invoiceheader',columns=['INVOICE_ID','INVOICE_NUMBER'])

#         doc      = dict(zip(doc.ENTRY_ID,doc.ACCOUNT_DOC_ID))
#         invoice  = dict(zip(invoice.INVOICE_NUMBER,invoice.INVOICE_ID))
        
#         # Read table to get Invoice IDs
#         transactions['ACCOUNT_DOC_ID']    = transactions['ACCOUNTING_DOC'].map(doc)
#         transactions['INVOICE_ID']        = transactions['INVOICE_NUMBER'].map(invoice)
   
#         # invoices = self.DB.read_table('ap_accountdocuments', columns=['ACCOUNT_ID', 'INVOICE_NUMBER'])
#         # invoices = dict(zip(invoices.INVOICE_NUMBER, invoices.ACCOUNT_ID))

#         # transactions['ACCOUNT_ID'] = transactions['INVOICE_NUMBER'].map(invoices)

#         transactions['POSTING_DATE'] = transactions['POSTED_DATE']
#         transactions['ENTRY_DATE']   = transactions['ENTERED_DATE']

#         required_cols = ['AXIOM_ID', 'ENTERED_BY', 'TRANSACTION_DESCRIPTION', 'GL_ACCOUNT_DESCRIPTION','DEBIT_AMOUNT','INVOICE_PRICE','PO_PRICE','INVOICE_QUANTITY','PO_QUANTITY',
#                          'CREDIT_AMOUNT', 'PAYMENT_DATE', 'ACCOUNT_NUMBER', 'ACCOUNT_NAME','POSTED_BY','DISCOUNT_PERCENTAGE','DISCOUNT_TAKEN',
#                           'SYSTEM_UPDATED_DATE','CREDIT_PERIOD','DUE_DATE','INVOICE_NUMBER','PURCHASE_ORDER_NUMBER','TRANSACTION_CODE','COMPANY_ID','ACCOUNT_ID',
#                           'SUPPLIER_ID','LOCATION_ID','ACCOUNT_DOC_ID','PAYMENT_TERMS','INVOICE_AMOUNT','INVOICE_DATE','VENDORID','POSTING_DATE','ENTRY_DATE','INVOICE_ID',
#                           'GRN_NUMBER','GRN_DATE','PURCHASE_ORDER_DATE','DOC_TYPE','AMOUNT','PAYMENT_METHOD','REQUISITION_DATE','TRANSPORTATION_DATE'
#                         ]

#         transactions = transactions[required_cols]


#         logger.info("Insertion to AP Transactions Started")

#         self.DB.upload_data_to_database(transactions, 'ap_transaction')
#         logger.info("Insertion to AP Transactions Completed")

#     def run_job(self):
#         """
#         Function to Run AP Ingestor Job
#         """

#         logger.info("Reading Source Data")
#         ingest_data = self.DB.read_table('_SRC_AP_DATA')
#         logger.info("Source data Shape "+str(ingest_data.shape))
#         logger.info("Reading transaction data for duplicate check ")
#         existing_data = self.DB.read_table('ap_transaction', columns=['TRANSACTION_CODE'])

#         logger.info("Duplicate check started")
#         transactions = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(existing_data['TRANSACTION_CODE'])].copy()
#         logger.info("Shape of Dataframe after Duplicate check "+str(transactions.shape))
#         # self.msentrytype(transactions)
#         self.mscompany(transactions)
#         self.msfmsuser(transactions)
#         self.mslocation(transactions)
#         self.ap_vendorlist(transactions)
#         self.mschartofaccounts(transactions)
#         self.ap_transaction(transactions)


# def main():
    
#     AP = AP_Ingestor('DB.json')
#     AP.run_job()


# if __name__ == "__main__":
#     main()
