# import pandas as pd
# import numpy as np
# import asyncio
# from db_connector import MySQL_DB
# # from GL_Module.logger import logger

# class GL_Ingestor:
    
#     def __init__(self,dbjson):
#         """
#         Initializing Database Connection
#         """        
#         self.DB = MySQL_DB(dbjson)
    
#     def mscompany(self,transactions):
#         """
#         Function to populate mscompany table
#         """
#         columns = ['COMPANY_CODE']

#         logger.info("Ingesting MSCOMPANY")

#         current_table = self.DB.read_table('mscompany',columns)
#         new_table     = pd.DataFrame(transactions['SAP_COMPANY'].unique(),columns=['COMPANY_CODE'])

#         final_table   = new_table.loc[~new_table['COMPANY_CODE'].isin(current_table['COMPANY_CODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['COMPANY_CODE'],inplace=True)

#         logger.info("Number of records to ingest",final_table.shape)

#         self.DB.upload_data_to_database(final_table,'mscompany')
    
#     def msfmsuser(self,transactions):
#         """
#         Function to populate msfmsuser table
#         """
#         columns           = ['FMSUSER_CODE','FMSUSER_DEPARTMENT','FMSUSER_POSITION','FMSUSER_LOCATION']
#         entered_by_rename = {"ENTERED_BY":"FMSUSER_CODE","ENTERED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT","ENTERED_BY_POSTION":"FMSUSER_POSITION",'ENTERED_LOCATION':"FMSUSER_LOCATION"}
#         posted_by_rename  = {"POSTED_BY":"FMSUSER_CODE","POSTED_BY_DEPARTMENT":"FMSUSER_DEPARTMENT","POSTED_BY_POSTION":"FMSUSER_POSITION",'POSTED_LOCATION':"FMSUSER_LOCATION",'POSTED_BY_FUNCTION':'FMSUSER_FUNCTION','POSTED_BY_ORG_CHART_UNIT':'FMES_ORG_CHART_UNIT','POSTED_BY_USER_TYPE':'FMES_USER_TYPE'}##'POSTED_BY_USER_TYPE'

#         logger.info("Ingesting MSFMSUSER")

#         current_table     = self.DB.read_table('msfmsuser',columns)
#         new_table_entered = transactions.groupby('ENTERED_BY').agg({'ENTERED_BY_POSTION':'max','ENTERED_BY_DEPARTMENT':'last','ENTERED_LOCATION':'last'}).reset_index().rename(columns=entered_by_rename)
#         new_table_posted  = transactions.groupby('POSTED_BY').agg({'POSTED_BY_POSTION':'max','POSTED_BY_DEPARTMENT':'last','POSTED_LOCATION':'last','POSTED_BY_FUNCTION':'last','POSTED_BY_ORG_CHART_UNIT':'last','POSTED_BY_USER_TYPE':'last'}).reset_index().rename(columns=posted_by_rename)
#         new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['FMSUSER_CODE']).reset_index().drop(columns=['index'])  

        
#         final_table       = new_table.loc[~new_table['FMSUSER_CODE'].isin(current_table['FMSUSER_CODE'])].copy()
#         final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table       = final_table.drop_duplicates(subset=['FMSUSER_CODE'])
#         final_table.dropna(axis=0,subset=['FMSUSER_CODE'],inplace=True)

#         self.DB.upload_data_to_database(final_table,'msfmsuser')

#     def mslocation(self,transactions):
#         """
#         Function to populate mslocation table
#         """
#         columns = ['LOCATION_CODE']

#         logger.info("Ingesting MSLOCATION")
    
#         current_table     = self.DB.read_table('mslocation',columns)
#         new_table_entered = pd.DataFrame(transactions['ENTERED_LOCATION'].unique(),columns=['LOCATION_CODE'])
#         new_table_posted  = pd.DataFrame(transactions['POSTED_LOCATION'].unique(),columns=['LOCATION_CODE'])
#         new_table         = pd.concat([new_table_entered,new_table_posted]).drop_duplicates(subset=['LOCATION_CODE'])

#         final_table       = new_table.loc[~new_table['LOCATION_CODE'].isin(current_table['LOCATION_CODE'])].copy()
#         final_table       = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['LOCATION_CODE'],inplace=True)

#         self.DB.upload_data_to_database(final_table,'mslocation')

#     def mschartofaccounts(self,transactions):
#         """
#         Function to populate mschartofaccounts table
#         """
#         columns     =['ACCOUNT_CODE','ACCOUNT_DESCRIPTION','P01_ACCOUNT_DESCRIPTION','P02_ACCOUNT_DESCRIPTION','P03_ACCOUNT_DESCRIPTION','P04_ACCOUNT_DESCRIPTION','P05_ACCOUNT_DESCRIPTION']

#         rename_cols = {'SAP_ACCOUNT':'ACCOUNT_CODE','ACCOUNT_DESCRIPTION':'ACCOUNT_DESCRIPTION','SAP_P01_ACCOUNT_DESCRIPTION':'P01_ACCOUNT_DESCRIPTION',
#         'SAP_P02_ACCOUNT_DESCRIPTION':'P02_ACCOUNT_DESCRIPTION','SAP_P03_ACCOUNT_DESCRIPTION':'P03_ACCOUNT_DESCRIPTION',
#         'SAP_P04_ACCOUNT_DESCRIPTION':'P04_ACCOUNT_DESCRIPTION','SAP_P05_ACCOUNT_DESCRIPTION':'P05_ACCOUNT_DESCRIPTION'}

#         logger.info("Ingesting MSCHARTOFACCOUNTS")
#         current_table = self.DB.read_table('mschartofaccounts',columns)
#         new_table     = transactions.groupby('SAP_ACCOUNT').agg({'ACCOUNT_DESCRIPTION':'last','SAP_P01_ACCOUNT_DESCRIPTION':'last','SAP_P02_ACCOUNT_DESCRIPTION':'last',
#         'SAP_P03_ACCOUNT_DESCRIPTION':'last','SAP_P04_ACCOUNT_DESCRIPTION':'last','SAP_P05_ACCOUNT_DESCRIPTION':'last'}).reset_index().rename(columns=rename_cols)

#         final_table   = new_table.loc[~new_table['ACCOUNT_CODE'].isin(current_table['ACCOUNT_CODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['ACCOUNT_CODE'],inplace=True)


#         self.DB.upload_data_to_database(final_table,'mschartofaccounts')

#     def msdocumenttype(self,transactions):
#         """
#         Function to populate msdocumenttype table
#         """
#         columns = ['DOCUMENT_TYPE_CODE','DOCUMENT_TYPE_DESCRIPTION']
#         rename_cols = {"DOC_TYPE":"DOCUMENT_TYPE_CODE","DOC_TYPE_DESCRIPTION":'DOCUMENT_TYPE_DESCRIPTION'}

#         logger.info("Ingesting MSDOCUMENTYPE")

#         current_table = self.DB.read_table('msdocumenttype',columns)
#         new_table     = transactions.groupby('DOC_TYPE').agg({"DOC_TYPE_DESCRIPTION":'last'}).reset_index().rename(columns=rename_cols)

#         final_table   = new_table.loc[~new_table['DOCUMENT_TYPE_CODE'].isin(current_table['DOCUMENT_TYPE_CODE'])].copy()
#         final_table   = final_table.replace(r'^\s*$', np.nan, regex=True)
#         final_table.dropna(axis=0,subset=['DOCUMENT_TYPE_CODE'],inplace=True)

#         logger.info("Number of records to ingest",final_table.shape)

#         self.DB.upload_data_to_database(final_table,'msdocumenttype')

#     def rptaccountdocument(self,transactions):
#         """
#         Function to populate rptaccountdocument table
#         """
#         logger.info("Ingesting RPTACCOUNTDOCUMENT")
#         logger.info("Reading Existing data")
        
#         existing_table = self.DB.read_table('rptaccountdocument',columns=['ACCOUNTDOC_CODE'])

#         rptaccdoc      = transactions.groupby('ACCOUNTDOC_CODE').agg({'COMPANYID':'last','DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum',
#                                                   'POSTED_BY':'last','POSTED_DATE':'last','POSTED_LOCATION':'last',
#                                                   'DOCUMENTID':'last'}).reset_index()
#         rptaccdoc.rename(columns={"COMPANYID":"COMPANY_CODE"},inplace=True)

#         final_table    = rptaccdoc.loc[~rptaccdoc['ACCOUNTDOC_CODE'].isin(existing_table['ACCOUNTDOC_CODE'])].copy()

#         self.DB.upload_data_to_database(final_table,'rptaccountdocument')
        
#         logger.info("Ingesting Accounting Docs finished")
        
    
#     def rpttransactions(self,transactions):
    
#         doctypes = self.DB.read_table('msdocumenttype',columns=['DOCUMENTID','DOCUMENT_TYPE_CODE'])
#         location = self.DB.read_table('mslocation',columns=['LOCATIONID','LOCATION_CODE'])
#         coa      = self.DB.read_table('mschartofaccounts',columns=['ACCOUNTID','ACCOUNT_CODE'])
#         user     = self.DB.read_table('msfmsuser',columns=['FMSUSERID','FMSUSER_CODE'])
#         company  = self.DB.read_table('mscompany',columns=['COMPANYID','COMPANY_CODE'])

#         doctypes = dict(zip(doctypes.DOCUMENT_TYPE_CODE,doctypes.DOCUMENTID))
#         location = dict(zip(location.LOCATION_CODE,location.LOCATIONID))
#         coa      = dict(zip(coa.ACCOUNT_CODE,coa.ACCOUNTID))
#         user     = dict(zip(user.FMSUSER_CODE,user.FMSUSERID))
#         company  = dict(zip(company.COMPANY_CODE,company.COMPANYID))

#         transactions['DOCUMENTID']        = transactions['DOC_TYPE'].map(doctypes)
#         transactions['POSTED_LOCATION']   = transactions['POSTED_LOCATION'].map(location)
#         transactions['ENTERED_LOCATION']  = transactions['ENTERED_LOCATION'].map(location)
#         transactions['ACCOUNTID']         = transactions['SAP_ACCOUNT'].map(coa)
#         transactions['ENTERED_BY']        = transactions['ENTERED_BY'].map(user)
#         transactions['POSTED_BY']         = transactions['POSTED_BY'].map(user)
#         transactions['COMPANYID']         = transactions['SAP_COMPANY'].map(company)
#         transactions['DEBIT_AMOUNT']      = np.where(transactions.DEBIT_CREDIT_INDICATOR=="D",transactions.AMOUNT,0)
#         transactions['CREDIT_AMOUNT']     = np.where(transactions.DEBIT_CREDIT_INDICATOR=="C",transactions.AMOUNT,0)

#         #renames
#         transactions = transactions.rename(columns={"TRANSACTION_ID_GA":"TRANSACTION_CODE","TRANSACTION_DESCRIPTION":"TRANSACTION_DESC",
#                                                     "ACCOUNTING_DOC":"ACCOUNTDOC_CODE",'POSTING_DATE':'POSTED_DATE','HOLIDAY':'IS_POSTED_HOLIDAY'})

#         required_cols = ['TRANSACTION_CODE','TRANSACTION_DESC','ACCOUNTDOC_CODE','DEBIT_AMOUNT','CREDIT_AMOUNT',
#                         'ACCOUNTID','DOCUMENTID','COMPANYID','ENTERED_DATE','ENTERED_BY','ENTERED_LOCATION','POSTED_DATE','POSTED_BY','POSTED_LOCATION',
#                         'IS_REVERSED','IS_REVERSAL','IS_POSTED_HOLIDAY']

#         transactions = transactions[required_cols]

#         self.rptaccountdocument(transactions)
        
#         accountdoc = self.DB.read_table('rptaccountdocument',columns=['ACCOUNTDOCID','ACCOUNTDOC_CODE'])
#         accountdoc = dict(zip(accountdoc.ACCOUNTDOC_CODE,accountdoc.ACCOUNTDOCID))

#         transactions['ACCOUNTDOCID'] = transactions['ACCOUNTDOC_CODE'].map(accountdoc)
#         transactions.rename(columns={'DOCUMENTID':'DOCTYPEID'},inplace=True)

#         transactions.drop(['POSTED_BY','POSTED_LOCATION','ACCOUNTDOC_CODE'],axis=1,inplace=True)
        
#         logger.info("Ingestion to rpttransaction started")
#         self.DB.upload_data_to_database(transactions,'rpttransaction')
#         logger.info("Ingestion to rpttransaction finished")

#     async def run_job(self): 
        
#         logger.info("Reading Source Data")
#         ingest_data = self.DB.read_table('_SRC_GL_DATA')
#         logger.info("Reading transaction data for duplicate check ")
#         existing_data = self.DB.read_table('rpttransaction',columns=['TRANSACTION_CODE'])

#         logger.info("Duplicate check started")
#         transactions = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(existing_data['TRANSACTION_CODE'])].copy()

#         #Populating Master Tables
#         self.mscompany(transactions)
#         self.msfmsuser(transactions)
#         self.mslocation(transactions)
#         self.mschartofaccounts(transactions)
#         self.msdocumenttype(transactions)

#         #Populating Reporting tables
#         self.rpttransactions(transactions)

# def main():

#     GL = GL_Ingestor('DB.json')
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(GL.run_job())
    

# if __name__ == "__main__":
    
#     main()