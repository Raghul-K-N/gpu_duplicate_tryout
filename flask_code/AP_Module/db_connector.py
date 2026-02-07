import os
import json
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import pymysql.cursors
# from code1.logger import logger
from code1.logger import capture_log_message
from flask import g
import time
import random

import utils

class MySQL_DB:
    
    def __init__(self,config_filename):
        """
        Initializing Database variables from the DB.json
        """
        import os
        from local_database import get_database_credentials
        credentials = get_database_credentials()
        self.user = credentials["username"]
        self.password = credentials["password"]
        self.host = os.getenv("DB_HOST")
        self.port= os.getenv("DB_PORT")
        self.dbname = os.getenv("DB_NAME")
        self.ssl_ca_file = os.getenv("SSL_CA")
        self.USE_SSL_CA = os.getenv("USE_SSL_CA", "false").lower() == "true"
        self.connect_args = {'ssl': {'ca': self.ssl_ca_file}} if self.USE_SSL_CA else {'ssl': None}
        self.ssl_args = {'ssl_ca': self.ssl_ca_file } if self.USE_SSL_CA else {}

    def get_db_configs(self):
        """
        Getting the Database configurations
        """
        db = os.path.join(os.getcwd(),self.config_filename)
        with open(db) as f:
            db_configs = json.load(f,strict=False)
            
        return db_configs
    
    def connect_to_database(self):
        """
        Connect to Database for Fetching Data
        """
        return mysql.connector.connect(user=self.user, password=self.password,host=self.host,
                                       port=self.port,database=self.dbname, **self.ssl_args)

    def update_transaction_table_with_scores(self):
        """
        Updating the rpttransaction table with values in rpttransactionscore[newly scored values]
        """
        update_tran_query = """UPDATE rpttransaction rpt1 INNER JOIN rpttransactionscore rpts ON rpt1.TRANSACTIONID=rpts.TRANSACTIONID
        SET rpt1.DEVIATION = rpts.DEVIATION,rpt1.BLENDED_RISK_SCORE = rpts.BLENDED_RISK_SCORE,rpt1.AI_RISK_SCORE = rpts.AI_RISK_SCORE,
        rpt1.STAT_RISK_SCORE = rpts.STAT_SCORE_INDEXED,rpt1.RULES_RISK_SCORE = rpts.RULES_RISK_SCORE,rpt1.CONTROL_DEVIATION = rpts.CONTROL_DEVIATION,
        rpt1.NEXT_QTR_POSTING = rpts.NEXT_QTR_POSTING,rpt1.POSTS_HOLIDAYS = rpts.POSTS_HOLIDAYS,rpt1.POSTS_NIGHT = rpts.POSTS_NIGHT,rpt1.POSTS_WEEKEND = rpts.POSTS_WEEKEND,
        rpt1.SAME_USER_POSTING = rpts.SAME_USER_POSTING,rpt1.SUSPICIOUS_KEYWORDS = rpts.SUSPICIOUS_KEYWORDS,rpt1.NON_FINANCE_PERSON = rpts.NON_FINANCE_PERSON,
        rpt1.ACCRUAL = rpts.ACCRUAL,rpt1.REVERSALS = rpts.REVERSALS,rpt1.DESCRIPTION = rpts.DESCRIPTION,rpt1.FIREFIGHTER = rpts.FIREFIGHTER,rpt1.NON_BALANCED = rpts.NON_BALANCED,
        rpt1.CASH_NEGATIVE_BALANCE = rpts.CASH_NEGATIVE_BALANCE,rpt1.CASH_ACCRUALS = rpts.CASH_ACCRUALS,rpt1.CASH_CONCEN_CREDIT = rpts.CASH_CONCEN_CREDIT,rpt1.CASH_CONCEN_DEBIT = rpts.CASH_CONCEN_DEBIT,
        rpt1.CASH_DB_DEBIT = rpts.CASH_DB_DEBIT,rpt1.CASH_DB_CREDIT = rpts.CASH_DB_CREDIT,rpt1.CASH_PR_DEBIT = rpts.CASH_PR_DEBIT,rpt1.CASH_PR_CREDIT = rpts.CASH_PR_CREDIT,
        rpt1.CASH_LOCKBOX = rpts.CASH_LOCKBOX ,rpt1.CASH_LOCKBOX_SUSPENSE = rpts.CASH_LOCKBOX_SUSPENSE; 
        """
        capture_log_message("Updating Scores in Transaction Table")
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_tran_query)
        connect.commit()
        connect.close()
        capture_log_message("Update transaction table with score Finished")
    
    def update_accountdoc_table_with_scores(self):
        """
        Updating the rptaccountdoc table with values in rptaccountdocscore[newly scored values]
        """
        update_accdoc_query = """UPDATE rptaccountdocument rpt1 INNER JOIN rptaccountdocscore rpts ON rpt1.ACCOUNTDOCID=rpts.ACCOUNTDOCID
        SET rpt1.DEVIATION = rpts.DEVIATION,rpt1.BLENDED_RISK_SCORE = rpts.BLENDED_RISK_SCORE,rpt1.AI_RISK_SCORE = rpts.AI_RISK_SCORE,
        rpt1.STAT_RISK_SCORE = rpts.STAT_SCORE_INDEXED,rpt1.RULES_RISK_SCORE = rpts.RULES_RISK_SCORE,rpt1.CONTROL_DEVIATION = rpts.CONTROL_DEVIATION,
        rpt1.NEXT_QTR_POSTING = rpts.NEXT_QTR_POSTING,rpt1.POSTS_HOLIDAYS = rpts.POSTS_HOLIDAYS,rpt1.POSTS_NIGHT = rpts.POSTS_NIGHT,rpt1.POSTS_WEEKEND = rpts.POSTS_WEEKEND,
        rpt1.SAME_USER_POSTING = rpts.SAME_USER_POSTING,rpt1.SUSPICIOUS_KEYWORDS = rpts.SUSPICIOUS_KEYWORDS,rpt1.NON_FINANCE_PERSON = rpts.NON_FINANCE_PERSON,
        rpt1.ACCRUAL = rpts.ACCRUAL,rpt1.REVERSALS = rpts.REVERSALS,rpt1.DESCRIPTION = rpts.DESCRIPTION,rpt1.FIREFIGHTER = rpts.FIREFIGHTER,rpt1.NON_BALANCED = rpts.NON_BALANCED,
        rpt1.CASH_NEGATIVE_BALANCE = rpts.CASH_NEGATIVE_BALANCE,rpt1.CASH_ACCRUALS = rpts.CASH_ACCRUALS,rpt1.CASH_CONCEN_CREDIT = rpts.CASH_CONCEN_CREDIT,rpt1.CASH_CONCEN_DEBIT = rpts.CASH_CONCEN_DEBIT,
        rpt1.CASH_DB_DEBIT = rpts.CASH_DB_DEBIT,rpt1.CASH_DB_CREDIT = rpts.CASH_DB_CREDIT,rpt1.CASH_PR_DEBIT = rpts.CASH_PR_DEBIT,rpt1.CASH_PR_CREDIT = rpts.CASH_PR_CREDIT,
        rpt1.CASH_LOCKBOX = rpts.CASH_LOCKBOX ,rpt1.CASH_LOCKBOX_SUSPENSE = rpts.CASH_LOCKBOX_SUSPENSE;
        """
        capture_log_message("Updating Scores in Accounting Doc Table")
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_accdoc_query)
        connect.commit()
        connect.close()
        capture_log_message("Update accountdoc table with score Finished")

    def update_scored_transaction(self,audit_id):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored transactions
        """
        capture_log_message("Updating Scored Transaction")
        update_query=f"""UPDATE ap_transaction_{audit_id} aptran
        inner join ap_transactionscore_{audit_id} aptransc on aptran.TRANSACTION_ID = aptransc.TRANSACTION_ID
        set aptran.IS_SCORED = 1
        where aptran.IS_SCORED = 0 and aptran.audit_id = {audit_id};
        """
        
        with self.connect_to_database() as connect:
            with connect.cursor() as cur:
                cur.execute(update_query)
                connect.commit()
                connect.close()
                capture_log_message("Update scored_transaction Finished")

    def update_scored_accountdoc(self,audit_id):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored accountingdocs
        """
        import time
        import random
        attempt = 0
        retry_count=3
        while attempt < retry_count:
            try:
                capture_log_message("Updating Scored Accountdoc")
                update_query=f"""UPDATE ap_accountdocuments_{audit_id} apaccdoc
                inner join ap_accountdocscore_{audit_id} apaccdocscore on apaccdoc.ACCOUNT_DOC_ID = apaccdocscore.ACCOUNT_DOC_ID
                set apaccdoc.IS_SCORED = 1
                where apaccdoc.IS_SCORED = 0 and apaccdoc.audit_id = {audit_id};
                """
                with self.connect_to_database() as connect:
                    with connect.cursor() as cur:
                        cur.execute(update_query)
                        connect.commit()
                        connect.close()
                capture_log_message("Update scored_accountdoc Finished")
                return True
            except Exception as e:
                if 'deadlock' in str(e).lower():
                    attempt += 1
                    time.sleep(random.uniform(0.5, 2.0))  # Sleep for a random short interval before retrying
                    capture_log_message(log_message=f'AUD_{audit_id}FLAT_Attempt {attempt} failed: {e}. Retrying in 2 seconds...')
                    continue # Retry the transaction
                else:
                    capture_log_message(current_logger=g.error_logger, log_message="Error in updating ap_accountdocuments " + str(e))
                    return False

    def update_tables(self,audit_id, max_retries=4):
        """
        calling the above functions at once
        """
        # self.update_transaction_table_with_scores()
        # self.update_accountdoc_table_with_scores()
        # self.update_scored_transaction(audit_id)
        # self.update_scored_accountdoc(audit_id)
        for attempt in range(max_retries):
            try:
                capture_log_message("Starting update_tables process")
                self.update_scored_transaction(audit_id)
                self.update_scored_accountdoc(audit_id)
                capture_log_message("update_tables process completed successfully")
                break  # Break out of the loop if the transactions were successful
            except Exception as e:
                capture_log_message(current_logger=g.error_logger,log_message=f"Error in update_tables process: {e}",error_name=utils.ISSUES_WITH_STORING_RESULTS)
                if 'deadlock' in str(e).lower():
                    wait_time = random.uniform(1, 3)  # Wait for a random time between 1 to 3 seconds
                    capture_log_message(f"Deadlock detected. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)  # Sleep before retrying
                else:
                    raise  # Re-raise the exception if it's not a deadlock

    def update_job_status(self,status,process_details):
        """
        Function to update job status in ML Execution table
        """
        process_details = json.dumps(process_details)
        with open('execution_id.txt','r') as execution_id:
            exec_id = execution_id.readline()
        update_job_query = """UPDATE ml_execution SET PROCESS_STATUS={status},PROCESS_DETAILS='{pdetail}' WHERE EXECUTION_ID={exe_id};""".format(status=status,exe_id=exec_id,pdetail=process_details)
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_job_query)
        connect.commit()
        connect.close()
        capture_log_message("Update job status Finished")


    def read_table(self,tablename,columns=None):
        """
        Function to read table from database
        """
        connection  = self.connect_to_database()
        if columns is None:
            table = pd.read_sql("""SELECT * FROM {};""".format(tablename),con=connection)
        else:
            cols = ",".join(columns)
            table = pd.read_sql("""SELECT {} FROM {};""".format(cols,tablename),con=connection)
        connection.commit()
        connection.close()
        return table
    
    def read_table_for_client(self,tablename,columns=None,client_id=None):
        """
        Function to read table from database
        """
        with self.connect_to_database() as connection:
                if (columns is None ) and (client_id is None):
                    table = pd.read_sql("""SELECT * from {};""".format(tablename),con=connection)
                elif (columns is None ) and (client_id):
                    table = pd.read_sql("""SELECT * FROM {} where client_id = {};""".format(tablename,client_id),con=connection)
                elif (columns) and (client_id is None):
                    cols = ','.join(columns)
                    table = pd.read_sql("""SELECT {} FROM {};""".format(cols,tablename),con=connection)
                    
                elif (columns) and (client_id):
                    cols=",".join(columns)
                    table=pd.read_sql("""SELECT {} FROM {} where client_id = {};""".format(cols,tablename,client_id),con=connection)
                else:
                    return pd.DataFrame()
                
        return table

    def update_adf_process_status(self,pstatus):
        """
        Updating the job status in the ADF Process table
        """
        update_job_query = """UPDATE _ADF_PROCESS SET PSTATUS='{}' WHERE PSTATUS=1""".format(pstatus)
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_job_query)
        connect.commit()
        connect.close()
        capture_log_message("Update adf_process_status Finished")
        
    def upload_data_to_database(self,data,tablename):
        """
        Upload data to a database
        ------------------------
        Input :
                data : data to be uploaded
                tablename : table to which the data should be uploaded
        """
    
        engine = create_engine("mysql+pymysql://"+self.user+":"+self.password+"@"+self.host+":"+str(self.port)+"/"+self.dbname, connect_args = self.connect_args)
        with engine.connect() as connection:
            data.to_sql(tablename, con=connection,index = False, if_exists = 'append')
            engine.dispose()

    def connect_db_cursorclass(self):

        conn = pymysql.connect(
        host=self.host,
        user=self.user,
        password=self.password,
        db=self.dbname,
        cursorclass=pymysql.cursors.DictCursor,
        **self.ssl_args
        )
        return conn  

    def get_user_matrix(self):
        """
        Fetch the User matrix data from the user matrix table
        """
        with self.connect_db_cursorclass() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM user_approval_matrix")
                user_matrix = cursor.fetchall()
        return user_matrix
    
    def get_approvers_accdoc(self):
        """
        Fetch the Approver user id and amount from the accountdocument table
        """
        columns = 'ACCOUNT_DOC_ID,AMOUNT,APPROVED_USER_1,APPROVED_USER_2,APPROVED_USER_3,APPROVED_USER_4,APPROVED_USER_5'

        with self.connect_to_database() as connection:
            approvers_data= pd.read_sql(f"SELECT {columns} FROM ap_accountdocuments_{g.audit_id} where IS_SCORED=0 ;",con=connection)

        return approvers_data

    def get_approval_matrix(self):
        """
        Fetch the Approval Matrix data from the approval_matrix table
        """
        with self.connect_db_cursorclass() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM approval_matrix")
                approval_matrix = cursor.fetchall()
        return approval_matrix
    
    def get_approval_matrix_mode(self,audit_id):
        """
        Fetch the mode value from trconfiguration for approval matrix rulw
        """
        with self.connect_to_database() as connection:
            mode = pd.read_sql(f"select KEYVALUE from trconfiguration where KEYNAME = 'APPROVAL_MATRIX_MODE' and audit_id = {audit_id};",con=connection)

        return mode
    
    def add_values_in_ap_vendorlist_table(self):
        try:
            capture_log_message("Adding unique vendor values from historical data in ap_vendorlist table")
            vendor = self.read_table_for_client(client_id=g.client_id,tablename='ap_vendorlist',columns=['VENDORCODE','VENDOR_NAME'])
            new_vendor_df = g.unique_vendor_names_in_hist_data
            new_vendor_df.rename(columns={'SUPPLIER_NAME':'VENDOR_NAME'},inplace=True)
            existing_vendor_codes = vendor['VENDORCODE'].values
            existing_vendor_codes = [str(each).lower() for each in existing_vendor_codes]
            final_vendor_df = new_vendor_df[ ~(new_vendor_df['VENDORCODE'].astype(str).str.lower().isin(existing_vendor_codes))].copy()
            if final_vendor_df.empty:
                capture_log_message('No unique vendor names found in  historical data')
            else:
                final_vendor_df = final_vendor_df.drop_duplicates(subset=['VENDORCODE','VENDOR_NAME'],keep='first')
                final_vendor_df['client_id']=g.client_id
                capture_log_message(f"Adding new vendors in ap_vendorlist {final_vendor_df.shape}")
                self.upload_data_to_database(final_vendor_df,'ap_vendorlist')
                
        except Exception as e:
            capture_log_message(current_logger=g.error_logger,log_message=str(e))
            
            
    def add_values_in_mscompany_table(self):
        try:
            capture_log_message('Adding unique company codes from historical data in mscompany table')
            company_codes = self.read_table_for_client(client_id=g.client_id,tablename="mscompany",columns=['COMPANY_CODE'])
            new_company_code_df = pd.DataFrame(g.unique_company_codes_in_hist_data,columns=['COMPANY_CODE'])
            existing_company_codes = list(company_codes['COMPANY_CODE'].values)
            existing_company_codes = [ str(each).lower() for each in existing_company_codes]
            final_company_df = new_company_code_df [~(new_company_code_df['COMPANY_CODE'].astype(str).str.lower().isin(existing_company_codes))].copy()
            if final_company_df.empty:
                capture_log_message('No unique Company codes found in historical data')
            else:
                final_company_df = final_company_df.drop_duplicates(subset=['COMPANY_CODE'],keep='first')
                final_company_df['client_id'] = g.client_id
                capture_log_message(f" Adding new company codes in mscompany table{final_company_df.shape}")
                self.upload_data_to_database(final_company_df,'mscompany')
                
        except Exception as e:
            capture_log_message(current_logger=g.error_logger,log_message=str(e))
            
            
    def add_values_in_msfmsuser_table(self):
        try:
            capture_log_message('Adding unique Usernames from historical data in msfmsuser table')
            users = self.read_table_for_client(client_id=g.client_id,tablename='msfmsuser',columns=['FMSUSER_CODE'])
            new_users_df = pd.DataFrame(g.unique_user_names_in_hist_data,columns=['FMSUSER_CODE'])
            existing_user_values = list(users['FMSUSER_CODE'].values)
            existing_user_values = [str(each).lower() for each in existing_user_values]
            final_user_df = new_users_df[~(new_users_df['FMSUSER_CODE'].astype(str).str.lower().isin(existing_user_values))].copy()
            if final_user_df.empty:
                capture_log_message('No Unique users found in Historical Data')
            else:
                final_user_df = final_user_df.drop_duplicates(subset=['FMSUSER_CODE'],keep='first')
                final_user_df['client_id'] = g.client_id
                capture_log_message(f"Adding new users in msfmsuer table :{ final_user_df.shape}")
                self.upload_data_to_database(final_user_df,'msfmsuser')
        except Exception as e:
            capture_log_message(current_logger=g.error_logger,log_message=str(e))
            
