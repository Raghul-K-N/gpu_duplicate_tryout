from operator import index
import os,json,mysql.connector
from sqlalchemy import create_engine
import pandas as pd
# from GL_Module.logger import logger
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
        capture_log_message(log_message="Updating Scores in Transaction Table")
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_tran_query)
        connect.commit()
        connect.close()
        capture_log_message(log_message="Update transaction table with score Finished")
    
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
        capture_log_message(log_message="Updating Scores in Accounting Doc Table")
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_accdoc_query)
        connect.commit()
        connect.close()
        capture_log_message(log_message="Update accountdoc table with score Finished")

    def update_scored_transaction(self,audit_id):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored transactions
        """
        capture_log_message(log_message="Updating Scored Transaction")
        update_query=f"""UPDATE rpttransaction_{audit_id} rpttran
        inner join rpttransactionscore_{audit_id} rpttransc on rpttran.TRANSACTIONID = rpttransc.TRANSACTIONID
        set rpttran.IS_SCORED = 1
        where rpttran.IS_SCORED = 0 and rpttran.audit_id = {audit_id};
        """
        with self.connect_to_database() as connect:
            with connect.cursor() as cur:
                cur.execute(update_query)
                connect.commit()
        capture_log_message(log_message="Update scored_transaction Finished")

    def update_scored_accountdoc(self,audit_id):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored accountingdocs
        """
        capture_log_message(log_message="Updating Scored Accountdoc")
        update_query=f"""UPDATE rptaccountdocument_{audit_id} rptaccdoc
        inner join rptaccountdocscore_{audit_id} rptaccdocscore on rptaccdoc.ACCOUNTDOCID = rptaccdocscore.ACCOUNTDOCID
        set rptaccdoc.IS_SCORED = 1
        where rptaccdoc.IS_SCORED = 0 and rptaccdoc.audit_id = {audit_id};
        """
        with self.connect_to_database() as connect:
            with connect.cursor() as cur:
                cur.execute(update_query)
                connect.commit()
        capture_log_message(log_message="Update scored_accountdoc Finished")

    def update_tables(self,audit_id, max_retries=4):
        """
        calling the above functions at once
        """
        # self.update_transaction_table_with_scores()
        # self.update_accountdoc_table_with_scores()
        for attempt in range(max_retries):
            try:
                capture_log_message("Starting update_tables process")
                self.update_scored_transaction(audit_id)
                self.update_scored_accountdoc(audit_id)
                capture_log_message("update_tables process completed successfully")
                break
            except Exception as e:
                capture_log_message(current_logger=g.error_logger,log_message=f"Error in update_tables process: {e}",error_name=utils.ISSUES_WITH_STORING_RESULTS)
                if 'deadlock' in str(e).lower():
                    wait_time = random.uniform(1, 3)  # Wait for a random time between 1 to 3 seconds
                    capture_log_message(f"Deadlock detected while updating IS_SCORED. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)  # Sleep before retrying
                else:
                    raise 

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
        capture_log_message(log_message="Update job status Finished")


    # def read_table(self,tablename,columns=None):
    #     """
    #     Function to read table from database
    #     """
    #     connection  = self.connect_to_database()
    #     chunk_list=[]
    #     if columns == None:
    #         table = pd.read_sql("""SELECT * FROM {};""".format(tablename),con=connection)
    #     else:
    #         cols = ",".join(columns)
    #         table = pd.read_sql("""SELECT {} FROM {};""".format(cols,tablename),con=connection)
    #     connection.close()
    #     return table

    def read_table(self,tablename, columns=None, audit_id=None, client_id=None):
        """
        Function to read table from database
        """
        query = None
        capture_log_message(f'fetching data from {tablename} for audit id {audit_id} and client id {client_id}')

        if columns is None:
            if audit_id:
                query = f"SELECT * FROM {tablename} where audit_id = {audit_id};"
            elif client_id:
                query = f"SELECT * FROM {tablename} where client_id = {client_id};"
            else:
                query = f"SELECT * FROM {tablename};"
        else:
            cols = ",".join(columns)
            if audit_id:
                query = f"SELECT {cols} FROM {tablename} where audit_id = {audit_id};"
            elif client_id:
                query = f"SELECT {cols} FROM {tablename} where client_id = {client_id};"
            else:   
                query = f"SELECT {cols} FROM {tablename};"
        with self.connect_to_database() as connection:
            table = pd.read_sql(query, con=connection)
        return table


    def read_SRC_table(self,tablename,columns=None):
        """
        Function to read table from database
        """
        connection  = self.connect_to_database()
        chunk_list=[]
        if columns == None:
            table = pd.read_sql("""SELECT * FROM {} where ID BETWEEN 0 and 1000000;""".format(tablename),con=connection)
        else:
            cols = ",".join(columns)
            table = pd.read_sql("""SELECT {} FROM {};""".format(cols,tablename),con=connection)
        connection.close()
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
        capture_log_message(log_message="Update adf_process_status Finished")
        
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
            data.to_sql(tablename, con=connection, index = False, if_exists = 'append')
        engine.dispose()

    def get_matching_account_numbers_from_coa(self,column_to_filter,value_to_filter):
        """This function is used to fetch Account Numbers from ChartofAccounts that matches a particular category"""
        with self.connect_to_database() as connection:
            table = pd.read_sql("""select account_code from mschartofaccounts where {}='{}';""".format(column_to_filter,value_to_filter),con=connection)
        if table.empty:
            capture_log_message(log_message=f"No matching account numbers found for {column_to_filter} = {value_to_filter}")
            return []
        return table['account_code'].to_list()
        
   
    def get_data_from_database_for_date_range(self,start_date,end_date):
        
        
        sql_query = """SELECT tran.TRANSACTIONID,tran.TRANSACTION_DESC,tran.ACCOUNTDOCID,tran.ENTERED_DATE, tran.ENTERED_BY as ENTERED_BY_USERID,
        tran.POSTED_DATE,tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.IS_REVERSAL,tran.IS_REVERSED,
        doc.DOCUMENT_TYPE_CODE as DOC_TYPE,com.COMPANY_CODE,
        acctdoc.ACCOUNTDOC_CODE,
        acc.ACCOUNT_CODE,acc.ACCOUNT_DESCRIPTION,
        posted.FMSUSERID as POSTED_BY_USERID,posted.FMSUSER_CODE as POSTED_BY,
        entered.FMSUSER_CODE as ENTERED_BY,tran.PARKED_DATE,tran.LINE_ITEM_TEXT,tran.HEADER_TEXT
        from rpttransaction tran
        left join msfmsuser entered on tran.ENTERED_BY = entered.FMSUSERID
        left join msdocumenttype doc on tran.DOCTYPEID=doc.DOCUMENTID
        left join rptaccountdocument acctdoc on tran.ACCOUNTDOCID=acctdoc.ACCOUNTDOCID
        left join mschartofaccounts acc on tran.ACCOUNTID=acc.ACCOUNTID
        left join mscompany com on tran.COMPANYID=com.COMPANYID
        left join msfmsuser posted on acctdoc.POSTED_BY=posted.FMSUSERID
        WHERE tran.POSTED_DATE BETWEEN '{}' AND '{}';""".format(start_date,end_date)
        
        with self.connect_to_database() as connection:
            df = pd.read_sql(sql_query,con=connection)
        return df
    
    def store_updated_list_of_users_with_few_entries(self,mylist):
        
        """store updated list of users with Few Entries in Database"""
        mylist = json.dumps(list(mylist))
        update_query = """UPDATE trconfiguration set KEYVALUE = '{}'
                        where MODULE ='framework' and KEYNAME ='users_with_few_entries';
                        """.format(mylist)
        print(update_query)
        with self.connect_to_database() as connect:
            with connect.cursor() as cur:
                cur.execute(update_query)
                connect.commit()                
        capture_log_message(log_message="Updated list of users with few entries in database")
        
