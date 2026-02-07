from operator import index
import os,json,mysql.connector
from sqlalchemy import create_engine
import pandas as pd
# from GL_Module.logger import logger
from code1.logger import capture_log_message

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

    def update_scored_transaction(self):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored transactions
        """
        capture_log_message(log_message="Updating Scored Transaction")
        update_query="""UPDATE rpttransaction rpttran
        inner join rpttransactionscore rpttransc on rpttran.TRANSACTIONID = rpttransc.TRANSACTIONID
        set rpttran.IS_SCORED = 1
        where rpttran.IS_SCORED = 0;
        """
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_query)
        connect.commit()
        connect.close()
        capture_log_message(log_message="Update scored_transaction Finished")

    def update_scored_accountdoc(self):
        """
        Updating the IS_SCORED flag from 0 to 1 for the scored accountingdocs
        """
        capture_log_message(log_message="Updating Scored Accountdoc")
        update_query="""UPDATE rptaccountdocument rptaccdoc
        inner join rptaccountdocscore rptaccdocscore on rptaccdoc.ACCOUNTDOCID = rptaccdocscore.ACCOUNTDOCID
        set rptaccdoc.IS_SCORED = 1
        where rptaccdoc.IS_SCORED = 0;
        """
        connect = self.connect_to_database()
        cur = connect.cursor()
        cur.execute(update_query)
        connect.commit()
        connect.close()
        capture_log_message(log_message="Update scored_accountdoc Finished")

    def update_tables(self):
        """
        calling the above functions at once
        """
        # self.update_transaction_table_with_scores()
        # self.update_accountdoc_table_with_scores()
        self.update_scored_transaction()
        self.update_scored_accountdoc()

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

    def read_table(self,tablename, columns=None, audit_id=None, client_id=None):
        """
        Function to read table from database
        """
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
            table = pd.read_sql(query,connection)
        return table


    # def read_table(self,tablename,columns=None):
    #     """
    #     Function to read table from database
    #     """
    #     connection  = self.connect_to_database()
    #     if columns == None:
    #         table = pd.read_sql("""SELECT * FROM {};""".format(tablename),con=connection)
    #     else:
    #         cols = ",".join(columns)
    #         table = pd.read_sql("""SELECT {} FROM {};""".format(cols,tablename),con=connection)
    #     connection.close()
    #     return table

    def read_SRC_table(self,tablename,columns=None):
        """
        Function to read table from database
        """
        connection  = self.connect_to_database()
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