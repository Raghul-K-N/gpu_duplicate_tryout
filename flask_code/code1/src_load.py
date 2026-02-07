'''
Python script to load the data to SRC table and update status in SRC_STATUS table
'''

import json
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
# from code1.logger import logger
from code1.logger import capture_log_message
import time
import os
from flask import g
import utils
from datetime import datetime, timezone
from local_database import get_database_credentials
from Ingestor.fetch_data import get_quarters
import re

credentials = get_database_credentials()
DB_USERNAME = credentials["username"]
DB_PASSWORD = credentials["password"]
DB_HOST = os.getenv("DB_HOST")
DB_PORT= os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SSL_CA_FILE = os.getenv("SSL_CA")
USE_SSL_CA = os.getenv("USE_SSL_CA", "false").lower() == "true"
connect_args = {'ssl': {'ca': SSL_CA_FILE}} if USE_SSL_CA else {'ssl': None}
ssl_args = {'ssl_ca': SSL_CA_FILE } if USE_SSL_CA else {}

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)
historical_data_directory = os.path.join(base_directory,'hist_data')
tracker_directory = os.path.join(historical_data_directory,'tracker')
if not os.path.exists(historical_data_directory):
    os.mkdir(historical_data_directory)
    if not os.path.exists(tracker_directory):
        os.mkdir(tracker_directory)
        df = pd.DataFrame(columns=['audit_id','no_of_rows'])
        df.to_csv(os.path.join(tracker_directory,'hist_data_tracker.csv'),index=False)

def read_table(tablename, columns=None, audit_id=None, client_id=None):

    """
    Function to read table from database
    """
    query = None
    if columns is None:
        if audit_id:
            query = f"SELECT * FROM {tablename} WHERE audit_id = {audit_id};"
        elif client_id:
            query = f"SELECT * FROM {tablename} WHERE client_id = {client_id};"
        else:
            query = f"SELECT * FROM {tablename};"
    else:
        cols = ",".join(columns)
        if audit_id:
            query = f"SELECT {cols} FROM {tablename} WHERE audit_id = {audit_id};"
        elif client_id:
            query = f"SELECT {cols} FROM {tablename} WHERE client_id = {client_id};"
        else:   
            query = f"SELECT {cols} FROM {tablename};"
    # Using `with` to manage the connection
    with connect_to_database() as connection:
        table = pd.read_sql(query, con=connection)

    return table



def test_db_connection():
    """
    Test database connection by connecting to the database and fetching version info
    """
    try:
        connection = mysql.connector.connect(user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME, **ssl_args)
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()") 
        db_version = cursor.fetchone() 
        connection.close() 
        return f"Connected to database version {db_version[0]}"
    except mysql.connector.Error as error: 
        return "Failed to connect to database: {}".format(error)

def upload_data_to_database(data, tablename):
    """
    Upload data to a database
    ------------------------
    Input :
            data : data to be uploaded
            tablename : table to which the data should be uploaded
    """
    engine = create_engine("mysql+pymysql://"+DB_USERNAME+":"+DB_PASSWORD +
                        "@"+DB_HOST+":"+DB_PORT+"/"+DB_NAME,connect_args = connect_args)                                       
    # engine = engine.raw_connection()
    with engine.connect() as connection:
        data.to_sql(tablename, con=connection, index=False, if_exists='append')
    engine.dispose()

def get_filename_src(src_id):
    """
    Getting the filename and mapping id from src_status using src_id of SRC_STATUS
    """
    connection = connect_to_database()
    curr_src = pd.read_sql("""select id, audit_id from SRC_STATUS where CURRENT_RUN = 1 ORDER BY id desc;""",con=connect_to_database())
    curr_audit_id = int(curr_src['audit_id'][0])
    data = pd.read_sql(f"SELECT FILE_NAME,MAPPING_ID FROM SRC_STATUS where id ={src_id} and audit_id = {curr_audit_id};",con=connection)

    return data['FILE_NAME'][0],data['MAPPING_ID'][0]

def get_mapping_values(mapping_id,module, client_id):
    """
    Gets the defined and mapped column names from tronboarddatamapping table with mapping_id
    """
    capture_log_message(log_message=f"Getting mapping values for mapping_id :{mapping_id}",store_in_db=False)
    # data_query = """SELECT tr1.TR_FIELDNAME,tr1.SOURCE_FIELDNAME,tr1.MAPPING_MODULE  from tronboarddatamapping tr1 
    #                     join (select max(CREATED_DATE) as max_date from tronboarddatamapping ) tr2
    #                     where tr1.CREATED_DATE = max_date and
    #                     tr1.MAPPINGROUP_ID= %s and 
    #                     tr1.MAPPING_MODULE = %s and tr1.client_id = %s;"""
    with  connect_to_database() as connection:
        data_query="""SELECT tr1.TR_FIELDNAME,tr1.SOURCE_FIELDNAME,tr1.MAPPING_MODULE  from tronboarddatamapping tr1
                    where tr1.MAPPINGROUP_ID= %s and tr1.MAPPING_MODULE = %s and tr1.client_id = %s;"""
        params = (mapping_id,module,client_id)
        data = pd.read_sql(data_query,con=connection,params=params)

        return data

def fetch_mapping_values_with_retry(mapping_id, module, client_id, max_retries=3, wait_time=2):
    for attempt in range(max_retries):
        try:
            data = get_mapping_values(mapping_id, module, client_id)
            if not data.empty:
                return data
            else:
                capture_log_message(log_message=f"No mapping values found for mapping_id: {mapping_id} on attempt {attempt + 1}", store_in_db=False)
        except Exception as e:
            capture_log_message(log_message=f"Error fetching mapping values for mapping_id: {mapping_id} on attempt {attempt + 1}: {str(e)}", store_in_db=False)
        time.sleep(wait_time)
    raise ValueError(f"Failed to get mapping values for mapping_id: {mapping_id} after {max_retries} attempts")
    
def get_glcheck():
    """
    Get the gl check config details from triniconfigmapping
    """
    with connect_to_database() as connection:
        data_query=f"""select CONFIG_KEY,CONFIG_VALUE from triniconfigmapping
                        where (MAPPINGGROUPID =1);"""
                            
        data = pd.read_sql(data_query,con=connection)

        return data

def get_apcheck():
    """
    Get the ap check config details from triniconfigmapping
    """

    # data_query = f"""select CONFIG_KEY,CONFIG_VALUE from triniconfigmapping
    #                     where (MAPPINGGROUPID =2)
    #                     OR (MAPPINGGROUPID = {mapping_id} and
    #                     created_date = (
    #                         select MAX(created_date) from triniconfigmapping 
    #                         where MAPPINGGROUPID = {mapping_id}) and client_id = {client_id}
    #                     );"""
    
    
                        
    with connect_to_database() as connection:   
        data_query=f"""select CONFIG_KEY,CONFIG_VALUE from triniconfigmapping
                where (MAPPINGGROUPID =2);"""                     
        data = pd.read_sql(data_query,con=connection)
        return data


def read_doctype():
    """
    Gets the unique values of doctype from msdoctype table
    """
    with connect_to_database() as connection:
        data = pd.read_sql("SELECT DOCUMENT_TYPE_CODE,DOCUMENT_TYPE_DESCRIPTION from msdocumenttype;",con=connection)
        return data

def connect_to_database():
    """
    Connect to Database for Fetching Data
    """
    connection = mysql.connector.connect(user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME, **ssl_args)
    return connection

def truncate_table(tablename):
    """
    Function to truncate table
    """
    
    update_job_query = f"TRUNCATE TABLE {tablename};"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_job_query)
    connect.commit()
    connect.close()
    capture_log_message(log_message="Truncated table "+tablename)

def update_job_status_runjob(src_id, run_job, process_details,audit_id):
    """
    Function to update job status in ML Execution table
    """

    # update_job_query = f"INSERT INTO SRC_STATUS(STATUS,MESSAGE,`MODULE`,NUM_RECORDS,RULES_ENABLED) VALUES({status},{process_details},'{module}',{shape},{rules});"
    update_job_query = f"UPDATE SRC_STATUS set MESSAGE = '{process_details}',RUN_JOB={run_job} where id = {src_id} and audit_id = {audit_id};"
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.execute(update_job_query)
            connect.commit()
            capture_log_message("Updated run job status for src_id {} , run_job {} , process_details {}".format(src_id,run_job,process_details))


def update_job_status(status, process_details,src_id,shape,rules,run_job,audit_id):
    """
    Function to update job status in ML Execution table
    """
    process_details = json.dumps(process_details)
    rules = json.dumps(rules)

    # update_job_query = f"INSERT INTO SRC_STATUS(STATUS,MESSAGE,`MODULE`,NUM_RECORDS,RULES_ENABLED) VALUES({status},{process_details},'{module}',{shape},{rules});"
    update_job_query = f"UPDATE SRC_STATUS set STATUS = {status},MESSAGE = {process_details},NUM_RECORDS = {shape},RULES_ENABLED = {rules},RUN_JOB={run_job} where id = {src_id} and audit_id = {audit_id};"
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.execute(update_job_query)
            connect.commit()
            capture_log_message("Updated job status for src_id {} , status {}".format(src_id,status))

def update_ingestion_status(src_id, process_details):
    """
    Function to update job status in ML Execution table
    """
    process_details = json.dumps(process_details)
    
    update_job_query = f"UPDATE SRC_STATUS set MESSAGE = {process_details},STATUS = 2 WHERE id = {src_id};"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_job_query)
    connect.commit()
    connect.close()
    capture_log_message("Updated ingestion status for src_id {} , process_details {}".format(src_id,process_details))

def update_scoring_status(src_id, process_details):
    """
    Function to update job status in ML Execution table
    """
    process_details = json.dumps(process_details)
    
    update_job_query = f"UPDATE SRC_STATUS set MESSAGE = {process_details},STATUS = 3 WHERE id = {src_id};"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_job_query)
    connect.commit()
    connect.close()
    capture_log_message("Updated scoring status for src_id {} , process_details {}".format(src_id,process_details))
    

def get_src_gl_data(preprocessed_data,gl_rules,src_id, audit_id):
    '''
    Function used to receive the preprocessed data
    '''
    ingest_data = preprocessed_data

    # ingest_data['SRC_SID'] = src_id
    
    date_columns = ['INVOICE_DATE','DUE_DATE','POSTED_DATE','ENTERED_DATE','PAYMENT_DATE', 'GRN_DATE']
    date_cols = [i for i in date_columns if i in list(ingest_data.columns)] 
    ingest_data[date_cols] = ingest_data[date_cols].apply(lambda x:pd.to_datetime(x,dayfirst=True))

    mandatory_columns = ['ACCOUNTING_DOC','TRANSACTION_ID_GA','DOC_TYPE','DOC_TYPE_DESCRIPTION','AMOUNT',
                         'SAP_ACCOUNT','ACCOUNT_DESCRIPTION','POSTED_DATE','ENTERED_DATE','POSTED_BY',
                         'SAP_COMPANY','POSTED_LOCATION','DEBIT_CREDIT_INDICATOR','SRC_SID']
    
    recommended_columns = ['ENTERED_BY','TRANSACTION_DESCRIPTION','ENTERED_LOCATION','POSTED_POSITION',
                           'POSTED_DEPARTMENT','ENTERED_BY_POSITION','ENTERED_BY_DEPARTMENT','REVERSAL',
                           'LEDGER','IS_REVERSED','COST_CENTER','WBS_ELEMENT','PARKED_DATE','PARKED_BY',
                           'MATERIAL_NUMBER','HEADER_TEXT','LINE_ITEM_TEXT']
    #Getting the common columns from dataframe column and mandatory,recommended column
    full_columns = mandatory_columns+recommended_columns
    final_column = set(full_columns) & set(ingest_data.columns)
    final_column = list(set(final_column))

    ingest_data=ingest_data.filter(items=final_column)
    capture_log_message(log_message="Source data Shape"+ str(ingest_data.shape))
    capture_log_message(log_message="Source data columns"+ str(ingest_data.columns),store_in_db=False)
    capture_log_message(log_message="Reading data for duplicate check ")

    final_data = ingest_data
    # existing_data = read_table('_SRC_GL_DATA',audit_id=audit_id)
    # capture_log_message(log_message="Duplicate check started")

    # final_data = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(
    #     existing_data['TRANSACTION_ID_GA'])].copy()
    final_data["DEBIT_CREDIT_INDICATOR"] = final_data["DEBIT_CREDIT_INDICATOR"].map({"S":"D","H":"C"})

    capture_log_message(log_message="Final Shape of SRC gl data :"+str(final_data.shape))
    return final_data
    # module = "GL"
    # final_data['audit_id'] = audit_id
    # shape = final_data.shape[0]
    # capture_log_message(log_message='Shape of data before loading into _SRC_GL_DATA :{}'.format(final_data.shape))
    # if final_data.shape[0] != 0:
        # final_data["DEBIT_CREDIT_INDICATOR"] = final_data["DEBIT_CREDIT_INDICATOR"].map({"S":"D","H":"C"})
        # g.src_gl_cols = final_data.columns
        # capture_log_message(log_message=f"Src_gl_cols:{g.src_gl_cols}")
        # upload_data_to_database(final_data, '_SRC_GL_DATA')
        # capture_log_message(log_message="Data uploaded into SRC_GL table")
        
        # process_details = "SRC_GL Table updated"
        # update_job_status(1, process_details,src_id,shape,gl_rules,1, audit_id)
    # else: 
    #     process_details = "SRC_GL Table not updated duplicate Entries"
    #     update_job_status(0, process_details,src_id,shape,gl_rules,0, audit_id)
    #     capture_log_message(log_message="Duplicate Entries")


def get_src_ap_data(preprocessed_data,ap_rules,src_id, audit_id):
    '''
    Function used to receive the preprocessed data
    '''
    ingest_data = preprocessed_data
    # CHANGE
    date_columns = ['INVOICE_DATE','DUE_DATE','POSTED_DATE','ENTERED_DATE','PAYMENT_DATE', 'GRN_DATE', 'PURCHASE_ORDER_DATE']
    date_cols = [i for i in date_columns if i in list(ingest_data.columns)] 
    ingest_data[date_cols] = ingest_data[date_cols].apply(lambda x:pd.to_datetime(x,dayfirst=True))

    # Create a copy of COMPANY_CODE as COMPANY_NAME instead of renaming
    ingest_data['COMPANY_NAME'] = ingest_data['COMPANY_CODE']
    
    if 'DISCOUNT_TAKEN' in list(ingest_data.columns):
        ingest_data['DISCOUNT_TAKEN'].fillna(0,inplace = True)

    #AP columns
    mandatory_columns = ['TRANSACTION_ID_GA','ACCOUNTING_DOC','TRANSACTION_DESCRIPTION','DOC_TYPE','AMOUNT','ENTERED_BY','POSTED_BY','GL_ACCOUNT_TYPE','ACCOUNT_DESCRIPTION','COMPANY_NAME','ENTERED_DATE','INVOICE_DATE','INVOICE_AMOUNT','INVOICE_NUMBER','DUE_DATE','PAYMENT_DATE','SUPPLIER_ID','SUPPLIER_NAME','PAYMENT_TERMS','DISCOUNT_PERCENTAGE_1','DISCOUNT_PERCENTAGE_2','DISCOUNT_TAKEN','PAYMENT_AMOUNT','POSTED_LOCATION','ENTERED_LOCATION','POSTED_DATE','DISCOUNT_PERIOD_1','DISCOUNT_PERIOD_2','CREDIT_PERIOD','DEBIT_AMOUNT','CREDIT_AMOUNT','DEBIT_CREDIT_INDICATOR','INVOICE_STATUS','CLEARING_DOC']
    
    recommended_columns = ['BALANCE_AMOUNT','PURCHASE_ORDER_NUMBER','PAYMENT_METHOD','BANK_ACCOUNT_NUMBER','PAYMENT_TERMS_DESCRIPTION','GRN_NUMBER','GRN_DATE','PURCHASE_ORDER_DATE','BASELINE_DATE','REQUISITION_DATE','PURCHASE_REQUEST_NUMBER','ENTERED_DATE','APPROVED_USER_1','APPROVED_USER_2','APPROVED_USER_3','APPROVED_USER_4','APPROVED_USER_5','LEGAL_ENTITY_NAME_AND_ADDRESS','VAT_ID','INVOICE_CURRENCY','FISCAL_YEAR','CLIENT']
    #Getting the common columns from dataframe column and mandatory,recommended column
    full_columns = mandatory_columns+recommended_columns
    final_column = set(full_columns) & set(ingest_data.columns)

    # ingest_data=ingest_data.filter(items=final_column)
    capture_log_message(log_message="Source data Shape"+ str(ingest_data.shape))
    capture_log_message(log_message='Reading data for duplicate check ')

    
    # Check whether historical data is given, if Yes , separate historical_data and current data
    
    # store only current data in SRC_AP_DATA table
    final_data = ingest_data

    capture_log_message(log_message="Shape of Dataframe after Filtering"+str(final_data.shape))
    
    # Update Account_number column
    final_data['ACCOUNT_NUMBER'] = final_data['GL_ACCOUNT_TYPE']
    # final_data['audit_id'] = audit_id
    # module = "AP"
    capture_log_message(log_message='Final Shape of data :{}'.format(final_data.shape))
    
    return final_data
    #     process_details = "SRC_AP Table updated"
    #     update_job_status(1, process_details,src_id,shape,ap_rules,1, audit_id)

def update_flat_table():
    """
    Function to update flat table
    """

    update_doc_query = "insert into rptaccountdocumentflat select * from rptaccountdocumentflatview;"
    update_tran_query = "insert into rpttransactionflat select * from rpttransactionflatview;"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_doc_query)
    cur.execute(update_tran_query)
    connect.commit()
    connect.close()
    capture_log_message(log_message="Updated Flat Tables for GL")

def update_apflat_table():
    """
    Function to update flat table
    """

    update_doc_query = "insert into apaccountdocumentsflat select * from apaccountdocumentsflatview;"
    update_tran_query = "insert into aptransactionflat select * from aptransactionflatview;"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_doc_query)
    cur.execute(update_tran_query)
    connect.commit()
    connect.close()
    capture_log_message(log_message="Updated Flat Tables for AP")

def update_current_job(src_id, value, audit_id):
    '''
    Function to update current_job column in SRC_STATUS
    '''
    # curr_src = pd.read_sql("""select id, audit_id from SRC_STATUS where CURRENT_RUN = 1 ORDER BY id desc;""",con=connect_to_database())
    # curr_audit_id = int(curr_src['audit_id'][0])
    update_job_query = f"UPDATE SRC_STATUS set CURRENT_RUN = {value}  WHERE id = {src_id} and audit_id = {audit_id}"
    with  connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.execute(update_job_query)
            connect.commit()
            

def save_temp_filename(src_id,filename, audit_id):
    '''
    Function to save the temp filename to SRC_STATUS
    '''
    capture_log_message(f'Saving table names:{filename} for audit_id :{audit_id} in database')
    update_job_query = f"UPDATE SRC_STATUS set TEMP_TABLE = '{filename}'  WHERE id = {src_id} and audit_id = {audit_id};"
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            cur = connect.cursor()
            cur.execute(update_job_query)
            connect.commit()

def get_temp_data(src_id, audit_id):
    '''
    Function to get the temp data
    '''
    with connect_to_database() as connection:
        temp_name = pd.read_sql(
            f"SELECT TEMP_TABLE from SRC_STATUS where id = {src_id} and audit_id= {audit_id};",
            con=connection
        )
        temp_name = temp_name['TEMP_TABLE'][0]
        fetch_query = f"SELECT * FROM {temp_name};"
        temp_data = pd.read_sql(fetch_query, con=connection)
    return temp_data

def create_errorlist_col(src_id,audit_id):
    '''
    Function to create errorlist column
    '''
    temp_name = pd.read_sql(f"SELECT TEMP_TABLE from SRC_STATUS where id = {src_id} and audit_id= {audit_id};",con=connect_to_database())
    temp_name = temp_name['TEMP_TABLE'][0]
    error_col = f"ALTER TABLE {temp_name} ADD errorlist VARCHAR(255),ADD ERRORFLAG int NOT NULL DEFAULT(0);"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(error_col)
    connect.commit()
    connect.close()

def errorlist_to_temp(src_id,column_values,row_num, audit_id):
    '''
    Function to write the error output to errolist column in temp table
    '''
    capture_log_message(log_message="Executing errorlist_to_temp function, src_id :{}, columns:{},No. of rows :{}".format(src_id,column_values,len(row_num)),store_in_db=False)
    with connect_to_database() as connection:
        temp_name = pd.read_sql(f"SELECT TEMP_TABLE from SRC_STATUS where id = {src_id} and audit_id={audit_id};",con=connection)
        temp_name = temp_name['TEMP_TABLE'][0]
        if len(row_num)==1:
            val = row_num[0]
            row_num = '('+str(val)+')'
        errorlist = f"UPDATE {temp_name} SET ERRORFLAG=1, errorlist = CONCAT_WS('|',COALESCE(errorlist,''), {column_values},',') WHERE ROW_NUM in {row_num};"
        with connection.cursor() as cur:
            cur.execute(errorlist)
            connection.commit()
        capture_log_message(log_message="Errorlist query :{}".format(errorlist),store_in_db=False)
    

# def errorrows_to_src(src_id,rows):
#     '''
#     Function to write unique rows to src_status in ERROR_ROWS Column
#     '''
#     update_job_query = "UPDATE SRC_STATUS SET ERROR_ROWS = %s WHERE id = %s"
#     val = (rows, src_id)
#     connect = connect_to_database()
#     cur = connect.cursor()
#     cur.execute(update_job_query,val)
#     connect.commit()
#     connect.close()

def get_temp_tablename_AP(src_id, audit_id):
    '''
    Function to get the TEMP_TABLE name from SRC_STATUS
    '''
    with connect_to_database() as connection:
        temp_name = pd.read_sql(f"SELECT TEMP_TABLE from SRC_STATUS where id = {src_id} and audit_id= {audit_id};",con=connection)
    temp_name = temp_name['TEMP_TABLE'][0]
    gltemp,aptemp = temp_name.split(',')
    return gltemp,aptemp

def create_errorlist_col_ap(temp_name):
    '''
    Function to create errorlist and error flag column in temp table
    '''
    # error_col = f"ALTER TABLE {temp_name} ADD errorlist VARCHAR(255),ADD ERRORFLAG int NOT NULL DEFAULT(0);"
    try:
        capture_log_message(f"Adding error list col for {temp_name}")
        error_col = f"ALTER TABLE {temp_name} ADD errorlist VARCHAR(255),ADD ERRORFLAG int NOT NULL DEFAULT(0);"
        connect = connect_to_database()
        capture_log_message(f'inside create_errorlist_col_ap, connected to database {connect}')
        cur = connect.cursor()
        capture_log_message(f'Inside create_errorlist_col_ap, query is {error_col}')
        cur.execute(error_col)
        connect.commit()
        capture_log_message('Command is executed and new columns are added')
        
    except mysql.connector.Error as e:
        capture_log_message(log_message=f'Database error: {e}')

    except Exception as e:
        capture_log_message(log_message=f'Unexpected Error {e}')
    finally:
        if connect.is_connected():
            connect.commit()
            cur.close()
            connect.close()

def errorlist_to_temp_ap(temp_name,column_values,row_num):
    '''
    Function to write the error output to errolist column in temp table
    '''
    if len(row_num)==1:
        val = row_num[0]
        row_num = '('+str(val)+')'
    
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            errorlist = f"UPDATE {temp_name} SET ERRORFLAG=1, errorlist = CONCAT_WS('|',COALESCE(errorlist,''), {column_values},',') WHERE ROW_NUM in {row_num};"
            cur.execute(errorlist)
            connect.commit()

def get_temp_dataUI_AP(src_id,audit_id):
    '''
    Function to get the TEMP_TABLE name for AP from SRC_STATUS
    '''
    capture_log_message(f'Fetching temp data for audit id:{audit_id} and src_id:{src_id}')
    with connect_to_database() as connection:
        temp_name = pd.read_sql(f"SELECT TEMP_TABLE from SRC_STATUS where id = {src_id} and audit_id={audit_id};",con=connection)

    capture_log_message(f'Database results: {temp_name}')
    temp_name = temp_name['TEMP_TABLE'][0]
    capture_log_message(f'Fetched temp table names are :{temp_name}')
    gltemp,aptemp = temp_name.split(',')
    fetch_query = f"SELECT * FROM {aptemp};"
    with connect_to_database() as connection:
        temp_data = pd.read_sql(fetch_query,con=connection)
    return temp_data

def update_health_status(process_details,src_id, audit_id):
    """
    Function to update health status in SRC_STATUS table
    """
    process_details = json.dumps(process_details)


    update_job_query = f"UPDATE SRC_STATUS set MESSAGE = {process_details} where id = {src_id} and audit_id= {audit_id};"

    with connect_to_database() as connect:
        with connect.cursor() as cur:
            cur.execute(update_job_query)
            connect.commit()
            
    capture_log_message("Updated health status for src_id {} , process_details {}".format(src_id,process_details))

def get_gl_ap_cols(mapping_cols,column,src_id, audit_id):
    gl_temp,ap_temp = get_temp_tablename_AP(src_id, audit_id)
    gl_column = pd.read_sql(f"show COLUMNS from {gl_temp};",con=connect_to_database())
    ap_column = pd.read_sql(f"show COLUMNS from {ap_temp};",con=connect_to_database())
    glcols =gl_column['Field'].to_list()
    apcols =ap_column['Field'].to_list()
    for map in mapping_cols:
        trName = map[0]
        srcName = map[1]
        if column in trName and srcName in glcols:
            return "GLROW"
        elif column in trName and srcName in apcols:
            return "APROW"

def get_client_id_and_erp_id(hist_id):
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query ="""select client_id,erp_id from client_historical_data
                        where client_historical_data_id= %s"""
                cursor.execute(query,(hist_id,))
                results = cursor.fetchall()
                if len(results)>0:
                    return results[0][0],results[0][1]
                else:
                    return None,None
    except Exception as e:
        if hasattr(g,'error_logger'):
            g.error_logger.debug(str(e))
        return None,None
    
def get_input_file_path_for_hist_id(hist_id):
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """SELECT temp_folder_location FROM client_historical_data
                           WHERE client_historical_data_id = %s"""
                cursor.execute(query, (hist_id,))
                results = cursor.fetchall()
                if len(results)>0:
                    return results[0][0]
                else:
                    return None
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e),error_name=utils.DB_CONNECTION_ISSUE)
        return None
    
    
def add_entry_in_lookup_src_table(hist_id,quarter,year,src_name):
    capture_log_message(log_message=f'Inserting data into lookup_src table , hist_id {hist_id},Quarter {quarter},Year {year}, SRC_NAME {src_name}')
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """INSERT INTO lookup_src(client_historical_id, quarter, year, src_name) 
                           VALUES (%s, %s, %s, %s) """
                cursor.execute(query, (hist_id, quarter, year, src_name))
                src_id = cursor.lastrowid
                connection.commit()
                capture_log_message(log_message=f'Values inserted into lookup_src table , src_id {src_id}')
                return src_id
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))
        return None
    
def add_entry_in_batch_lookup_src_table(batch_id,pqt_filename,year,quarter,month,no_of_records):
    capture_log_message(log_message=f'Inserting data into batch_lookup_src table , batch_id {batch_id},PQT_FILENAME {pqt_filename},Year {year},Quarter {quarter},No of records {no_of_records}')
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query= """INSERT into batch_lookup_src(batch_id,pqt_filename,year,quarter,month,number_of_records) values (%s,%s,%s,%s,%s,%s)"""
                values = (batch_id,pqt_filename,year,quarter,month,no_of_records)
                cursor.execute(query,values)
                connection.commit()
                capture_log_message(log_message='Values inserted into batch_lookup_src table')
                return True
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))
        return None
    
    
def fetch_src_id_from_lookup_src_table(hist_id,quarter,year):
    capture_log_message(f'Fetching src_id from lookup_src table for hist_id {hist_id},Quarter {quarter},Year {year}')
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """select src_id from lookup_src where quarter = %s 
                and year = %s and client_historical_id =  %s"""
                cursor.execute(query,(quarter,year,hist_id,))
                results = cursor.fetchall()
                if len(results) >0:
                    capture_log_message(f" SRC_ID is :{results[0][0]}")
                    return results[0][0]
                else:
                    return None
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))
        return None
    
    
def add_snapshot_data_in_src_snapshot_table(src_id,no_of_records,uploaded_by,uploaded_at):
    capture_log_message(f"Adding row in src_snapshot table for src_id {src_id},no_of_Records {no_of_records},Uploaded by {uploaded_by}, uploaded at {uploaded_at}")
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """ INSERT INTO src_snapshot(src_id,number_of_records,sync,uploaded_by,uploaded_at,upload_status)
                            values (%s , %s, %s, %s, %s , %s)"""
                cursor.execute(query,(src_id,no_of_records,False,uploaded_by,uploaded_at,4,))
                connection.commit()
                return True
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))
        return False
    
    
def update_record_in_client_historical_data(hist_id,time_start,time_end,no_of_records,modified_by,modified_at,status_id):
    capture_log_message(f'Updating record in client_historical_data for Hist id {hist_id} , Starttime {time_start} end time {time_end}, No of records {no_of_records},Modified by {modified_by}, Modified at {modified_at}')
    try:
        time_start = time_start.strftime("%Y-%m-%d %H:%M:%S")
        time_end = time_end.strftime("%Y-%m-%d %H:%M:%S")
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """ update client_historical_data set time_range_start_date = %s , time_range_end_date = %s,
                            number_of_records = %s , updated_by = %s , updated_at = %s  , status = %s
                            where client_historical_data_id = %s """
                cursor.execute(query,(time_start,time_end,no_of_records,modified_by,modified_at,status_id,hist_id,))
                connection.commit()
                capture_log_message('Record updated suceessfully ')
                return True
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, log_message=str(e))
        return False

def fetch_details_from_client_historical_data(hist_id,):
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """ select time_range_start_date,time_range_end_date,number_of_records  from
                            client_historical_data where client_historical_data_id = %s"""
                cursor.execute(query,(hist_id,))
                results = cursor.fetchall()
                if len(results)>0:
                    return results[0][0],results[0][1],results[0][2]
                else:
                    return None,None,None
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, log_message=str(e))
        return None,None,None
        
    
def update_final_values_in_client_historical_data_table(hist_id,start_date,end_date,no_of_records,modified_by,modified_at):
    try:
        capture_log_message('Fetch Existing Values from Client_historical_data table')
        new_start_date = pd.to_datetime(start_date)
        new_end_date = pd.to_datetime(end_date)
        final_start_date = None
        final_end_date = None
        new_records_count = 0
        existing_start_date,existing_end_date,existing_records = fetch_details_from_client_historical_data(hist_id=hist_id)
        capture_log_message(f" Existing data in client_historical_data table for hist_id {hist_id}")
        capture_log_message(f"Existing values: Start time :{existing_start_date}, End time :{existing_end_date},No. of records:{existing_records}")
        if existing_start_date is not None:
            existing_start_date = pd.to_datetime(existing_start_date)
            
            if new_start_date < existing_start_date:
                final_start_date = new_start_date
            else:
                final_start_date = existing_start_date
        else:
            final_start_date = new_start_date
        
        if existing_end_date is not None:
            existing_end_date = pd.to_datetime(existing_end_date)
            new_end_date = pd.to_datetime(end_date)
            if new_end_date > existing_end_date:
                final_end_date = new_end_date
            else:
                final_end_date = existing_end_date
        else:
            final_end_date = new_end_date
        
        if existing_records is not None:
            new_records_count = existing_records+no_of_records
        else:
            new_records_count = no_of_records
            
        status = update_record_in_client_historical_data(hist_id=hist_id,time_start=final_start_date,
                                                         time_end=final_end_date,no_of_records=new_records_count,
                                                         modified_by=modified_by,modified_at=modified_at,status_id=4)
        return status  
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))
        return False
    
def get_ap_columns_to_create_uuid():
    """ This function fetches the list of columns to create UUID for each client and ERP combination"""
    
    return ['ACCOUNTING_DOC','FISCAL_YEAR','COMPANY_CODE','CLIENT']
    

def get_zblock_columns_to_create_uuid():
    """ This function fetches the list of columns to create UUID for each client and ERP combination"""
        
    return ['ACCOUNTING_DOC','FISCAL_YEAR','COMPANY_CODE','CLIENT']
    
    
def get_duplicate_invoice_threshold_value(use_audit_id = False) -> float:
    try:
        if use_audit_id:
            query = """ select keyvalue from trconfiguration where module = %s and keyname = %s and audit_id = %s"""
        else:
            query = """ select keyvalue from trconfiguration where module = %s and keyname = %s"""
            
        capture_log_message(f"Getting duplicate invoice threshold for audit id {g.audit_id}")
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                if use_audit_id:
                    cursor.execute(query,('apframework','duplicate_invoice_threshold',g.audit_id))
                else:
                    cursor.execute(query,('apframework','duplicate_invoice_threshold'))
                 
                results = cursor.fetchall()
                if len(results):
                    capture_log_message(f"Fetched threshold value is {results[0][0]}")
                    return float(results[0][0])
                else:
                    capture_log_message(f'Could not fetch duplicate invoice threshold value for this audit {g.audit_id}')
                    return 60
                
                
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=str(e))   
        return 60
    


def create_new_batch_entry(module_nm):
    """This functions inserts the timestamp into the batch table and returns the last row id"""

    try:
        capture_log_message("Creating new batch entry")
        query = "INSERT INTO batch (uploaded_at, module) VALUES (%s, %s);"
        values = (datetime.now(timezone.utc), module_nm, )
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()
                result = cursor.lastrowid
                return result
    except Exception as e:
        error_message = "Cannot create new batch entry "+str(e)
        capture_log_message(current_logger=g.error_logger,log_message=error_message)
        return None

def update_run_timestamp_in_batch_table(batch_id,run_timestamp):
    try:
        capture_log_message(f"Updating run_timestamp in batch table for batch_id {batch_id}")
        query = "UPDATE batch SET run_timestamp = %s WHERE batch_id = %s;"
        values = (run_timestamp, batch_id)
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()

        return True
    except Exception as e:
        error_message = "Cannot update run_timestamp in batch table "+str(e)
        capture_log_message(current_logger=g.error_logger,log_message=error_message)
        return False

def fetch_run_timestamp_for_batch_id(batch_id):
    try:
        capture_log_message(f"Fetching run_timestamp for batch_id {batch_id}")
        query = "SELECT run_timestamp FROM batch WHERE batch_id = %s;"
        values = (batch_id,)
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        error_message = "Cannot fetch run_timestamp for batch_id "+str(e)
        capture_log_message(current_logger=g.error_logger,log_message=error_message)
        return None

    
    
def update_status_in_pipeline_run_table(batch_id, pipeline, current_status, status_message=None):
    """
    Update the status of a pipeline_run record using batch_id and pipeline as filters.
    
    Args:
        batch_id: The batch identifier
        pipeline: The pipeline name (e.g., 'ZBLOCK', 'AP')
        current_status: The status value to set
        status_message: Optional message describing the status/success/failure details
        
    Returns:
        True if update successful, None if exception occurs
    """
    try:
        finished_at = datetime.now(timezone.utc)
        capture_log_message(f"Updating status in pipeline_run_table for batch_id {batch_id}, pipeline {pipeline}, current_status {current_status}, status_message {status_message}")
        
        if status_message:
            query = "UPDATE pipeline_run SET status = %s, status_message = %s, finished_at = %s WHERE batch_id = %s AND pipeline = %s;"
            values = (current_status, status_message, finished_at, batch_id, pipeline)
        else:
            query = "UPDATE pipeline_run SET status = %s, finished_at = %s WHERE batch_id = %s AND pipeline = %s;"
            values = (current_status, finished_at, batch_id, pipeline)
        
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()
                capture_log_message(f"Successfully updated status to {current_status} for batch_id {batch_id}, pipeline {pipeline}, message: {status_message}")
                return True
    except Exception as e:
        error_message = "Cannot update status in pipeline_run_table " + str(e)
        capture_log_message(current_logger=g.error_logger, log_message=error_message)
        return None



def update_current_step_in_pipeline_run_table(batch_id, pipeline, current_step):
    """
    This function updates the current_step column in the pipeline_run table
    """
    try:
        finished_at = datetime.now(timezone.utc)
        capture_log_message(f"Updating current_step in pipeline_run_table for batch_id {batch_id}, pipeline {pipeline}, current_step {current_step}")

        query = "UPDATE pipeline_run SET current_step = %s, finished_at = %s WHERE batch_id = %s AND pipeline = %s;"
        values = (current_step, finished_at, batch_id, pipeline)
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()
                capture_log_message(f"Successfully updated current_step to {current_step} for batch_id {batch_id}, pipeline {pipeline}")
                return True
        
    except Exception as e:
        error_message = "Cannot update current_step in pipeline_run_table "+str(e)
        capture_log_message(current_logger=g.error_logger,log_message=error_message)
        return None

def add_no_of_rows_present_in_the_uploaded_data_in_batch_metadata(batch_id, no_of_rows, data_start, data_end,no_of_acc_docs,comments):
    """
    This function updates the no_of_rows column in the batch_metadata_table
    """
    try:
        capture_log_message(f"Updating no_of_rows in batch_metadata_table for batch_id {batch_id} with no_of_rows {no_of_rows} and comments {comments}")

        query = "INSERT INTO batch_metadata (batch_id, number_of_records, number_of_acc_docs, data_start, data_end,comments) VALUES (%s, %s, %s, %s, %s, %s);"

        values = (batch_id, no_of_rows, no_of_acc_docs, data_start, data_end, comments)

        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()
                return True
        
    except Exception as e:
        error_message = "Cannot update no_of_rows in batch_metadata_table "+str(e)
        capture_log_message(current_logger=g.error_logger,log_message=error_message)
        return None
    

# def update_end_time_for_batch_in_batch_metadata_table(batch_id):
#     """
#     This function updates the end_time column in the batch table
#     """
#     try:
#         capture_log_message(f"Updating end_time in batch_metadata table")
#         query = "UPDATE batch_metadata SET upload_end = %s WHERE batch_id = %s;"
#         values = (datetime.now(timezone.utc),batch_id)
#         with connect_to_database() as connection:
#             with connection.cursor() as cursor:
#                 cursor.execute(query, values)
#                 connection.commit()
#                 capture_log_message("End time updated in batch_metadata table")
#                 return True
#     except Exception as e:
#         error_message = "Cannot update end_time in batch_metadata_table "+str(e)
#         capture_log_message(current_logger=g.error_logger,log_message=error_message)
#         return None
    
    
    
def fetch_iv_module_data(fields: list, vendor_ids:list): 
    """Fetch anomaly data for all vendors across quarters."""
    try:
        #TODO: get Quarters list
        # quarters = ['q1_2024', 'q2_2024', 'q3_2024']
        quarters = get_quarters(g.batch_id)
        capture_log_message(f'Quarters list is :{quarters}')

        # Build a single parameterized query for all quarters
        union_queries = []
        vendor_ids_tuple = (tuple(vendor_ids))
        # fields.append('VENDORID')
        
        for qtr in quarters:
            union_queries.append(f"""
                SELECT ip.*, iv.* 
                FROM invoice_verification_{qtr} AS iv
                INNER JOIN invoice_processing_{qtr} AS ip 
                ON iv.transaction_id = ip.transaction_id
                WHERE ip.vendor_id IN {vendor_ids_tuple}
            """)
        
        full_query = " UNION ALL ".join(union_queries)
        capture_log_message(f'query: {full_query}')
        
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(full_query)
                results = cursor.fetchall()
                
                if results:
                    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                    df.rename(columns={'vendor_id':'VENDORID'}, inplace=True)
                    capture_log_message(f"Fetched {len(df)} records across {len(quarters)} quarters and columns are {df.columns}")

                    list_of_cols = ['VENDORID'] + fields
                    return df[list_of_cols]
                else:
                    capture_log_message("No anomaly data found")
                    return pd.DataFrame()  # Return empty DF instead of None

    except Exception as e:
        error_msg = f"Invoice Verification Module Data Fetch failed: {str(e)}"
        capture_log_message(current_logger=g.error_logger, log_message=error_msg)
        return pd.DataFrame()

def df_to_dict(df, id_col, value_col, remove_duplicates=True):
    """
    Enhanced version with option to remove duplicate values per ID
    Ensures all IDs are included in result, even if they have no valid values
    
    Args:
        df: pandas DataFrame
        id_col: name of the ID column
        value_col: name of the value column
        remove_duplicates: whether to remove duplicate values for each ID
    
    Returns:
        dict: {id: [list of unique non-empty string values]}
    """
    if df.empty:
        return {}
    df[id_col] = df[id_col].astype(str)
    all_ids = df[id_col].unique()
    
    df_clean = df.copy()
    df_clean[value_col] = df_clean[value_col].astype(str)
    
    mask = ((df_clean[value_col] != 'nan') & (df_clean[value_col] != '') & (df_clean[value_col].str.strip() != '') & (df_clean[value_col] != 'None'))
    df_filtered = df_clean[mask]
    
    if remove_duplicates:
        result_dict = (
            df_filtered.drop_duplicates(subset=[id_col, value_col])
            .groupby(id_col)[value_col]
            .apply(list)
            .to_dict()
        )
    else:
        result_dict = df_filtered.groupby(id_col)[value_col].apply(list).to_dict()
    
    for id_val in all_ids:
        if id_val not in result_dict:
            result_dict[id_val] = []
    
    return result_dict

# def payment_terms_extraction(payment_terms):
#     """
#     Function to extract payment terms description from the payment terms column
#     """
#     if not payment_terms or str(payment_terms).strip() == "":
#         return None, None, None
#     payment_terms = str(payment_terms).strip().lower()
#     discount_period_keys = ['days']
#     credit_period_keys = ['days net','days due','net']
#     seperators = ['or','and','&',',','.','/','\\',"otherwise",'or else','else']
#     payment_terms_split = payment_terms.split()
#     payment_terms_split = [s.strip().lower() for s in payment_terms_split if s.strip()]
#     secret = "@#$"

#     credit_period = None
#     discount_period = None
#     discount_percentage = None

#     if not(bool(re.search(r'\d', payment_terms))):
#         return discount_percentage, discount_period, credit_period
    
#     discount_percentage_extract = re.search(r'\d+(\.\d+)?\s*%', payment_terms)
#     if discount_percentage_extract:
#         discount_percentage = float(discount_percentage_extract.group(0).split('%')[0])
#         payment_terms = (payment_terms.replace(discount_percentage_extract.group(0), secret))
#         numbers = [num for num in re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms) if num != discount_percentage_extract]
#         if len(numbers) == 1:
#             flag = any(key in payment_terms for key in discount_period_keys)
#             if flag:
#                 discount_period = numbers[0]
#                 return discount_percentage, discount_period, credit_period
#         elif len(numbers) >= 2:
#             # payment_terms_string = payment_terms.replace(discount_percentage_extract.group(0), secret)
#             for sep in seperators:
#                 if sep in payment_terms:
#                     parts = payment_terms.split(sep)
#                     for part in parts:
#                         part = part.strip()
#                         if secret in part:
#                             key_to_right = {s: payment_terms_split[i+1] for i, s in enumerate(payment_terms_split[:-1]) if s in ["net"] and i+1 < len(payment_terms_split) and payment_terms_split[i+1].isdigit()}
#                             key_to_left = {s: payment_terms_split[i-1] for i, s in enumerate(payment_terms_split[1:], 1) if s in ["days net","days due"] and i-1 != -1 and payment_terms_split[i-1].isdigit()}
#                             if any(key in payment_terms for key in discount_period_keys):
#                                 discount_period = int(re.findall(r'\b\d+\.\d+|\b\d+\b', part)[0]) if re.findall(r'\b\d+\.\d+|\b\d+\b', part) else None
#                             # Found secret, now search for keys in this part
#                         elif any(key in payment_terms for key in credit_period_keys):
#                             credit_period = int(re.findall(r'\b\d+\.\d+|\b\d+\b', part)[0]) if re.findall(r'\b\d+\.\d+|\b\d+\b', part) else None
#                             # Early exit if both found
#                         if credit_period and discount_period:
#                             return discount_percentage, discount_period, credit_period
#                     # Early exit if secret found in any separator
#                     if credit_period or discount_period:
#                         break
            
#             return discount_percentage, discount_period, credit_period
#     else:
#         numbers = re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms)
#         if len(numbers) == 1:
#             flag = any(key in payment_terms for key in credit_period_keys)
#             if flag:
#                 credit_period = numbers[0]
#                 return discount_percentage, discount_period, credit_period
#         elif len(numbers) >= 2:
#             key_to_right = {s: payment_terms_split[i+1] for i, s in enumerate(payment_terms_split[:-1]) if s in ["net"] and i+1 < len(payment_terms_split) and payment_terms_split[i+1].isdigit()}
#             key_to_left = {s: payment_terms_split[i-1] for i, s in enumerate(payment_terms_split[1:], 1) if s in ["days"] and i-1 != -1 and payment_terms_split[i-1].isdigit()}
#             if key_to_right:
#                 credit_period = next(iter(key_to_right.values()))
#             elif key_to_left:
#                 credit_period = next(iter(key_to_left.values()))
#             return discount_percentage, discount_period, credit_period
    
#     return discount_percentage, discount_period, credit_period
# def find_closest_numbers(text, target_word):
#     """Compact but readable version."""
#     nums = [(int(m.group()), m.start(), m.end()) for m in re.finditer(r'\b\d{1,2}\b', text)]
#     tgts = [(m.start(), m.end()) for m in re.finditer(rf'\b{re.escape(target_word)}\b', text, re.I)]
    
#     if not nums or not tgts:
#         return None
    
#     # Calculate distances and build result
#     result = []
#     for n, ns, ne in nums:
#         min_dist = min(min(abs(ne-ts), abs(ns-te), abs(ns-ts), abs(ne-te)) for ts, te in tgts)
#         closest_target = min(tgts, key=lambda t: min(abs(ne-t[0]), abs(ns-t[1]), abs(ns-t[0]), abs(ne-t[1])))
#         result.append({'number': n, 'position': (ns, ne), 'distance': min_dist, 'target_position': closest_target})
    
#     result.sort(key=lambda x: x['distance'])
#     min_distance = result[0]['distance']
#     optimal = [x for x in result if x['distance'] == min_distance]
    
#     return int([x['number'] for x in optimal][0]) if [x['number'] for x in optimal] else None

# def payment_terms_extraction(payment_terms, take_last_discount:bool = True):
#     """
#     Function to extract payment terms description from the payment terms column
#     """
#     if not payment_terms or str(payment_terms).strip() == "":
#         return None, None, None
#     payment_terms = str(payment_terms).strip().lower()
#     payment_terms = str(re.sub(r'(\d+),(\d+)(\s*[%％])', r'\1.\2\3', payment_terms))
#     discount_period_keys = ['days']
#     credit_period_keys = ['days net','days due','net']
#     seperators = ['or','and','&',',','.','/','\\',"otherwise"]
#     payment_terms_split = payment_terms.split()
#     payment_terms_split = [s.strip().lower() for s in payment_terms_split if s.strip()]
#     secret = "@#$"

#     credit_period = None
#     discount_period = None
#     discount_percentage = None

#     if not(bool(re.search(r'\d', payment_terms))):
#         return discount_percentage, discount_period, credit_period
    
#     discount_percentage_extract = re.findall(r'(\d+(?:\.\d+)?)\s*[%％]', payment_terms)
#     if discount_percentage_extract:
#         discount_percentage = float(discount_percentage_extract[0] if not(take_last_discount) else discount_percentage_extract[-1])
#         numbers = [num for num in re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms) if num not in discount_percentage_extract]
#         if len(numbers) == 1:
#             discount_period_keys_special = ['days','net'] if 'net' in payment_terms else discount_period_keys
#             flag = any(key in payment_terms for key in discount_period_keys)
#             if flag:
#                 discount_period = numbers[0]
#                 return discount_percentage, discount_period, credit_period
#         elif len(numbers) >= 2:
#             percentage_strings = [f"{str(discount_percentage_extract[-1])}%",f"{str(discount_percentage_extract[-1])} %",f"{str(discount_percentage_extract[-1])}％",f"{str(discount_percentage_extract[-1])} ％"]
#             for sep in seperators:
#                 if sep in payment_terms:
#                     parts = payment_terms.split(sep)
#                     for part in parts:
#                         part = part.strip()
#                         if any(substring in part for substring in percentage_strings) and discount_period is None:
#                             if len(discount_percentage_extract)>1 and any(substring in part for substring in percentage_strings):
#                                 key = [key for key in discount_period_keys if key in part]
#                                 if key:
#                                     discount_period = find_closest_numbers(text=part, target_word=key[0])
#                             elif len(discount_percentage_extract)==1 and any(key in part for key in discount_period_keys):
#                                 key = [key for key in discount_period_keys if key in part]
#                                 if key:
#                                     discount_period = find_closest_numbers(text=part, target_word=key[0])
#                             # Found secret, now search for keys in this part
#                         else:# not(str(discount_percentage_extract[-1]) in part):
#                             key = [key for key in credit_period_keys if key in part]
#                             if key:
#                                 credit_period = find_closest_numbers(text=part, target_word=key[0])
#                         if credit_period and discount_period:
#                             return discount_percentage, discount_period, credit_period
#                     # Early exit if secret found in any separator
#                     if credit_period or discount_period:
#                         break
                
#             return discount_percentage, discount_period, credit_period
#     else:
#         numbers = re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms)
#         if len(numbers) == 1:
#             credit_period_keys.append("within")
#             flag = any(key in payment_terms for key in credit_period_keys)
#             if flag:
#                 credit_period = numbers[0]
#                 return discount_percentage, discount_period, credit_period
#         elif len(numbers) >= 2:
#             key = [key for key in credit_period_keys if key in payment_terms]
#             if key:
#                 credit_period = find_closest_numbers(text=payment_terms, target_word=key[0])
#             return discount_percentage, discount_period, credit_period
    
#     return discount_percentage, discount_period, credit_period

def find_closest_numbers(text, target_word):
    """
    Find the closest number to a target word in a given text.
    
    This function identifies all 1-2 digit numbers in the text and finds which one
    is closest to any occurrence of the target word based on character positions.
    
    Args:
        text (str): The input text to search in
        target_word (str): The word to find proximity to numbers
        
    Returns:
        int or None: The closest number to the target word, or None if no numbers/targets found
    """
    # Find all 1-2 digit numbers with their positions
    nums = [(int(m.group()), m.start(), m.end()) for m in re.finditer(r'\b\d{1,2}\b', text)]
    
    # Find all occurrences of target word (case insensitive) with their positions
    tgts = [(m.start(), m.end()) for m in re.finditer(rf'\b{re.escape(target_word)}\b', text, re.I)]
    
    # Return None if no numbers or targets found
    if not nums or not tgts:
        return None
    
    # Calculate distances between each number and all target word occurrences
    result = []
    for n, ns, ne in nums:  # n=number, ns=number_start, ne=number_end
        # Calculate minimum distance from this number to any target word
        min_dist = min(min(abs(ne-ts), abs(ns-te), abs(ns-ts), abs(ne-te)) for ts, te in tgts)
        
        # Find the closest target word occurrence
        closest_target = min(tgts, key=lambda t: min(abs(ne-t[0]), abs(ns-t[1]), abs(ns-t[0]), abs(ne-t[1])))
        
        # Store results for this number
        result.append({
            'number': n, 
            'position': (ns, ne), 
            'distance': min_dist, 
            'target_position': closest_target
        })
    
    # Sort by distance (closest first)
    result.sort(key=lambda x: x['distance'])
    
    # Get minimum distance and all numbers with that distance
    min_distance = result[0]['distance']
    optimal = [x for x in result if x['distance'] == min_distance]
    
    # Return the first number with minimum distance
    return int([x['number'] for x in optimal][0]) if [x['number'] for x in optimal] else None


def normalize_payment_terms(payment_terms):
    """Normalize and clean payment terms input."""
    if not payment_terms or str(payment_terms).strip() == "":
        return None
    
    # Clean and normalize input text
    payment_terms = str(payment_terms).strip().lower()
    # Convert comma decimal format to dot format for percentages (e.g., "2,5%" -> "2.5%")
    payment_terms = str(re.sub(r'(\d+),(\d+)(\s*[%％])', r'\1.\2\3', payment_terms))
    
    return payment_terms


def get_keyword_lists():
    """Get predefined keyword lists for term identification."""
    discount_period_keys = ['days']
    credit_period_keys = ['days net', 'days due', 'net']
    separators = [',', 'and', 'or', '&', '.', '/', '\\', "otherwise"]
    
    return discount_period_keys, credit_period_keys, separators


def extract_discount_percentages(payment_terms):
    """Extract discount percentages and create percentage strings for matching."""
    discount_percentage_extract = re.findall(r'(\d+(?:\.\d+)?)\s*[%％]', payment_terms)
    
    if not discount_percentage_extract:
        return None, None, [], []
    
    # Process found percentages
    discount_percentage_1 = float(discount_percentage_extract[0])
    discount_percentage_2 = float(discount_percentage_extract[1]) if len(discount_percentage_extract) > 1 else None
    
    # Create variations of percentage strings for matching
    percentage_strings = [
        f"{str(discount_percentage_extract[0])}%",
        f"{str(discount_percentage_extract[0])} %",
        f"{str(discount_percentage_extract[0])}％",
        f"{str(discount_percentage_extract[0])} ％"
    ]
    
    percentage_strings_2 = []
    if len(discount_percentage_extract) > 1:
        percentage_strings_2 = [
            f"{str(discount_percentage_extract[1])}%",
            f"{str(discount_percentage_extract[1])} %",
            f"{str(discount_percentage_extract[1])}％",
            f"{str(discount_percentage_extract[1])} ％"
        ]
        percentage_strings = percentage_strings + percentage_strings_2
    
    return discount_percentage_1, discount_percentage_2, percentage_strings, percentage_strings_2


def get_numbers_excluding_percentages(payment_terms, discount_percentage_extract):
    """Get all numbers excluding those already identified as percentages."""
    return [num for num in re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms) 
            if num not in discount_percentage_extract]


def process_single_number_with_percentages(payment_terms, numbers, percentage_strings, discount_period_keys, credit_period_keys, separators):
    """Process case where there's one number and percentages exist."""
    discount_period_1 = None
    credit_period = None
    
    # Special handling for 'net' keyword
    discount_period_keys_special = ['days', 'net'] if 'net' in payment_terms else discount_period_keys
    
    # Process each separator to find discount periods
    for sep in separators:
        if sep in payment_terms:
            parts = payment_terms.split(sep)
            for part in parts:
                part = part.strip()
                
                # Check if this part contains a percentage string
                for discount_string in percentage_strings:
                    if discount_string in part:
                        # Look for discount period keywords in this part
                        key = [key for key in discount_period_keys if key in part]
                        if key:
                            discount_period_1 = find_closest_numbers(text=part, target_word=key[0])
                            return discount_period_1, credit_period
                    else:
                        # Check for credit period if no percentage in this part
                        flag = any(key in payment_terms for key in credit_period_keys)
                        if flag:
                            credit_period = numbers[0]
                            return discount_period_1, credit_period
    
    # If no separators worked, check for discount period keywords
    flag = any(key in payment_terms for key in discount_period_keys)
    if flag:
        discount_period_1 = numbers[0]
    
    return discount_period_1, credit_period


def process_multiple_numbers_with_percentages(payment_terms, percentage_strings, percentage_strings_2, 
                                            discount_percentage_extract, discount_period_keys, 
                                            credit_period_keys, separators):
    """Process case where there are multiple numbers and percentages exist."""
    discount_period_1 = None
    discount_period_2 = None
    credit_period = None
    discount_period_2_flag = False
    
    # Process each separator
    for sep in separators:
        if sep in payment_terms:
            parts = payment_terms.split(sep)
            for part in parts:
                part = part.strip()
                
                # Check if this part contains percentage strings
                if any(substring in part for substring in percentage_strings):
                    
                    # Handle multiple discount percentages
                    if len(discount_percentage_extract) > 1 and any(substring in part for substring in percentage_strings):
                        key = [key for key in discount_period_keys if key in part]
                        if key:
                            discount_period = find_closest_numbers(text=part, target_word=key[0])
                            
                            # Determine if this belongs to second discount percentage
                            for discount_string in percentage_strings:
                                if (discount_string in percentage_strings_2) and (discount_string in part):
                                    discount_period_2_flag = True
                                    discount_period_2 = discount_period
                            
                            if not(discount_period_2_flag):
                                discount_period_1 = discount_period
                    
                    # Handle single discount percentage
                    elif len(discount_percentage_extract) == 1 and any(key in part for key in discount_period_keys):
                        key = [key for key in discount_period_keys if key in part]
                        if key:
                            discount_period_1 = find_closest_numbers(text=part, target_word=key[0])
                
                # Process parts without percentage strings (likely credit terms)
                else:
                    key = [key for key in credit_period_keys if key in part]
                    if key:
                        credit_period = find_closest_numbers(text=part, target_word=key[0])
                
                # Early exit if all terms found
                if credit_period and discount_period_1 and discount_period_2:
                    break
    
    return discount_period_1, discount_period_2, credit_period


def process_no_percentages(payment_terms, credit_period_keys):
    """Process case where no discount percentages are found."""
    credit_period = None
    numbers = re.findall(r'\b\d+\.\d+|\b\d+\b', payment_terms)
    
    # Single number case
    if len(numbers) == 1:
        credit_period_keys.append("within")  # Add 'within' as credit period indicator
        flag = any(key in payment_terms for key in credit_period_keys)
        if flag:
            credit_period = numbers[0]
    
    # Multiple numbers case
    elif len(numbers) >= 2:
        key = [key for key in credit_period_keys if key in payment_terms]
        if key:
            credit_period = find_closest_numbers(text=payment_terms, target_word=key[0])
    
    return credit_period


def payment_terms_extraction(payment_term: str):
    """
    Extract payment terms information from payment terms text.
    
    This function parses payment terms text to extract:
    - Discount percentages (up to 2)
    - Discount periods (up to 2) 
    - Credit period
    
    Args:
        payment_terms (str): The payment terms text to parse
    
    Returns:
        tuple: (discount_percentage_1, discount_percentage_2, discount_period_1, 
                discount_period_2, credit_period)
                Each element can be None if not found
    """
    
    # Initialize return values
    credit_period = None
    discount_period_1 = None
    discount_period_2 = None
    discount_percentage_1 = None
    discount_percentage_2 = None
    
    # Normalize input
    payment_terms = normalize_payment_terms(payment_term)
    if payment_terms is None:
        return None, None, None, None, None
    
    # Get keyword lists
    discount_period_keys, credit_period_keys, separators = get_keyword_lists()
    
    # Early return if no digits found in text
    if not(bool(re.search(r'\d', payment_terms))):
        return discount_percentage_1, discount_percentage_2, discount_period_1, discount_period_2, credit_period
    
    # Extract discount percentages
    discount_percentage_1, discount_percentage_2, percentage_strings, percentage_strings_2 = extract_discount_percentages(payment_terms)
    
    if discount_percentage_1 is not None:  # Percentages found
        # Get discount percentage extract for further processing
        discount_percentage_extract = re.findall(r'(\d+(?:\.\d+)?)\s*[%％]', payment_terms)
        
        # Get numbers excluding percentages
        numbers = get_numbers_excluding_percentages(payment_terms, discount_percentage_extract)
        
        # Process based on number count
        if len(numbers) == 1:
            discount_period_1, credit_period = process_single_number_with_percentages(
                payment_terms, numbers, percentage_strings, discount_period_keys, 
                credit_period_keys, separators
            )
        elif len(numbers) >= 2:
            discount_period_1, discount_period_2, credit_period = process_multiple_numbers_with_percentages(
                payment_terms, percentage_strings, percentage_strings_2, 
                discount_percentage_extract, discount_period_keys, 
                credit_period_keys, separators
            )
    
    else:  # No percentages found
        credit_period = process_no_percentages(payment_terms, credit_period_keys.copy())
    
    # Final return with all extracted values
    return discount_percentage_1, discount_percentage_2, discount_period_1, discount_period_2, credit_period

def build_payment_terms_dict(data_dict):

    extract = payment_terms_extraction
    out = {}

    for vendor_id, raw_values in data_dict.items():
        cleaned = []

        for raw in raw_values:
            # call your extraction once
            _, _, _, _, credit_period = extract(raw)
            if credit_period is None:
                continue

            # normalize
            txt = str(credit_period).strip().lower()
            cleaned.append(txt)

        out[vendor_id] = cleaned

    return out

def get_next_pending_pipeline_run():
    """
    Fetch the oldest pending pipeline_run record to process.
    Returns in FIFO order (first created, first processed).
    
    Returns:
        dict with keys: run_id, batch_id, pipeline OR None if no pending runs
    """
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """SELECT run_id, batch_id, pipeline 
                          FROM pipeline_run 
                          WHERE status = 'PENDING' 
                          ORDER BY created_at ASC 
                          LIMIT 1;"""
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    return {
                        'run_id': result[0],
                        'batch_id': result[1],
                        'pipeline': result[2]
                    }
                return None
    except Exception as e:
        from code1.logger import logger
        logger.error(f"Error fetching next pending pipeline run: {str(e)}")
        return None


def check_running_pipeline_count():
    """
    Check if any pipeline is currently running.
    Prevents concurrent execution - only one pipeline at a time.
    
    Returns:
        int: Count of RUNNING pipelines (0 if none)
    """
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """SELECT COUNT(*) 
                          FROM pipeline_run 
                          WHERE status = 'RUNNING';"""
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    return result[0]
                return 0
    except Exception as e:
        from code1.logger import logger
        logger.error(f"Error checking running pipeline count: {str(e)}")
        return 0


def get_run_date_for_pipeline_run(run_id):
    """
    Fetch the run_date from pipeline_run table for a given run_id.
    This is the SCHEDULED time, not the actual execution time.
    
    Args:
        run_id: The pipeline_run id
        
    Returns:
        datetime object or None if not found
    """
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = "SELECT run_date FROM pipeline_run WHERE run_id = %s;"
                cursor.execute(query, (run_id,))
                result = cursor.fetchone()
                if result:
                    run_date_value = result[0]
                    
                    # Handle different return types from MySQL
                    if isinstance(run_date_value, str):
                        # If it's a string, convert to datetime
                        # Supports common formats: "2026-01-23 12:00:00" or "2026-01-23T12:00:00"
                        try:
                            if 'T' in run_date_value:
                                run_date_value = datetime.fromisoformat(run_date_value)
                            else:
                                run_date_value = datetime.strptime(run_date_value, "%Y-%m-%d %H:%M:%S")
                        except Exception as parse_error:
                            from code1.logger import logger
                            logger.error(f"Failed to parse run_date string '{run_date_value}': {str(parse_error)}")
                            return None
                    elif not isinstance(run_date_value, datetime):
                        # If it's some other type, log warning but try to return as-is
                        from code1.logger import logger
                        logger.warning(f"Unexpected type for run_date: {type(run_date_value)}")
                    
                    return run_date_value  # Now guaranteed to be datetime or None
        return None
    except Exception as e:
        from code1.logger import logger
        logger.error(f"Error fetching run_date for run_id {run_id}: {str(e)}")
        return None


def get_pipeline_run_details(run_id):
    """
    Fetch full details of a pipeline_run record for auditing and debugging.
    
    Args:
        run_id: The run_id to fetch
        
    Returns:
        dict with pipeline_run record details OR None if not found
    """
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = """SELECT run_id, pipeline, status, current_step, batch_id, run_date, 
                                  created_at, started_at, finished_at, status_message
                          FROM pipeline_run 
                          WHERE run_id = %s;"""
                cursor.execute(query, (run_id,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'run_id': result[0],
                        'pipeline': result[1],
                        'status': result[2],
                        'current_step': result[3],
                        'batch_id': result[4],
                        'run_date': result[5],
                        'created_at': result[6],
                        'started_at': result[7],
                        'finished_at': result[8],
                        'status_message': result[9]
                    }
                return None
    except Exception as e:
        from code1.logger import logger
        logger.error(f"Error fetching pipeline run details for run_id {run_id}: {str(e)}")
        return None


def enqueue_pipeline(pipeline, run_datetime):
    """
    Enqueue a pipeline for execution. Creates a new batch and pipeline_run record.
    
    Args:
        pipeline: Pipeline name ('ZBLOCK' or 'AP')
        run_datetime: ISO format datetime string for the run (e.g., '2026-01-22T14:00:00+00:00')
        
    Returns:
        dict with keys: success (bool), message (str), batch_id (int), run_id (int), status_code (int)
    """
    try:
        # Step 1: Validate pipeline
        if pipeline not in ['ZBLOCK', 'AP']:
            return {
                'success': False,
                'message': f"Invalid pipeline: {pipeline}. Must be 'ZBLOCK' or 'AP'.",
                'batch_id': None,
                'run_id': None,
                'status_code': 400
            }
        
        # Step 2: Parse and validate ISO datetime
        try:
            from dateutil import parser as dateutil_parser
            parsed_datetime = dateutil_parser.parse(run_datetime)
        except Exception as e:
            return {
                'success': False,
                'message': f"Invalid datetime format: {run_datetime}. Must be ISO format.",
                'batch_id': None,
                'run_id': None,
                'status_code': 400
            }
        
        # Step 3: Create new batch entry FIRST
        try:
            batch_id = create_new_batch_entry(pipeline)
            if not batch_id:
                return {
                    'success': False,
                    'message': f"Failed to create batch entry for pipeline {pipeline}.",
                    'batch_id': None,
                    'run_id': None,
                    'status_code': 500
                }
        except Exception as e:
            return {
                'success': False,
                'message': f"Error creating batch: {str(e)}",
                'batch_id': None,
                'run_id': None,
                'status_code': 500
            }
        
        # Step 4: Check for duplicate using (pipeline, run_datetime) combination
        try:
            with connect_to_database() as connection:
                with connection.cursor() as cursor:
                    # Use full datetime for run_date (DATETIME column)
                    # run_datetime is always rounded to hour (e.g., 2026-01-23T10:00:00Z, 2026-01-23T12:00:00Z)
                    # Each hour is unique, so check for exact datetime match (not just date)
                    run_date = parsed_datetime
                    check_query = "SELECT run_id FROM pipeline_run WHERE pipeline = %s AND run_date = %s;"
                    cursor.execute(check_query, (pipeline, run_date))
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        return {
                            'success': False,
                            'message': f"Pipeline {pipeline} already enqueued for {run_date.isoformat()}. Cannot enqueue same pipeline at same hour.",
                            'batch_id': None,
                            'run_id': existing_record[0],
                            'status_code': 409
                        }
        except Exception as e:
            return {
                'success': False,
                'message': f"Error checking for duplicate: {str(e)}",
                'batch_id': None,
                'run_id': None,
                'status_code': 500
            }
        
        # Step 5: Insert pipeline_run record with initial values
        try:
            with connect_to_database() as connection:
                with connection.cursor() as cursor:
                    insert_query = """INSERT INTO pipeline_run 
                        (batch_id, pipeline, run_date, status, started_at , current_step, status_message) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                    started_at = datetime.now(timezone.utc)
                    cursor.execute(insert_query, (
                        batch_id,
                        pipeline,
                        run_date,
                        'PENDING',
                        started_at,
                        None,
                        f"Pipeline enqueued for execution"
                    ))
                    connection.commit()
                    run_id = cursor.lastrowid
                    
                    return {
                        'success': True,
                        'message': f"Pipeline {pipeline} successfully enqueued with batch_id {batch_id}, run_id {run_id}.",
                        'batch_id': batch_id,
                        'run_id': run_id,
                        'status_code': 200
                    }
        except Exception as e:
            return {
                'success': False,
                'message': f"Error inserting pipeline_run record: {str(e)}",
                'batch_id': batch_id,
                'run_id': None,
                'status_code': 500
            }
            
    except Exception as e:
        return {
            'success': False,
            'message': f"Unexpected error: {str(e)}",
            'batch_id': None,
            'run_id': None,
            'status_code': 500
        }