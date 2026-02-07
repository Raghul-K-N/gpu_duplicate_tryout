'''
Python script to load the data to SRC table and update status in SRC_STATUS table
'''
import time
import random
from datetime import datetime, timezone
import json
import pandas as pd
import requests
from flask import g
from sqlalchemy import create_engine
import mysql.connector
# from code1.logger import logger
from code1.logger import capture_log_message, process_data_for_sending_internal_mail
import utils
import os
import traceback
from local_database import get_database_credentials

from pipeline_data import PipelineData

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


def read_table(tablename, columns=None, audit_id=None, client_id=None):
   """
   Function to read table from database
   """
   # chunk_list = []
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
   with connect_to_database() as connection:
       table = pd.read_sql(query,con=connection)
   return table


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
    data = pd.read_sql(f"SELECT FILE_NAME,MAPPING_ID FROM SRC_STATUS where id ={src_id};",con=connection)

    return data['FILE_NAME'][0],data['MAPPING_ID'][0]

def get_mapping_values(mapping_id):
    """
    Gets the defined and mapped column names from tronboarddatamapping table with mapping_id
    """
    connection = connect_to_database()
    data = pd.read_sql(f"SELECT TR_FIELDNAME,SOURCE_FIELDNAME,MAPPING_MODULE from tronboarddatamapping where MAPPINGROUP_ID={mapping_id};",con=connection)

    return data

def get_glcheck(mapping_id,client_id):
    """
    Get the gl check config details from triniconfigmapping
    """
    connection = connect_to_database()
    data = pd.read_sql(f"SELECT CONFIG_KEY,CONFIG_VALUE FROM triniconfigmapping where MAPPINGGROUPID in (1,{mapping_id}) and client_id = {client_id};",con=connection)

    return data

def get_apcheck(mapping_id,client_id):
    """
    Get the ap check config details from triniconfigmapping
    """
    connection = connect_to_database()
    data = pd.read_sql(f"SELECT CONFIG_KEY,CONFIG_VALUE FROM triniconfigmapping where MAPPINGGROUPID in (2,{mapping_id}) and client_id = {client_id};",con=connection)

    return data

def connect_to_database():
    """
    Connect to Database for Fetching Data
    """
    return mysql.connector.connect(user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST,
                                   port=DB_PORT, database=DB_NAME,**ssl_args)

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

def update_job_status(status, process_details,src_id,shape,rules):
    """
    Function to update job status in ML Execution table
    """
    process_details = json.dumps(process_details)
    rules = json.dumps(rules)

    # update_job_query = f"INSERT INTO SRC_STATUS(STATUS,MESSAGE,`MODULE`,NUM_RECORDS,RULES_ENABLED) VALUES({status},{process_details},'{module}',{shape},{rules});"
    update_job_query = f"UPDATE SRC_STATUS set STATUS = {status},MESSAGE = {process_details},NUM_RECORDS = {shape},RULES_ENABLED = {rules},RUN_JOB=1 where id = {src_id};"
    connect = connect_to_database()
    cur = connect.cursor()
    cur.execute(update_job_query)
    connect.commit()
    connect.close()
    capture_log_message(log_message="Update Finished for src_status id "+str(src_id)+" with status "+str(status)+" process message "+str(process_details))

def update_ingestion_status(src_id, process_details, curr_audit_id):
    """
    Function to update job status in ML Execution table
    """
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            process_details = json.dumps(process_details)
            update_job_query = f"UPDATE SRC_STATUS set MESSAGE = {process_details},STATUS = 2 WHERE id = {src_id} and audit_id = {curr_audit_id};"
            cur.execute(update_job_query)
            connect.commit()
            connect.close()
            capture_log_message(log_message="Update Finished for src_status id "+str(src_id)+" with process message "+str(process_details))

    # update_job_query = """
    # UPDATE SRC_STATUS
    # SET MESSAGE = %s, STATUS = 2
    # WHERE id = %s AND audit_id = %s;
    # """
    # try:
    #     connect = connect_to_database()
    #     cur = connect.cursor()
        
    #     # Set isolation level to SERIALIZABLE
    #     cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        
    #     # Start a transaction
    #     connect.start_transaction()
        
    #     # Execute the update query
    #     cur.execute(update_job_query, (process_details, src_id, curr_audit_id))
        
    #     # Commit the transaction
    #     connect.commit()
        
    #     capture_log_message(log_message=f"Update Finished for audit_id {curr_audit_id} src_status id "+str(src_id)+" with process message "+str(process_details))
        
    # except Error as e:
    #     # Rollback the transaction in case of error
    #     connect.rollback()
    #     capture_log_message(f"Error updating src_status id {src_id}: {e}")
    # finally:
    #     if connect.is_connected():
    #         cur.close()
    #         connect.close()

    

def update_scoring_status(src_id, process_details, curr_audit_id):
    """
    Function to update job status in ML Execution table
    """
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            process_details = json.dumps(process_details)
            update_job_query = f"UPDATE SRC_STATUS set MESSAGE = {process_details},STATUS = 3,RUN_JOB=2 WHERE id = {src_id}  and audit_id = {curr_audit_id};"
            cur.execute(update_job_query)
            connect.commit()
            connect.close()
            capture_log_message(log_message='Scoring status updated')
            

def get_src_gl_data(preprocessed_data,gl_rules,src_id):
    '''
    Function used to receive the preprocessed data
    '''
    ingest_data = preprocessed_data
    
    ingest_data[['POSTED_DATE','ENTERED_DATE']] = ingest_data[['POSTED_DATE','ENTERED_DATE']].apply(pd.to_datetime)

    mandatory_columns = ['ACCOUNTING_DOC','TRANSACTION_DESCRIPTION','TRANSACTION_ID_GA','DOC_TYPE','DOC_TYPE_DESCRIPTION','AMOUNT','ENTERED_BY','SAP_ACCOUNT','ACCOUNT_DESCRIPTION','POSTED_DATE','ENTERED_DATE','POSTED_BY','SAP_COMPANY','POSTED_LOCATION','ENTERED_LOCATION','DEBIT_CREDIT_INDICATOR']
    ingest_data=ingest_data.filter(items=mandatory_columns)
    capture_log_message(log_message="Source data Shape "+str(ingest_data.shape))
    capture_log_message(log_message="Reading data for duplicate check ")

    existing_data = read_table('_SRC_GL_DATA')
    capture_log_message(log_message="Duplicate check started")

    final_data = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(
        existing_data['TRANSACTION_ID_GA'])].copy()

    capture_log_message(log_message="Shape of Dataframe after Duplicate check "+str(final_data.shape))
    
    # module = "GL"
    shape = final_data.shape[0]
    if final_data.shape[0] != 0:
        upload_data_to_database(final_data, '_SRC_GL_DATA')
        process_details = "SRC_GL Table updated"
        update_job_status(1, process_details,src_id,shape,gl_rules)
    else: 
        process_details = "SRC_GL Table not updated duplicate Entries"
        update_job_status(0, process_details,src_id,shape,gl_rules)

def get_src_ap_data(preprocessed_data,ap_rules):
    '''
    Function used to receive the preprocessed data
    ''' 
    ingest_data = preprocessed_data

    ingest_data[['INVOICE_DATE','DUE_DATE','PAYMENT_DATE','POSTED_DATE','ENTERED_DATE']] = ingest_data[['INVOICE_DATE','DUE_DATE','PAYMENT_DATE','POSTED_DATE','ENTERED_DATE']].apply(pd.to_datetime)



    #AP columns
    # mandatory_columns = ['TRANSACTION_ID_GA','ACCOUNTING_DOC','TRANSACTION_DESCRIPTION','DOC_TYPE','AMOUNT','ENTERED_BY','POSTED_BY','GL_ACCOUNT_TYPE','ACCOUNT_DESCRIPTION','COMPANY_NAME','ENTERED_DATE','INVOICE_DATE','INVOICE_AMOUNT','INVOICE_NUMBER','DUE_DATE','PAYMENT_DATE','SUPPLIER_ID','SUPPLIER_NAME','PAYMENT_TERMS','DISCOUNT_PERCENTAGE','DISCOUNT_TAKEN','PAYMENT_AMOUNT','POSTED_LOCATION','ENTERED_LOCATION','POSTED_DATE','DISCOUNT_PERIOD','CREDIT_PERIOD','DEBIT_AMOUNT','CREDIT_AMOUNT','DEBIT_CREDIT_INDICATOR','INVOICE_STATUS','CLEARING_DOC']
    mandatory_columns = ['TRANSACTION_ID_GA','ACCOUNTING_DOC','TRANSACTION_DESCRIPTION','DOC_TYPE','AMOUNT','ENTERED_BY','POSTED_BY','ACCOUNT_DESCRIPTION','COMPANY_NAME','ENTERED_DATE','INVOICE_DATE','INVOICE_AMOUNT','INVOICE_NUMBER','DUE_DATE','PAYMENT_DATE','SUPPLIER_ID','SUPPLIER_NAME','DISCOUNT_PERCENTAGE','DISCOUNT_TAKEN','PAYMENT_AMOUNT','POSTED_LOCATION','ENTERED_LOCATION','POSTED_DATE','CREDIT_PERIOD','DEBIT_AMOUNT','CREDIT_AMOUNT','DEBIT_CREDIT_INDICATOR','INVOICE_STATUS','CLEARING_DOC']
    ingest_data=ingest_data[mandatory_columns]
    capture_log_message(log_message="Source data Shape "+str(ingest_data.shape))
    capture_log_message(log_message="Reading data for duplicate check ")

    existing_data = read_table('_SRC_AP_DATA')
    capture_log_message(log_message="Duplicate check started")

    final_data = ingest_data.loc[~ingest_data['TRANSACTION_ID_GA'].isin(
        existing_data['TRANSACTION_ID_GA'])].copy()
    capture_log_message(log_message="Shape of Dataframe after Duplicate check "+str(final_data.shape))

    module = "AP"
    shape = final_data.shape[0]
    if final_data.shape[0] != 0:

        upload_data_to_database(final_data, '_SRC_AP_DATA')
        capture_log_message(log_message="Data uploaded into SRC_AP table")
        
        process_details = "SRC_AP Table updated"
        update_job_status(1, process_details,module,shape,ap_rules)
    else: 
        process_details = "SRC_AP Table not updated duplicate Entries"
        update_job_status(0, process_details,module,shape,ap_rules)
        capture_log_message(log_message="Duplicate Entries")

def update_flat_table(audit_id, retry_count=3,months_lst=[]):
    """
    Function to update flat table
    """
    start_time = datetime.now(timezone.utc)
    attempt = 0
    while attempt < retry_count:
        try:
            capture_log_message(log_message=f'AUD_{audit_id} GL FLAT_attempt_{attempt}')
            for month in months_lst:
                update_tran_data = process_gl_rpt_tran_flat_data(month)
                update_doc_data = process_gl_acc_doc_flat_data(month)
                
                upload_data_to_database(update_tran_data,f'rpttransactionflat_{str(month)}')
                upload_data_to_database(update_doc_data,f'rptaccountdocumentflat_{str(month)}')
  
            end_time = datetime.now(timezone.utc)
            capture_log_message(current_logger=g.stage_logger,log_message="Updated Flat Tables related details",
                                start_time=start_time,end_time=end_time)
            return True
        except Exception as e:
            if 'deadlock' in str(e).lower():
                attempt += 1
                time.sleep(random.uniform(0.5, 2.0))  # Sleep for a random short interval before retrying
                capture_log_message(log_message=f'AUD_{audit_id}FLAT_Attempt {attempt} failed: {e}. Retrying in 2 seconds...')
                continue  # Retry the transaction
            else:
                end_time = datetime.now(timezone.utc)
                err_msg = str(e)+ str(traceback.format_exc())
                capture_log_message(current_logger=g.error_logger,log_message=f"Error in updating flat table: {err_msg}",
                                    start_time=start_time,end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
                # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE,is_success=False,
                #                                     subject='Scoring Status',
                #                                     description_list=['Scoring Completed but Error in updating data'],
                #                                     time_taken_list=[end_time],
                #                                     )
                return False
            
    end_time = datetime.now(timezone.utc)
    capture_log_message(current_logger=g.error_logger, log_message="Failed to update flat table after multiple attempts",
                        start_time=start_time, end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
    # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE, is_success=False,
    #                                         subject='Scoring Status',
    #                                         description_list=['Failed to update flat table due to deadlock after multiple attempts'],
    #                                         time_taken_list=[end_time])
    return False


def update_apflat_table(audit_id, retry_count=3, months_lst=[]):
    """
    Function to update flat table
    df (pd.DataFrame): DataFrame containing data to update the table.
    """
    start_time = datetime.now(timezone.utc)
    attempt = 0
    while attempt < retry_count:
        try:
            capture_log_message(log_message=f'AUD_{audit_id} FLAT_attempt_{attempt}')
            for month in months_lst:
                update_doc_data = process_ap_doc_flat_data(month)
                update_tran_data = process_ap_tran_flat_data(month)

                upload_data_to_database(update_doc_data,f'apaccountdocumentsflat_{str(month)}')
                upload_data_to_database(update_tran_data,f'aptransactionflat_{str(month)}')
                
            end_time = datetime.now(timezone.utc)
            capture_log_message(current_logger=g.stage_logger, log_message="Updated Flat Tables related details",
                                start_time=start_time, end_time=end_time)
            return True
        
        except Exception as e:
            if 'deadlock' in str(e).lower():
                attempt += 1
                time.sleep(random.uniform(0.5, 2.0))  # Sleep for a random short interval before retrying
                capture_log_message(log_message=f'AUD_{audit_id}FLAT_Attempt {attempt} failed: {e}. Retrying in 2 seconds...')
                continue  # Retry the transaction
            else:
                end_time = datetime.now(timezone.utc)
                err_msg = str(e)+ str(traceback.format_exc())
                capture_log_message(current_logger=g.error_logger, log_message=f"Error in updating flat table: {err_msg} ",
                                    start_time=start_time, end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
                # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE, is_success=False,
                #                                         subject='Scoring Status',
                #                                         description_list=['Scoring Completed but Error in updating data'],
                #                                         time_taken_list=[end_time])
                return False

    end_time = datetime.now(timezone.utc)
    capture_log_message(current_logger=g.error_logger, log_message="Failed to update flat table after multiple attempts",
                        start_time=start_time, end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
    # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE, is_success=False,
    #                                         subject='Scoring Status',
    #                                         description_list=['Failed to update flat table due to deadlock after multiple attempts'],
    #                                         time_taken_list=[end_time])
    return False

def update_zblock_flat_table(retry_count=3, quarters_lst=[]):
    """
    Function to update flat table
    df (pd.DataFrame): DataFrame containing data to update the table.
    """
    start_time = datetime.now(timezone.utc)
    attempt = 0
    while attempt < retry_count:
        try:
            capture_log_message(log_message=f'Batch: {g.batch_id} FLAT_attempt_{attempt}')
            for quarter in quarters_lst:
                update_zblock_doc_data = process_zblock_doc_flat_data(quarter)
                update_zblock_tran_data = process_zblock_tran_flat_data(quarter)

                upload_data_to_database(update_zblock_doc_data,f'z_block_account_document_flat_{str(quarter)}')
                upload_data_to_database(update_zblock_tran_data,f'z_block_transaction_flat_{str(quarter)}')
                
            end_time = datetime.now(timezone.utc)
            capture_log_message(current_logger=g.stage_logger, log_message="Updated Flat Tables related details",
                                start_time=start_time, end_time=end_time)
            return True
        
        except Exception as e:
            if 'deadlock' in str(e).lower():
                attempt += 1
                time.sleep(random.uniform(0.5, 2.0))  # Sleep for a random short interval before retrying
                capture_log_message(log_message=f'Batch:{g.batch_id} FLAT_Attempt {attempt} failed: {e}. Retrying in 2 seconds...')
                continue  # Retry the transaction
            else:
                end_time = datetime.now(timezone.utc)
                err_msg = str(e)+ str(traceback.format_exc())
                capture_log_message(current_logger=g.error_logger, log_message=f"Error in updating flat table: {err_msg} ",
                                    start_time=start_time, end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
                # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE, is_success=False,
                #                                         subject='Scoring Status',
                #                                         description_list=['Scoring Completed but Error in updating data'],
                #                                         time_taken_list=[end_time])
                return False

    end_time = datetime.now(timezone.utc)
    capture_log_message(current_logger=g.error_logger, log_message="Failed to update flat table after multiple attempts",
                        start_time=start_time, end_time=end_time,error_name=utils.ISSUES_WITH_STORING_RESULTS)
    # process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE, is_success=False,
    #                                         subject='Scoring Status',
    #                                         description_list=['Failed to update flat table due to deadlock after multiple attempts'],
    #                                         time_taken_list=[end_time])
    return False

# def update_apflat_table(audit_id):
#     """
#     Function to update flat table
#     """
#     start_time = datetime.now(timezone.utc)
#     try:
#         truncate_flat_table_doc ="truncate apaccountdocumentsflat;"
#         truncate_flat_table_trans ="truncate aptransactionflat;"
#         update_doc_query = f"insert into apaccountdocumentsflat select * from apaccountdocumentsflatview;;"
#         update_tran_query = f"insert into aptransactionflat select * from aptransactionflatview;"
#         update_approval_matrix_id =f"update ap_accountdocuments apd left join approval_matrix ap on apd.AMOUNT >= ap.MIN_AMOUNT and apd.AMOUNT < ap.MAX_AMOUNT set apd.APPROVAL_MATRIX_ID = ap.APPROVAL_ID where audit_id = {audit_id};"
#         connect = connect_to_database()
#         cur = connect.cursor()
#         cur.execute(update_approval_matrix_id)
#         cur.execute(truncate_flat_table_doc)
#         cur.execute(update_doc_query)
#         cur.execute(truncate_flat_table_trans)
#         cur.execute(update_tran_query)
#         connect.commit()
#         connect.close()
#         end_time = datetime.now(timezone.utc)
        
#         capture_log_message(current_logger=g.stage_logger,log_message="Updated Flat Tables related details",
#                             start_time=start_time,end_time=end_time) 
#         return True
#     except Exception as e:
#         end_time = datetime.now(timezone.utc)
#         capture_log_message(current_logger=g.error_logger,log_message="Error in updating flat table "+str(e),
#                             start_time=start_time,end_time=end_time)
#         process_data_for_sending_internal_mail(stage=utils.DATA_SCORING_STAGE,is_success=False,
#                                                subject='Scoring Status',
#                                                description_list=['Scoring Completed but Error in updating data'],
#                                                time_taken_list=[end_time],
#                                                )
#         return False

def update_current_job(src_id,value, curr_audit_id):
    '''
    Function to update current_job column in SRC_STATUS
    '''
    with connect_to_database() as connect:
        with connect.cursor() as cur:
            update_job_query = f"UPDATE SRC_STATUS set CURRENT_RUN = {value}  WHERE id = {src_id} and audit_id = {curr_audit_id};"
            cur.execute(update_job_query)
            connect.commit()
            connect.close()

def mail_notification(src_id):
    '''
    Function to send mail after the scoring is done
    '''
    url = "https://clouddev.thinkrisk.ai:8080/public/index.php/v1/mlvm_notify"
    uniqueId = src_id

    payload = {'uniqueId': uniqueId}
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload,verify=False)
    capture_log_message(log_message=response.text)


def process_gl_acc_doc_flat_data(qtr_year):
    """
    handling the join and select operations in Python for rptaccountdocumentsflat
    """

    #load your pipeline db from instance
    obj = PipelineData()
    df = obj.get_data(f'audit_{g.audit_id}')

    # Load your data into DataFrames
    rpttransactions = df.copy()
    capture_log_message("rpt transactions shape used for rptaccdoc flat: "+str(rpttransactions.shape[0]))

    rpttransactions = rpttransactions.query(f"date_label == '{str(qtr_year)}'")
    capture_log_message("rpt transactions shape after filtering by date_label used for rptaccdoc flat: "+str(rpttransactions.shape[0]))
    
    rpttransactions = rpttransactions.rename(columns={'COMPANY_CODE': 'COMPANY_CODE_NAME',
                                                      'COMPANYID':'COMPANY_CODE',
                                                      'LOCATION_CODE': 'POSTED_LOCATION_NAME',
                                                      'POSTED_BY':'POSTED_BY_NAME_NEW',
                                                      'POSTED_BY_USERID':'POSTED_BY',
                                                      'DOCTYPEID':'DOCUMENTID'})

    rpt_accountdocuments = rpttransactions.groupby('ACCOUNTDOC_CODE').agg({'ACCOUNTDOCID':'first','DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum',
        'POSTED_BY_NAME':'last','POSTED_BY':'last','COMPANY_CODE_NAME':'last','COMPANY_CODE':'last',
        'POSTED_LOCATION_NAME':'first', 'POSTED_LOCATION':'first','DOCUMENTID':'first',
        'POSTED_DATE':'last'}).reset_index()


    rpt_accountdocuments['REVIEWSTATUSID'] = 1
    # rpt_accountdocuments = rpt_accountdocuments.rename(columns={'LOCATION_ID': 'LOCATIONID'})

    cols_to_remove = ['AI_RISK_SCORE','AI_RISK_SCORE_RAW','STAT_SCORE','STAT_SCORE_INDEXED',
                      'UNUSUAL_MONETARY_FLOW','SUSPENSE_ACCOUNT_WITH_CASH','SUSPENSE_ACCOUNT_WITH_INVENTORY']
    rpt_accountdocscore = read_table(tablename=f'rptaccountdocscore_{g.audit_id}',audit_id=g.audit_id)
    capture_log_message("No of rows in rpt_accountdocscore: "+str(rpt_accountdocscore.shape[0]))
    # rpt_accountdocuments = rpt_accountdocuments.drop(columns = 'ACCOUNTDOC_CODE')
    rpt_accountdocscore = rpt_accountdocscore.drop(columns = cols_to_remove)
    rpt_accountdocscore['ACCOUNTDOCID'] = pd.to_numeric(rpt_accountdocscore["ACCOUNTDOCID"]).astype(int)

    final_data = (rpt_accountdocuments.merge(rpt_accountdocscore, on="ACCOUNTDOCID",  how='inner'))

    capture_log_message(f'columns to upload in rpt_acc_document flat: {final_data.columns}')
    return final_data


def process_gl_rpt_tran_flat_data(qtr_year):
    """
    handling the join and select operations in Python instead of apacountdocumentsflatview
    """
    
    obj = PipelineData()
    df = obj.get_data(f'audit_{g.audit_id}')

    # Load your data into DataFrames
    rpt_transaction =df.copy()  # Replace with your data loading logic
    capture_log_message("No of rows in rpt_transaction: "+str(rpt_transaction.shape[0]))

    rpt_transaction = rpt_transaction.query(f"date_label == '{str(qtr_year)}'")
    capture_log_message("Shape of rpt_transaction after filtering by date_label: "+str(rpt_transaction.shape[0]))

    rpttransaction_cols_list = [ "TRANSACTIONID","ACCOUNTID", "ACCOUNT_CODE", 
                                "ENTERED_DATE", "LOCATION_CODE", "ENTERED_BY_NAME", "COMPANY_CODE",
                                "TRANSACTION_DESC", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "DESCRIPTION",
                                "HEADER_TEXT", "LINE_ITEM_TEXT" ]

    rpt_transaction = rpt_transaction[rpttransaction_cols_list]
    

    rpt_transactionscore = read_table(tablename=f"rpttransactionscore_{g.audit_id}")
    rpt_transactionscore['TRANSACTIONID'] = pd.to_numeric(rpt_transactionscore["TRANSACTIONID"]).astype(int)
    capture_log_message("No of rows in rpt_transactionscore: "+str(rpt_transactionscore.shape[0]))
    
    cols_to_remove = ['TRANSSCOREID','DEVIATION','BLENDED_RISK_SCORE_RAW','AI_RISK_SCORE_RAW','STAT_SCORE',
                        'RULES_RISK_SCORE_RAW','OPTIMISED_RULES_RISK_SCORE_RAW','OPTIMISED_BLENDED_RISK_SCORE_RAW',
                        'OPTIMISED_DEVIATION','STATUS','UNUSUAL_ACCOUNTING_PATTERN']
    rpt_transactionscore = rpt_transactionscore.drop(columns = cols_to_remove)
    final_df = rpt_transaction.merge(rpt_transactionscore, on='TRANSACTIONID')

    capture_log_message(f'columns to upload in rpt_transaction flat: {final_df.columns}')
    return final_df


def process_ap_doc_flat_data(month):
    """
    handling the join and select operations in Python for apaccountdocumentsflatview
    """

    #load your pipeline db from instance
    obj = PipelineData()
    df = obj.get_data(f'audit_{g.audit_id}')

    # Load your data into DataFrames
    transactions = df.copy()
    capture_log_message("No of rows in transactions: "+str(transactions.shape[0]))
    transactions = transactions.query(f"MONTH_LABEL == '{str(month)}'")
    capture_log_message("No of rows in transactions after filtering by date_label: "+str(transactions.shape[0]))
    ap_accountdocuments = transactions.groupby('ACCOUNTING_DOC').agg({'ACCOUNT_DOC_ID':'first','DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum',
        'POSTED_BY_NAME':'last','POSTED_BY':'last','PAYMENT_TERMS':'last','AMOUNT':'first','INVOICE_AMOUNT':'last','COMPANYID':'last','COMPANY_CODE_NAME':'last','LOCATION_ID':'first','POSTED_LOCATION_NAME':'first'
        ,'ENTRY_ID':'first','POSTED_DATE':'last','PAYMENT_DATE':'last','DUE_DATE':'last',
        'INVOICE_DATE':'last','INVOICE_NUMBER':'last','VENDORID':'last','VENDORCODE':'last','APPROVED_USER_5':'last','APPROVED_USER_4':'last',
        'APPROVED_USER_3':'last','APPROVED_USER_2':'last','APPROVED_USER_1':'last',
        "INVOICE_CURRENCY":'first',"PAYMENT_METHOD":'first','VAT_ID':'first',"LEGAL_ENTITY_NAME_AND_ADDRESS":'first',"DOC_TYPE": 'first',
        "DISCOUNT_TAKEN":'first', "DISCOUNT_PERCENTAGE_1":'first',"DISCOUNT_PERCENTAGE_2":'first',"DISCOUNT_PERIOD_1":'first',"DISCOUNT_PERIOD_2":'first', "CREDIT_PERIOD":'first',
        "PURCHASE_ORDER_NUMBER":'first',"GRN_NUMBER":'first',"GRN_DATE":'first',"PURCHASE_ORDER_DATE":'first','REGION':'first'}).reset_index()
    
    # Replace with your data loading logic
    ap_accountdocuments['REVIEWSTATUSID'] = 1
    ap_accountdocuments = ap_accountdocuments.rename(columns={'LOCATION_ID': 'LOCATIONID'})

    cols_to_remove = ['HIGH_VALUE_DEBIT_CREDIT_NOTES','DIFFERENCE_IN_INVOICE_PRICE','DIFFERENCE_IN_INVOICE_QUANTITY','ADVANCE_PAYMENT_AGAINST_INVOICES','CASH_REIMBURSEMENTS','CASH_EXPENSES']
    ap_accountdocscore = read_table(tablename=f'ap_accountdocscore_{g.audit_id}',audit_id=g.audit_id)
    ap_accountdocuments = ap_accountdocuments.drop(columns = 'ACCOUNTING_DOC')
    ap_accountdocscore = ap_accountdocscore.drop(columns = cols_to_remove)
    ap_accountdocscore['ACCOUNT_DOC_ID'] = pd.to_numeric(ap_accountdocscore["ACCOUNT_DOC_ID"]).astype(int)

    final_data = (ap_accountdocuments.merge(ap_accountdocscore, on="ACCOUNT_DOC_ID",  how='inner'))


    # Save or use `final_data`
    capture_log_message(f'columns to upload in acc document flat: {final_data.columns}')
    return final_data


def process_ap_tran_flat_data(month):
    """
    handling the join and select operations in Python instead of apacountdocumentsflatview
    """
    
    obj = PipelineData()
    df = obj.get_data(f'audit_{g.audit_id}')

    # Load your data into DataFrames
    ap_transaction =df.copy()  # Replace with your data loading logic
    capture_log_message("No of rows in ap_transaction: "+str(ap_transaction.shape[0]))
    ap_transaction = ap_transaction.query(f"MONTH_LABEL == '{str(month)}'")
    capture_log_message("No of rows in ap_transaction after filtering by date_label: "+str(ap_transaction.shape[0]))
    transaction_cols_list = [
        "TRANSACTION_ID", "PAYMENT_DATE", "ACCOUNT_CODE", 
        "ENTRY_DATE", "TRANSACTION_CODE", "POSTING_DATE", "TRANSACTION_DESCRIPTION", 
        "LOCATION_CODE", "ENTERED_BY_NAME", "DOC_TYPE",
        "ENTERED_BY", "ACCOUNT_NAME", "PAYMENT_TERMS", "INVOICE_AMOUNT","COMPANYID", "ACCOUNT_ID",
        "INVOICE_ID", "GL_ACCOUNT_DESCRIPTION", "POSTED_BY", "DEBIT_AMOUNT", "CREDIT_AMOUNT",
        "DISCOUNT_TAKEN", "DISCOUNT_PERCENTAGE_1", "DISCOUNT_PERCENTAGE_2","DISCOUNT_PERIOD_1","DISCOUNT_PERIOD_2", "CREDIT_PERIOD",
        "PURCHASE_ORDER_NUMBER","GRN_NUMBER","GRN_DATE","PURCHASE_ORDER_DATE",'REGION'
    ]
    ap_transaction = ap_transaction[transaction_cols_list]
    ap_transaction['DESCRIPTION'] = ap_transaction['GL_ACCOUNT_DESCRIPTION']

    ap_transactionscore = read_table(tablename=f"ap_transactionscore_{g.audit_id}")
    ap_transactionscore['TRANSACTION_ID'] = pd.to_numeric(ap_transactionscore["TRANSACTION_ID"]).astype(int)
    capture_log_message("No of rows in ap_transactionscore: "+str(ap_transactionscore.shape[0]))
    
    cols_to_remove = ['TRANSACTION_SCORE_ID','DEVIATION','BLENDED_RISK_SCORE_RAW','AI_RISK_SCORE_RAW','STAT_SCORE','RULES_RISK_SCORE',
                        'RULES_RISK_SCORE_RAW','STATUS','DIFFERENCE_IN_INVOICE_QUANTITY','DIFFERENCE_IN_INVOICE_PRICE','HIGH_VALUE_DEBIT_CREDIT_NOTES',
                        'ADVANCE_PAYMENT_AGAINST_INVOICES','CASH_REIMBURSEMENTS','CASH_EXPENSES']
    ap_transactionscore = ap_transactionscore.drop(columns = cols_to_remove)
    final_df = ap_transaction.merge(ap_transactionscore, on='TRANSACTION_ID')
    # capture_log_message(f"final_df cols: {final_df.columns.to_list()}")

    final_df = final_df.rename(
    columns={
        'ACCOUNT_CODE':'ACCOUNT_NUMBER',
        'LOCATION_CODE': 'ENTRY_LOCATION',
        'TRANSACTION_DESCRIPTION': 'TRANSACTION_DESC'
    } ) 
    # Save or display the result
    capture_log_message(f'columns to upload in aptransaction flat: {final_df.columns}')
    return final_df


def get_max_duplicate_id(available_months):
    """
    Reads all available duplicate_invoice tables for the given months and 
    calculates the maximum duplicate_id.

    :param available_months: List of month-year strings (e.g., ["m1_2024", "m2_2024"])
    :return: Maximum duplicate_id across all provided tables, or None if no valid data is found.
    """
    max_duplicate_id = None
    available_months = [str(month) for month in available_months if str(month).startswith('m')]
    capture_log_message(f"Get Max Duplicate ID for Available months: {available_months}")
    with connect_to_database() as connection:
        for month in available_months:
            table_name = f"duplicate_invoices_{month}"
            query = f"SELECT MAX(DUPLICATES_ID) AS max_id FROM {table_name}"

            try:
                df = pd.read_sql(query, connection)
                max_id = df.iloc[0]["max_id"]

                if max_id is not None:
                    max_duplicate_id = max(max_duplicate_id, max_id) if max_duplicate_id is not None else max_id

            except Exception as e:
                print(f"Error reading {table_name}: {e}")

    return max_duplicate_id if max_duplicate_id is not None else 0


def get_max_pair_id(available_months):
    """
    Reads all available credit_debit_pairs tables for the given months and 
    calculates the maximum pair_id.

    :param available_months: List of month-year strings (e.g., ["m1_2024", "m2_2024"])
    :return: Maximum pair_id across all provided tables, or 0 if no valid data is found.
    """
    max_pair_id = None
    available_months = [str(month) for month in available_months if str(month).startswith('m')]
    capture_log_message(f"Get Max Pair ID for Available months: {available_months}")
    with connect_to_database() as connection:
        for month in available_months:
            table_name = f"credit_debit_pairs_{month}"
            query = f"SELECT MAX(PAIR_ID) AS max_id FROM {table_name}"

            try:
                df = pd.read_sql(query, connection)
                max_id = df.iloc[0]["max_id"]

                if max_id is not None:
                    max_pair_id = max(max_pair_id, max_id) if max_pair_id is not None else max_id

            except Exception as e:
                print(f"Error reading {table_name}: {e}")

    return max_pair_id if max_pair_id is not None else 0


def get_max_series_id(available_months):
    """
    Reads all available series_duplicate_invoices tables for the given months and 
    calculates the maximum duplicates_id.

    :param available_months: List of month-year strings (e.g., ["m1_2024", "m2_2024"])
    :return: Maximum duplicates_id across all provided tables, or 0 if no valid data is found.
    """
    max_series_id = None
    available_months = [str(month) for month in available_months if str(month).startswith('m')]
    capture_log_message(f"Get Max Series ID for Available months: {available_months}")
    with connect_to_database() as connection:
        for month in available_months:
            table_name = f"series_duplicate_invoices_{month}"
            query = f"SELECT MAX(DUPLICATES_ID) AS max_id FROM {table_name}"

            try:
                df = pd.read_sql(query, connection)
                max_id = df.iloc[0]["max_id"]

                if max_id is not None:
                    max_series_id = max(max_series_id, max_id) if max_series_id is not None else max_id

            except Exception as e:
                print(f"Error reading {table_name}: {e}")

    return max_series_id if max_series_id is not None else 0


def process_zblock_doc_flat_data(quarter):
    """
    handling the join and select operations in Python for z_block_account_document_flat
    """

    obj = PipelineData()
    df = obj.get_data(f'zblock_audit_{g.audit_id}')
    # Load your data into DataFrames
    transactions = df.copy()
    capture_log_message("No of rows in transactions: "+str(transactions.shape[0]))

    transactions = transactions.query(f"date_label == '{str(quarter)}'")
    capture_log_message("No of rows in transactions after filtering by date_label: "+str(transactions.shape[0]))

    zblock_accountdocuments = transactions.groupby('ACCOUNTING_DOC').agg({'ACCOUNT_DOC_ID':'first','DEBIT_AMOUNT':'sum','CREDIT_AMOUNT':'sum',
        'POSTED_BY_NAME':'last','POSTED_BY':'last','PAYMENT_TERMS':'last','AMOUNT':'first','INVOICE_AMOUNT':'last','COMPANYID':'last','COMPANY_CODE_NAME':'last','LOCATION_ID':'first','POSTED_LOCATION_NAME':'first'
        ,'ENTRY_ID':'first','POSTED_DATE':'last','PAYMENT_DATE':'last','DUE_DATE':'last',
        'INVOICE_DATE':'last','INVOICE_NUMBER':'last','VENDORID':'last','VENDORCODE':'last','APPROVED_USER_5':'last','APPROVED_USER_4':'last',
        'APPROVED_USER_3':'last','APPROVED_USER_2':'last','APPROVED_USER_1':'last',
        "INVOICE_CURRENCY":'first',"PAYMENT_METHOD":'first','VAT_ID':'first',"LEGAL_ENTITY_NAME_AND_ADDRESS":'first',"DOC_TYPE": 'first',
        "DISCOUNT_TAKEN":'first', "DISCOUNT_PERCENTAGE_1":'first',"DISCOUNT_PERCENTAGE_2":'first',"DISCOUNT_PERIOD_1":'first',"DISCOUNT_PERIOD_2":'first', "CREDIT_PERIOD":'first',
        "PURCHASE_ORDER_NUMBER":'first',"GRN_NUMBER":'first',"GRN_DATE":'first',"PURCHASE_ORDER_DATE":'first'}).reset_index()
    
    zblock_accountdocuments['REVIEWSTATUSID'] = 1
    zblock_accountdocuments = zblock_accountdocuments.rename(columns={'LOCATION_ID': 'LOCATIONID'})
    zblock_accountdocuments = zblock_accountdocuments.drop(columns = 'ACCOUNTING_DOC')

    capture_log_message(f'columns to upload in acc document flat: {zblock_accountdocuments.columns}')
    return zblock_accountdocuments


def process_zblock_tran_flat_data(quarter):
    """
    handling the join and select operations in Python instead of z_block_transaction_flat
    """
    
    obj = PipelineData()
    df = obj.get_data(f'zblock_audit_{g.audit_id}') 

    zblock_transaction =df.copy()
    capture_log_message("No of rows in zblock_transaction: "+str(zblock_transaction.shape[0]))

    zblock_transaction = zblock_transaction.query(f"date_label == '{str(quarter)}'")
    capture_log_message("No of rows in zblock_transaction after filtering by date_label: "+str(zblock_transaction.shape[0]))

    transaction_cols_list = [
        "TRANSACTION_ID", "PAYMENT_DATE", "ACCOUNT_CODE", 
        "ENTRY_DATE", "TRANSACTION_CODE", "POSTING_DATE", "TRANSACTION_DESCRIPTION", 
        "LOCATION_CODE", "ENTERED_BY_NAME", "DOC_TYPE",
        "ENTERED_BY", "ACCOUNT_NAME", "PAYMENT_TERMS", "INVOICE_AMOUNT","COMPANYID", "ACCOUNT_ID",
        "INVOICE_ID", "GL_ACCOUNT_DESCRIPTION", "POSTED_BY", "DEBIT_AMOUNT", "CREDIT_AMOUNT",
        "DISCOUNT_TAKEN", "DISCOUNT_PERCENTAGE_1", "DISCOUNT_PERCENTAGE_2","DISCOUNT_PERIOD_1","DISCOUNT_PERIOD_2", "CREDIT_PERIOD",
        "PURCHASE_ORDER_NUMBER","GRN_NUMBER","GRN_DATE","PURCHASE_ORDER_DATE"
    ]
    zblock_transaction = zblock_transaction[transaction_cols_list]
    zblock_transaction['DESCRIPTION'] = zblock_transaction['GL_ACCOUNT_DESCRIPTION']

    zblock_transaction = zblock_transaction.rename(
    columns={
        'ACCOUNT_CODE':'ACCOUNT_NUMBER',
        'LOCATION_CODE': 'ENTRY_LOCATION',
        'TRANSACTION_DESCRIPTION': 'TRANSACTION_DESC'
    } ) 

    capture_log_message(f'columns to upload in zblocktransaction flat: {zblock_transaction.columns}')
    return zblock_transaction