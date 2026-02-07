import mysql.connector
import pandas as pd
from Ingestor.GL_Ingestor import GL_Ingestor
from Ingestor.AP_Ingestor import AP_Ingestor
from src_load import update_ingestion_status,update_scoring_status,update_flat_table,update_apflat_table,update_current_job
# from GL_Module.main import gl_main
from AP_Module.main import ap_main
from flask import g
# from code1.logger import logger
from code1.logger import capture_log_message, mark_pipeline_as_complete
import utils
from datetime import datetime, timezone
import os
from databases.sharding_tables import ShardingTables
from Ingestor.fetch_data import get_quarters

def connect_to_database():
    """
    Getting the Database configurations & Connecting to Database
    """
    from local_database import get_database_credentials
    credentials = get_database_credentials()
    DB_USERNAME = credentials["username"]
    DB_PASSWORD = credentials["password"]
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT= os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    SSL_CA_FILE = os.getenv("SSL_CA")
    USE_SSL_CA = os.getenv("USE_SSL_CA", "false").lower() == "true"
    ssl_args = {'ssl_ca': SSL_CA_FILE } if USE_SSL_CA else {}
    return mysql.connector.connect(user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST,
                                   port=DB_PORT, database=DB_NAME,**ssl_args)

def init_main(src_id, audit_id, client_id):

    '''
    Function which read the status from src status table
    '''

    #exec_table = pd.read_sql("""select id,STATUS,MESSAGE from SRC_STATUS order by id desc limit 1;""",con=connect_to_database())
    with connect_to_database() as connection:
        curr_src = pd.read_sql(f"""select id, audit_id from SRC_STATUS where CURRENT_RUN = 1 and audit_id = {audit_id};""",con=connection) # type: ignore
    capture_log_message(log_message='Curr_src value is :{}'.format(curr_src),store_in_db=False)
    curr_client_id = client_id
    
    if curr_src.shape[0] == 0:
        capture_log_message("No Process with CURRENT_RUN value 1")
    else:
        curr_src_id = str(curr_src['id'][0])
        curr_audit_id = int(curr_src['audit_id'][0])
        with connect_to_database() as connection:
            exec_table = pd.read_sql(f"""select STATUS,MODULE from SRC_STATUS where RUN_JOB = 3 and id = {curr_src_id} and audit_id = {curr_audit_id};""",con=connection) #type: ignore
            # todo: Handle scenario where quarters_df is empty
        
        months_lst = get_quarters(batch_id=audit_id)
        capture_log_message(log_message='Exec_table value is :{}'.format(exec_table),store_in_db=False)
        capture_log_message(f"Current audit id {curr_audit_id} current client id {curr_client_id} current src id {curr_src_id}")
        if exec_table.shape[0] == 0:
            update_current_job(curr_src_id,2, curr_audit_id)
        else:
            exe_id = str(exec_table['STATUS'][0])
            src_id = curr_src_id
            module = str(exec_table['MODULE'][0])
            capture_log_message(log_message='Current Source ID:{} Exe id :{}, Module:{}, Audit_ID:{}'.format(src_id,exe_id,module,audit_id),store_in_db=False)
            if exe_id == '1': #Ingestion
                #module = str(exec_table['MESSAGE'][0]).split()[0]
                if module=='GL':
                    GL = GL_Ingestor('Ingestor/DB.json',curr_audit_id,curr_client_id)
                    update_current_job(curr_src_id,2, curr_audit_id)
                    ingestion_start_time = datetime.now(timezone.utc)
                    ingestion_status = GL.run_job()
                    if (ingestion_status):
                        ingestion_end_time = datetime.now(timezone.utc)
                        capture_log_message(log_message='Time for data ingestion:{}'.format(ingestion_end_time-ingestion_start_time))
                        capture_log_message(f"Started Dynamic Flat Tables Creation for Months:{months_lst}")
                        obj = ShardingTables()
                        obj.create_flat_dupl_for_each_month(months=months_lst,module=g.module_nm)
                        process_details = "SRC_GL Ingestion Completed"
                        update_ingestion_status(src_id,process_details,curr_audit_id)
                        ## Scoring
                        scoring_status = gl_main(curr_audit_id)
                        if (scoring_status):
                            update_status = update_flat_table(audit_id=curr_audit_id, months_lst=months_lst)
                            if (update_status):
                                process_details = "SRC_GL Scoring Completed"
                                update_scoring_status(src_id,process_details, curr_audit_id)
                                mark_pipeline_as_complete()
                                return True
                                # mail_notification(src_id)
                            else:
                                return False
                        else:
                            return False
                    else:
                        return False
                else:
                    AP = AP_Ingestor('Ingestor/DB.json',curr_audit_id,curr_client_id)
                    update_current_job(curr_src_id,2, curr_audit_id)
                    ingestion_status = AP.run_job()
                    if (ingestion_status):
                        capture_log_message(f"Started Flat-Dupl Table Creation for Months:{months_lst}")
                        obj = ShardingTables()
                        obj.create_flat_dupl_for_each_month(months=months_lst,module=g.module_nm)
                        process_details = "SRC_AP Ingestion Completed"                    
                        update_ingestion_status(src_id,process_details,curr_audit_id)
                        ## Scoring
                        scoring_status = ap_main(curr_audit_id)
                        if (scoring_status):
                            update_status = update_apflat_table(audit_id=curr_audit_id, months_lst=months_lst)
                            if (update_status):
                                process_details = "SRC_AP Scoring Completed"
                                update_scoring_status(src_id,process_details, curr_audit_id)
                                mark_pipeline_as_complete()
                                return True
                            else:
                                return False
                        else:
                            return False
                    
                    else:
                        return False

            elif exe_id =='2': #Scoring
                #module = str(exec_table['MESSAGE'][0]).split()[0]
                
                if module=='GL':
                    update_current_job(curr_src_id,2, curr_audit_id)
                    scoring_status = gl_main(curr_audit_id)
                    if (scoring_status):
                        update_status = update_flat_table(curr_audit_id)
                        if (update_status):
                            process_details = "SRC_GL Scoring Completed"
                            update_scoring_status(src_id,process_details, curr_audit_id)
                            mark_pipeline_as_complete()
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    update_current_job(curr_src_id,2, curr_audit_id)
                    scoring_status = ap_main(curr_audit_id)
                    if (scoring_status):
                        update_status = update_apflat_table(curr_audit_id)
                        if (update_status):
                            process_details = "SRC_AP Scoring Completed"
                            update_scoring_status(src_id,process_details, curr_audit_id)
                            mark_pipeline_as_complete()
                            return True
                        else:
                            return False
                    else:
                        return False
            else :
                capture_log_message(current_logger=g.stage_logger,
                                    log_message='Ingestion and Scoring already done on the data')
                g.data_for_external_mail.append({
                    'stage':utils.DATA_SCORING_STAGE,'status':'success',
                    'description':'Ingestion and Scoring already done on the data',
                    'time_taken':'NA','date':datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    'record_count':'NA'})
                mark_pipeline_as_complete()
                return True
                
        
    # time.sleep(300)
    # post_turnOFF()
