from code1 import src_load
from datetime import datetime, timezone
import utils
from functools import lru_cache
from secret_manager import get_credentials
from flask import g

@lru_cache(maxsize=None)
def get_database_credentials():
    return get_credentials("DB")

def get_local_run_history_id(call_id):
    ''' This function is used to get the run history id for the current run
    Args:
    call_id : int : call_id of the current 
    Returns:
    int : run_history_id of the current run
    '''
    try:
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = "INSERT INTO run_history (call_id, started_at) VALUES (%s, %s);"
                values = (call_id,datetime.now(timezone.utc))
                cursor.execute(query, values)
                connect.commit()
                result = cursor.lastrowid
                connect.close()
        return result
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
        
def update_endtime_for_current_run(end_time):
    ''' This function is used to update the end time for the current run
    Args:
    end_time : datetime : end time of the current run
    '''
    try:
        
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = "UPDATE run_history SET completed_at = %s WHERE run_history_id = %s;"
                values = (end_time,g.run_id)
                cursor.execute(query, values)
                connect.commit()
                connect.close()
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
    
def initiate_and_assign_run_id(call_id):
    ''' This function is used to initiate and assign run id for the current run
    Args:
    call_id : int : call_id of the current run
    '''
    try:
        g.run_id = get_local_run_history_id(call_id)
        # g.run_id = g.run_id
        g.general_logger.debug("Run ID:{} is assigned for the current run".format(g.run_id))
            
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        

def get_all_module_name_and_id_details():
    ''' This function is used to get all module name and id details'''
    try:
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = "SELECT  module_name, module_id FROM module;"
                cursor.execute(query)
                result = cursor.fetchall()
                connect.close()
                g.module_name_to_id_mapping =  dict(result)
                g.general_logger.debug("Module Name to ID Mapping:{}".format(g.module_name_to_id_mapping))
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
def get_all_pipeline_type_and_id_details():
    ''' This function is used to get all pipeline type and id details'''
    try:
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor: 
                query = "SELECT call_name ,call_id  from call_list cl WHERE is_active =1;"
                cursor.execute(query)
                result = cursor.fetchall()
                connect.close()
                res_dict = {}
                for key, value in dict(result).items():
                    if str(key).lower() == 'ap':
                        res_dict['ap'] = value
                    if  str(key).lower() == 'gl':
                        res_dict['gl'] = value
                g.pipeline_type_to_id_mapping =  {'ap':res_dict.get('ap',1), 'gl':res_dict.get('gl',2),'zblock':res_dict.get('zblock',3)}
                g.general_logger.debug("Pipeline Type to ID Mapping:{}".format(g.pipeline_type_to_id_mapping))
        
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
        
def get_all_error_code_details():
    """ This function fetches all error coode details from the database"""
    try:
        g.error_name_to_code_mapping = {}
        with src_load.connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = "SELECT error_name,error_code from error_codes;"
                cursor.execute(query)
                results = cursor.fetchall()
               
        res_dict = {} 
        for key, value in dict(results).items():
            res_dict[key] = value
            
        g.error_name_to_code_mapping = res_dict
            
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
        
def capture_log_in_database(run_id,log_type,started_at,module_id,
                            completed_at=None,volume=None,is_success=None,
                            failed_rows=None,total_rows=None,log_message=None,error_code=None):
    ''' This function is used to capture log in database
    Args:
    run_id : int : run_id of the current run
    log_type : str : type of log
    started_at : datetime : start time of the log
    module_id : int : module id of the log
    completed_at : datetime : end time of the log
    volume : int : volume of the log
    is_success : int : success status of the log
    failed_rows : int : failed rows of the log
    total_rows : int : total rows of the log
    log_message : str : log message
    '''
    try:
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = """INSERT INTO log_history 
                        (run_history_id,log_type,started_at,completed_at,
                        module_id,volume,is_success,failed_rows,
                        total_rows,description, audit_id,error_code)
                        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                values = (run_id,log_type,started_at,completed_at,module_id,volume,is_success,failed_rows,total_rows,log_message, g.audit_id,error_code)
                cursor.execute(query, values)
                connect.commit()
                connect.close()
    except Exception as e:
        g.error_logger.error("Error Occurred in internal database call:{}".format(e))
        
        
        
def external_api_call_to_send_suceess_data():
    #Code to make external api call on successful run of the pipeline
    pass
        
def check_license_validation():
    # Code to check whether license is valid, added in future
    return 200

def check_expiry_date():
    ''' This function is used to check the expiry date
    Returns:
    bool : True if the current date is less than or equal to the expiry date, False otherwise
    '''
    current_date = datetime.now().date()
    expiry_date = utils.EXPIRY_DATE
    res = current_date <= expiry_date
    return res

def fetch_client_name(only_client=False):
    try:
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cur:
                if only_client:
                    query = """select client_name from client where client_id= %s"""
                    cur.execute(query,(g.client_id,))
                    results = cur.fetchall()
                    if len(results)>0:
                        g.CLIENT_NAME = results[0][0]
                        g.AUDIT_NAME = 'Custom Audit'
                    else:
                        g.CLIENT_NAME = "Custom Client"
                        g.AUDIT_NAME = 'Custom Audit'
                    
                else:
                    query = """select cl.client_name,au.audit_name from client cl
                                join audit au
                                where au.audit_id = %s
                                and cl.client_id = au.client_id"""
                    
                    cur.execute(query,(g.audit_id,))
                    results = cur.fetchall()
                    if (len(results)!=0):
                        client_name  = results[0][0]
                        audit_name = results[0][1]
                        g.CLIENT_NAME = client_name if client_name is not None else "custom Client"
                        g.AUDIT_NAME  = audit_name if audit_name is not None else "Custom Audit"
    except Exception as e:
        if hasattr(g,'error_logger'):
            g.error_logger.error(f'Error Occurred in fetching client name:{e}')


def update_historical_data_workflow_status(hist_data_id,status_id,created_by):
    try:
        query = """ INSERT into client_historical_status_mapping (client_historical_data_id,status_id,created_by) values (%s,%s,%s)"""
        with src_load.connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query,(hist_data_id,status_id,created_by))
                connection.commit()
                
    except Exception as e:
        if hasattr(g,'error_logger'):
            g.error_logger.debug(f'Error Occurred while updating hist id status :{str(e)}')
        else:
            from code1.logger import logger
            logger.error(f'Error Occurred while updating hist id status :{str(e)}')


def update_batch_upload_status_mapping(batch_id,status_id):
    try:
        query = """ INSERT into batch_upload_status_mapping (batch_id,status_id) values (%s,%s)"""
        with src_load.connect_to_database() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query,(batch_id,status_id))
                connection.commit()
                
    except Exception as e:
        if hasattr(g,'error_logger'):
            g.error_logger.debug(f'Error Occurred while updating batch upload status :{str(e)}')
        else:
            from code1.logger import logger
            logger.error(f'Error Occurred while updating batch upload status :{str(e)}')


