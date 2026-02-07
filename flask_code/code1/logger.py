from datetime import datetime, timedelta, timezone
import logging
import time
import os
from email_service import send_client_process_mail, send_internal_stage_email
from code1 import src_load
from local_database import capture_log_in_database, fetch_client_name, get_all_error_code_details, get_all_module_name_and_id_details, get_all_pipeline_type_and_id_details, initiate_and_assign_run_id
from itertools import zip_longest
from flask import g


script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)
log_directory = os.path.join(base_directory,'logs')
if not os.path.exists(log_directory):
    os.mkdir(log_directory)
    
log_format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'


log_filename = 'root_log.log'  # You can change this to the desired root log file name
root_handler = logging.FileHandler(log_filename)
root_handler.setLevel(logging.DEBUG)
root_handler.setFormatter(logging.Formatter(log_format))
logger = logging.getLogger('root_log')
logger.addHandler(root_handler)
logger.setLevel(logging.DEBUG)


def initialise_general_logger(log_folder_path: str):
    ''' This function is used to initialise general logger
    Args:
    log_folder_path : str : log folder path
    '''
    filename_t = time.strftime("%Y%m%d-%H%M%S") + "-%03d" % (datetime.now().microsecond // 1000)
    file_name = os.path.join(log_folder_path, "General_log_{}.log".format(filename_t))
    handler = logging.FileHandler(file_name.format(filename_t))
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(log_format))
    
    logger = logging.getLogger('main_log_'+str(filename_t))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    return logger

def initialise_error_logger(log_folder_path: str):
    ''' This function is used to initialise error logger
    Args:
    log_folder_path : str : log folder path
    '''
    filename_t = time.strftime("%Y%m%d-%H%M%S") + "-%03d" % (datetime.now().microsecond // 1000)
    file_name = os.path.join(log_folder_path, "Error_log_{}.log".format(filename_t))
    handler = logging.FileHandler(file_name.format(filename_t))
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(log_format))
    
    logger = logging.getLogger('error_log+'+str(filename_t))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    return logger


def initialise_stage_logger(log_folder_path: str):
    ''' This function is used to initialise stage logger
    Args:
    log_folder_path : str : log folder path
    '''
    filename_t = time.strftime("%Y%m%d-%H%M%S") + "-%03d" % (datetime.now().microsecond // 1000)
    file_name = os.path.join(log_folder_path, "Stage_log_{}.log".format(filename_t))
    handler = logging.FileHandler(file_name.format(filename_t))
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(log_format))
    
    logger = logging.getLogger('stage_log_'+str(filename_t))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    return logger


def close_and_reset_loggers():
    ''' This function is used to close and reset loggers '''
    logging.getLogger('error_log').handlers.clear()
    logging.getLogger('stage_log').handlers.clear()
    logging.getLogger('main_log').handlers.clear()
    # logging.shutdown()
    
    
def mark_pipeline_as_complete():
    g.final_status = True


def clear_all_variable_values():
    ''' This function is used to clear all variable values'''
    # g.general_logger = None
    # g.error_logger = None
    # g.stage_logger = None
    # utils.run_id = None
    # utils.audit_id = None
    # utils.client_id = None
    # utils.pipeline_id = None
    g.pipeline_name = None
    g.current_module = None
    g.final_status = None
    g.parqeut_storage_start = False
    g.module_name_to_id_mapping={}
    g.pipeline_type_to_id_mapping={}
    g.data_for_external_mail = []
    g.data_for_external_api_call = {}
    
    # close_and_reset_loggers()
    
def add_data_to_external_api_call(key, json_value):
    ''' This function is used to add data to external api call
    Args:
    key : str : key
    json_value : dict : json value pair containing data to be added
    '''
    try:
        if json_value:    
            dict_key, dict_value = next(iter(json_value.items()))
            if key not in g.data_for_external_api_call.keys():
                g.data_for_external_api_call[key] = {}
                g.data_for_external_api_call[key][dict_key] = dict_value
            else:
                g.data_for_external_api_call[key][dict_key] = dict_value
                
            g.general_logger.debug(f"Data added to external api call: {key} : {json_value}")
            
    except Exception as e:
        g.error_logger.error(f"Error occurred while adding data to external api call: {str(e)}")
        
    
def update_current_module(module_name):
    ''' This function is used to update current module
    Args:
    module_name : str : module name
    '''
    g.current_module = g.module_name_to_id_mapping.get(module_name,None)
    g.general_logger.debug(f"Current Module: {g.current_module}")
    
def update_start_time_for_audit_id():
    try:
        starttime = datetime.now(timezone.utc)
        capture_log_message(f"Update start time {starttime} for audit id {g.audit_id};")
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = "UPDATE audit set started_at=%s where audit_id =%s;"
                cursor.execute(query,(starttime,g.audit_id))
                connect.commit()
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=f"Error occurred :{e}")
    
        

def update_data_time_period_for_audit_id(time_period):
    try:
        capture_log_message(f"Update data time period {time_period} for audit_id {g.audit_id}")
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                
                query = f"UPDATE audit set data_time_period='{time_period}' where audit_id ={g.audit_id} "
                cursor.execute(query)
                connect.commit()
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=f"Error occurred :{e}")
    
def update_total_time_taken_for_audit(time_period:float):
    try:
        capture_log_message(f"Update total time taken {time_period} for audit_id {g.audit_id}")
        with  src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = f"UPDATE audit set time_taken= '{time_period}' where audit_id ={g.audit_id} "
                cursor = connect.cursor()
                cursor.execute(query)
                connect.commit()
                
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=f"Error occurred :{e}")
    
def update_total_account_doc_for_audit_id(acc_doc:int):
    try:
        capture_log_message(f"Update total acc doc count {acc_doc} for audit id {g.audit_id}")
        with src_load.connect_to_database() as connect:
            with connect.cursor() as cursor:
                query = f"UPDATE audit set acc_doc= '{acc_doc}' where audit_id ={g.audit_id} "
                cursor = connect.cursor()
                cursor.execute(query)
                connect.commit()
    except Exception as e:
        capture_log_message(current_logger=g.error_logger,log_message=f"Error occurred :{e}")
    
def initialise_logger(folder_path:str,audit_id,batch_id=0,module_name="ap",historical_flow=False):
    ''' This function is used to initialise logger
    Args:
    folder_path : str : folder path
    batch_id : int : batch identifier (optional, default: 0)
    module_name : str : module name (e.g., 'ap', 'zblock', default: 'ap')
    historical_flow : bool : whether this is historical data flow
    '''
    fetch_client_name(only_client=True)
    base_path = os.getenv('UPLOADS', log_directory)
    master_folder = os.getenv('MASTER_FOLDER', 'dow_transformation')
    master_log_folder = os.getenv('MASTER_LOG_FOLDER', 'master_logs')
    
    filename_t = time.strftime("%Y%m%d-%H%M%S")+'_'+str(audit_id)
    hist_flag = "hist" if historical_flow else ""
    
    # Construct folder name: <module_name>_<hist_flag>_<timestamp>_<audit_id>
    if hist_flag:
        folder_name = f"{module_name}_{hist_flag}_{filename_t}"
    else:
        folder_name = f"{module_name}_{filename_t}"
    
    # Path: UPLOADS -> MASTER_FOLDER -> MASTER_LOG_FOLDER -> batch_<batch_id>_logs -> folder_name
    folder_path = os.path.join(base_path, master_folder, master_log_folder, f"batch_{batch_id}_logs", folder_name)
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path,exist_ok=True)
    
    clear_all_variable_values()
    g.general_logger = initialise_general_logger(folder_path)
    g.error_logger = initialise_error_logger(folder_path)
    g.stage_logger = initialise_stage_logger(folder_path)
    get_all_module_name_and_id_details()
    get_all_pipeline_type_and_id_details()
    get_all_error_code_details()
    g.pipline_name = module_name
    g.pipeline_id = g.pipeline_type_to_id_mapping.get(module_name,1)
    # if '_ap' in folder_path.lower():
    #     g.pipeline_id = g.pipeline_type_to_id_mapping.get('ap',None)
    #     g.pipeline_name = 'ap'
    # if '_gl' in folder_path.lower():
    #     g.pipeline_id = g.pipeline_type_to_id_mapping.get('gl',None)
    #     g.pipeline_name = 'gl'
    # if '_vm' in folder_path.lower():
    #     g.pipeline_id = g.pipeline_type_to_id_mapping.get('vm',None)
    #     g.pipeline_name = 'vm'
    # if '_zblock' in folder_path.lower():
    #     g.pipeline_id = g.pipeline_type_to_id_mapping.get('zblock',None)
    #     g.pipeline_name = 'zblock'
    initiate_and_assign_run_id(g.pipeline_id)
    start_time = datetime.now(timezone.utc)
    g.start_time = start_time
    update_start_time_for_audit_id()
    add_data_to_external_api_call(key='run',json_value={'started_at':start_time.strftime("%Y-%m-%d %H:%M:%S")})
    if not historical_flow:
        fetch_client_name()
    

def capture_log_message(log_message,current_logger=None,data_shape=None,time_taken=None,
                        store_in_db=True,start_time=None,end_time=None,error_name= None):
    ''' This function is used to capture log message
    Args:
    log_message : str : log message
    current_logger : logger : current logger
    data_shape : tuple : data shape
    time_taken : float : time taken
    store_in_db : bool : store in database
    start_time : datetime : start time
    end_time : datetime : end time
    '''
    if log_message==None:
        log_message=""
    
    # Handle missing audit_id gracefully
    audit_id_str = str(g.audit_id) if hasattr(g, 'audit_id') and g.audit_id else "NO_AUDIT"
    log_message = str(log_message) + '- for Audit ' + audit_id_str
    
    if current_logger is None:
        current_logger = g.general_logger if hasattr(g, 'general_logger') and g.general_logger else logger
    data_shape_log = "" if data_shape is None else 'Data shape:{}'.format(str(data_shape))
    time_taken_log = "" if time_taken is None else 'Time taken:{}'.format(str(time_taken))
    start_time_log = "" if start_time is None else 'Start time:{}'.format(start_time.strftime("%Y-%m-%d %H:%M:%S"))
    end_time_log = "" if end_time is None else 'End time:{}'.format(end_time.strftime("%Y-%m-%d %H:%M:%S"))
    error_code = None if error_name is None else g.error_name_to_code_mapping.get(error_name, None) if hasattr(g, 'error_name_to_code_mapping') else None
    formatted_log_message = f"{log_message} {data_shape_log} {start_time_log} {end_time_log} {time_taken_log}"
    current_logger.debug(formatted_log_message)
    
    if (store_in_db and hasattr(g, 'run_id')):
        # store values in database (only if run_id exists)
        is_success = None
        log_type = None
        if current_logger == g.general_logger:
            log_type = 1
        if current_logger == g.error_logger:
            is_success = False
            log_type = 2
        if current_logger == g.stage_logger:
            is_success = True
            log_type = 3
        if start_time is None:
            start_time = datetime.now(timezone.utc)
            
        if data_shape is not None:
            data_shape = data_shape[0]
        
        if len(log_message)>65000:
            log_message = log_message[:65000]
        
        # Get optional values with fallbacks
        run_id = g.run_id if hasattr(g, 'run_id') else None
        current_module = g.current_module if hasattr(g, 'current_module') else None
        
        capture_log_in_database(run_id=run_id,log_type=log_type,started_at=start_time,completed_at=end_time,
                                module_id=current_module,log_message=log_message,volume=data_shape,
                                is_success=is_success,failed_rows=None,total_rows=None,error_code=error_code)
        
    

def convert_time_into_human_readable_format(time_taken:timedelta):
    seconds = time_taken.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    time_string = ""
    if hours > 0:
        time_string += f"{int(hours)} hr "
    if minutes > 0:
        time_string += f"{int(minutes)} mins "
    time_string += f"{int(seconds)} sec"

    return time_string



def process_data_for_sending_internal_mail(subject,stage,is_success,date_list=[],
                                volume_list=[],time_taken_list=[],description_list=[],failed_count_list=[],total_count_list=[],
                                failed_column_list=[],failed_reason_list=[],historical_flow=False):
    ''' This function is used to process data for sending internal mail
    Args:
    subject : str : subject of the mail
    stage : str : stage in the pipeline
    is_success : bool : status of the module in pipeline
    date_list : list : list of dates
    volume_list : list : list of volumes
    time_taken_list : list : list of time taken
    description_list : list : list of descriptions
    failed_count_list : list : list of failed count if applicable
    total_count_list : list : list of total count if applicable
    failed_column_list : list : list of failed column if applicable
    failed_reason_list : list : list of failed reason if applicable
    '''
    try:
        pipeline = g.pipeline_name.upper()
        if historical_flow:
            subject = 'HISTORICAL DATA ' + g.CLIENT_NAME + " " + pipeline+" "+ str(subject)
        else:
            subject = g.CLIENT_NAME +" "+ pipeline+" " + str(subject)
        email_data = []
        if len(failed_column_list)>1:
            # Have values for all entried if we have more than one failed column
            time_taken_list = time_taken_list*len(failed_column_list)
            date_list = date_list*len(failed_column_list)
            total_count_list = total_count_list*len(failed_column_list)
        # Iterate through all the lists and create a dictionary for each record
        for values in zip_longest(date_list,volume_list,time_taken_list,description_list,
                                  failed_count_list,total_count_list,failed_column_list,failed_reason_list,fillvalue=None):
            # Capture the date values outside the loop as date is common for all Entries
            date_value = values[0].strftime('%Y-%m-%d') if isinstance(values[0], datetime) else None 
            volume_value = '{:,}'.format(values[1][0]) if values[1] is not None else 'NA'
            time_taken_value = convert_time_into_human_readable_format(values[2]) if isinstance(values[2], timedelta) else None 
            description_value = values[3] if values[3] is not None else None
            failed_rows_count = values[4] if values[4] is not None else 'NA'
            total_rows_count = values[5][0] if values[5] is not None else 'NA'
            failed_column = values[6] if values[6] is not None else 'NA'
            failed_reason = values[7] if values[7] is not None else 'NA'
            temp_dict = {"record_count":volume_value,"time_taken":time_taken_value,
                        "description":description_value,"date":date_value,
                        "failed_count":failed_rows_count,"total_count":total_rows_count,
                        "reason":failed_reason,"column_name":failed_column}
            email_data.append(temp_dict)
            
            # Store data for sending external client mails
            external_mail_data = {"stage":stage,'status':'success' if is_success else 'failed',
                              'date':date_value,'description':description_value,'time_taken':time_taken_value,
                              'record_count':volume_value,'failed_count':failed_rows_count,
                              'total_count':total_rows_count,'reason':failed_reason,'column_name':failed_column}
            g.data_for_external_mail.append(external_mail_data)
        
        g.general_logger.debug('Internal Email data:{}'.format(email_data))
        res = send_internal_stage_mail(subject, stage, pipeline, is_success, email_data)
        if (res[0]):
            g.general_logger.debug(f"Internal mail Status: {res}")
        else:
            g.error_logger.error(f"Internal mail Failed: {res}")
    except Exception as e:
        g.error_logger.error(f"Failed to send internal mail: {str(e)}")
        
        
def process_data_for_sending_external_mail(is_success: bool, pipeline_stages: list):
    try:
        pipeline = g.pipeline_name.upper()
        subject = g.CLIENT_NAME + " " + pipeline+" " +' Status Mail'
        g.general_logger.debug('Final External Email data:{}'.format(g.data_for_external_mail))
        # res = send_client_process_mail(subject=subject, process=pipeline, is_success=is_success, 
        #                                data=g.data_for_external_mail,pipeline_stages=pipeline_stages)
        # if (res[0]):
        #     g.general_logger.debug(f"External mail Status: {res}")
        # else:
        #     g.error_logger.error(f"External mail Failed: {res}")

    except Exception as e:
        g.error_logger.error(f"Failed to send external mail: {str(e)}")
    
    


# BASEDIR = os.getcwd()
# LOG_FILE_PATH = os.path.join(BASEDIR,"logs")
# if not os.path.exists(LOG_FILE_PATH):
#     os.mkdir(LOG_FILE_PATH)
# filename_t = time.strftime("%Y%m%d-%H%M%S")
# file_name = os.path.join(LOG_FILE_PATH,"CatchError_log{}.log".format(filename_t))
# logging.basicConfig(filename = file_name.format(filename_t),level = logging.DEBUG,format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
# logger = logging.getLogger('main_log')
