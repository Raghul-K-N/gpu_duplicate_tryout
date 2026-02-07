from flask import g,jsonify
import os
from local_database import check_license_validation,check_expiry_date
from code1.logger import initialise_logger,capture_log_message,update_current_module
from databases.sharding_tables import ShardingTables
from datetime import datetime, timezone
from code1 import src_load
from utils import create_job_query

def perform_ap_module_ingestion_and_scoring_flow(batch_id):
    """  Perform AP Module Ingestion and Scoring Flow  """
    response_status = check_license_validation()
    is_date_valid = check_expiry_date()
    g.audit_id = batch_id
    g.client_id = os.getenv("CLIENT_ID",1)
    g.module_nm = "AP"

    if (response_status != 200) or (not is_date_valid):
        return jsonify({'error':'Invalid License. Please contact the administrator.'}),403
    
    initialise_logger('ap',audit_id=g.audit_id,batch_id=batch_id,module_name=g.module_nm)

    # Dynamically Create tables for Each audit based on audit_id
    # NOTE: these tables need not be needed, since AP flow depends on month wise sharded tables

    obj = ShardingTables()
    obj.create_tables_for_each_audit(g.audit_id, g.module_nm)
    # update_audit_status(g.audit_id, 2) # Set status as Ongoing
    capture_log_message(log_message='Starting custom-client-ap pipeline')
    update_current_module('upload')
    start_time = datetime.now(timezone.utc)
    
    capture_log_message(log_message=' custom-client-ap START:{}'.format(start_time))

    src_status = {
        "status":-1,
        "module":"AP",
        "audit_id":batch_id ,
        "client_id":g.client_id,
        "filename":'client_ap_data.xlsx',
        "message":"FILE UPLOADED",
        "created_by":4
    }

    
    src_id = create_job_query(src_status)
   
    from init import init_main
    
    src_load.update_job_status(1, "Ingestion Started",src_id,0,"",1, batch_id)
    src_load.update_current_job(src_id,1, src_status['audit_id'])
    src_load.update_job_status_runjob(src_id,3,f"VM Started_{batch_id}", src_status['audit_id'])
    
    # AP Module Ingestion and Scoring Flow
    response = init_main(src_id,g.audit_id,g.client_id)
    process_end_time= datetime.now(timezone.utc)
    message = 'AP Data Ingestion and scoring has been completed in {} seconds.You can view the results in dashboard'.format(process_end_time-start_time)
    capture_log_message(log_message=message)


    if response:
        response_data = [message]
       # check if g variable has ap_ingestion_end_time attribute
        if hasattr(g, 'ap_ingestion_end_time'):
           ap_ingestion_end_time = g.ap_ingestion_end_time
        else:
           ap_ingestion_end_time = process_end_time
        time_taken_for_ap_ingestion = ap_ingestion_end_time - start_time
        time_taken_for_scoring = process_end_time - ap_ingestion_end_time
       
        capture_log_message(f"{message} AP Ingestion Time Taken: {time_taken_for_ap_ingestion}, AP Scoring Time Taken: {time_taken_for_scoring}")
        data = {
            "message" : response_data,
            "batch_id" : batch_id
        }

        return jsonify(data),200
    else:
        response_data = ["Failed: AP Data Ingestion and scoring has failed"]
        capture_log_message(f"{response_data}")
        data = {
            'message': response_data
        }
        return jsonify(data),500
    

    


    


