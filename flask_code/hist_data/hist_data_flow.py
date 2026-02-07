from flask import Blueprint,g,jsonify
import pandas as pd
from local_database import check_license_validation,check_expiry_date, update_batch_upload_status_mapping, update_historical_data_workflow_status
from datetime import datetime, timezone
from code1.logger import initialise_logger,capture_log_message,update_current_module,process_data_for_sending_internal_mail

import os 
from code1 import src_load ,mainflow
import utils

hist_bp = Blueprint("hist_data", __name__)

@hist_bp.route("/custom_hist_ap/<int:hist_id>", methods=["GET"])
def custom_hist_ap(hist_id):
    response_status = check_license_validation()
    is_date_valid = check_expiry_date()
    g.user_id = 1
    g.hist_id = hist_id
    # Historical Data flow Does not Have Any audit ID values , since there is no concept of 
    # audits in historical uploads, so giving dummy values for audit id
    g.audit_id =  int(f"{hist_id}") 
    g.client_id = os.getenv("CLIENT_ID",1)
    g.erp_id = os.getenv("ERP_ID",1)
    g.module_nm = "AP"
    g.excel_flag = None
    if (response_status != 200) or (not is_date_valid):
        return jsonify({'error':'Invalid License. Please contact the administrator.'}),403
    
    
    initialise_logger(folder_path='ap',audit_id=g.hist_id,historical_flow=True)

    capture_log_message(f"Data fetched from DB for hist id {g.hist_id} Client id :{g.client_id}, erp_id  is {g.erp_id} for module {g.module_nm}")
    capture_log_message(f"Setting status as Ongoing - 2 for hist id {g.hist_id} and user id {g.user_id}")
    # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=2) # Ongoing
    capture_log_message(log_message='Starting Historical data-ap pipeline')
    update_current_module('upload')
    start_time = datetime.now(timezone.utc)
    files = []
    transactions = None
    encodings = ["utf-16","utf-8","CP1252","ISO-8859-1"]
    
    input_path =os.getenv("INPUT_FILES_FOLDER_PATH_AP",None)
    
    if input_path is not None:
        capture_log_message(f"Input files folder path from env file :{input_path}")
        input_folder_path = input_path
    else:
        input_folder_path = src_load.get_input_file_path_for_hist_id(hist_id=g.hist_id)
        capture_log_message(f"Input file folder path from DB:{input_folder_path}")
    if input_folder_path is not None:
        files_path  = os.path.join( os.getenv("UPLOADS","") , str(input_folder_path).lstrip('/')) 
        g.input_folder_path_for_historical_data = files_path
    else:
        capture_log_message(current_logger=g.error_logger,
                            log_message='Could not fetch input files path from client_historical_data table',
                            error_name=utils.NO_INPUT_FILES)
        return jsonify({'error':'Could not find input data files to read from the path'}),404
    capture_log_message(f'Input file path  is {files_path}')
    capture_log_message(log_message=' custom-hist-data-ap START:{}'.format(start_time))
    from ap_sap_data_loader import get_ap_sap_data
    transactions = get_ap_sap_data()
    
    # transactions = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\notebooks\current_data_ap_flow.csv")

    
    file_read_time = datetime.now(timezone.utc)

    src_status = {
        "status":-1,
        "module":g.module_nm,
        "audit_id":g.audit_id ,
        "client_id":g.client_id,
        "filename":'Hist client_ap_data.xlsx',
        "message":"FILE UPLOADED",
        "created_by":4
    }
    time_taken_to_upload = file_read_time - start_time
    if transactions is not None:
        capture_log_message(log_message=' Time taken to read AP files:{}'.format(time_taken_to_upload))
        capture_log_message(current_logger=g.stage_logger,log_message=' Data Uploaded Successfully for invoice files',
                            start_time=start_time,end_time=file_read_time,
                            data_shape=transactions.shape,time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
  
        g.hist_upload_status = True
        time_taken_to_upload = file_read_time - start_time
        msg_success = "Data successfully uploaded for AP processing. Number of records uploaded for AP: {}".format(transactions.shape[0])
        capture_log_message(log_message=msg_success)
    else:
        
        time_taken_to_upload = file_read_time - start_time
        data = {"error": "Something went wrong. Check the uploads directory"}
        capture_log_message(current_logger=g.error_logger,
                            log_message='{}'.format(data["error"]),
                            start_time=start_time,end_time=file_read_time,
                            time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
        
        # process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
        #                                        date_list=[start_time],time_taken_list=[time_taken_to_upload],
        #                                        description_list=['Data Upload Failed'],historical_flow=True)
        return jsonify(data)  
        
    src_id = utils.create_job_query(src_status)
    capture_log_message(log_message='Data loaded into SRC_STATUS,Source id:{}'.format(src_id))

    # mappinggroup_id = utils.set_mappings(src_status['module'], mappings_csv,src_status['client_id'])
    # g.mappinggroup_id = utils.get_random_int(max_int=999999)
    # configs = utils.get_config("configs.json")
    # utils.set_config(configs, mappinggroup_id,"AP",src_status['client_id'])
    update_current_module('health_check')

    # Add Entry into batch Table
    batch_id = src_load.create_new_batch_entry(module_nm='AP')
    if batch_id is None:
        error_message = 'Failed to create new batch entry'
        capture_log_message(current_logger=g.error_logger,log_message=error_message,error_name=utils.OTHER_ERRORS)
        return {'error':error_message},400
    else:
        capture_log_message(log_message='Batch entry created successfully with batch id:{}'.format(batch_id))
        g.batch_id = batch_id

    response = mainflow.do_preprocess_ap(transactions, src_id, src_status['audit_id'], src_status['client_id'])
   
    process_end_time= datetime.now(timezone.utc)
    time_taken_for_preprocessing = process_end_time - file_read_time
    if response["Result"]!="Success":
        message = "AP Preprocessing has failed due to issues in the data. The processing was completed in {} seconds. Please verify the data.".format(time_taken_for_preprocessing)
        capture_log_message(log_message=message)
        return response.get('data',{'Process':'Failed'})
    else:
        message = "AP Health check and Data Preprocessing has been completed in {} seconds.".format(time_taken_for_preprocessing)
        capture_log_message(log_message=message)
        df_data =  response.get('dataframe',pd.DataFrame())
        final_data = pd.DataFrame(df_data)
        # Store no. of rows present in the uploaded data for a batch in metadata table before checking for any duplicates
        data_start = final_data['POSTED_DATE'].min()
        data_end = final_data['POSTED_DATE'].max()
        src_load.add_no_of_rows_present_in_the_uploaded_data_in_batch(batch_id=g.batch_id, no_of_rows=final_data.shape[0],data_start= data_start, data_end=data_end)
        
        from hist_data.handle_hist_data import store_hist_data_in_parquet_file
        from hist_data.vat_data_flow import store_vat_data
        from hist_data.gl_account_tracker_flow import store_gl_account_tracker_data
        from hist_data.vendor_data_flow import store_vendor_data
        from hist_data.invoice_transaction_mapping_data_flow import store_invoice_transaction_mapping_data
        
        g.parqeut_storage_start =True
        historical_data_flag,status_message, new_data_flag = store_hist_data_in_parquet_file(batch_id=g.batch_id,df=final_data)
        
        # Get environment flags for different data files
        VAT_DATA_FLAG = int(os.getenv("VAT_DATA_FILE", "0")) == 1
        GL_TRACKER_FLAG = int(os.getenv("GL_TRACKER_FILE", "0")) == 1
        VENDOR_DATA_FLAG = int(os.getenv("VENDOR_DATA_FILE", "0")) == 1
        INV_TXN_MAP_FLAG = int(os.getenv("INVOICE_TRANSACTION_MAPPING_FILE", "0")) == 1

        capture_log_message("Environment flags - VAT: {}, GL_TRACKER: {}, VENDOR: {}, INV_TXN_MAP: {}".format(
            VAT_DATA_FLAG, GL_TRACKER_FLAG, VENDOR_DATA_FLAG, INV_TXN_MAP_FLAG))

        data_upload_end_time = datetime.now(timezone.utc)
        time_taken_for_data_storage = data_upload_end_time - process_end_time

        # Prepare email data
        success_descriptions = []
        success_volume_list = []
        success_time_taken_list = []
        success_date_list = [start_time]
        failure_descriptions = []
        failure_volume_list = []
        failure_time_taken_list = []
        failure_date_list = [start_time]
        overall_success = True
        
        # Check upload and health check status
        if hasattr(g, 'hist_upload_status') and g.hist_upload_status:
            success_descriptions.append('Data upload Completed')
            success_volume_list.append(transactions.shape)
            success_time_taken_list.append(time_taken_to_upload)

        if hasattr(g, 'hist_health_check_status') and g.hist_health_check_status:
            success_descriptions.append('Data Health Check Completed')
            success_volume_list.append(final_data.shape)
            success_time_taken_list.append(time_taken_for_preprocessing)

        # Check historical data storage
        if historical_data_flag:
            success_descriptions.append('Historical Data Stored Successfully')
            success_volume_list.append(final_data.shape)
            success_time_taken_list.append(time_taken_for_data_storage)
        else:
            overall_success = False
            failure_descriptions.append('Historical Data Storage Failed')
            failure_volume_list.append(final_data.shape)
            failure_time_taken_list.append(time_taken_for_data_storage)

        # Check each data type based on environment flags
        if VAT_DATA_FLAG:
            vat_strt_time = datetime.now(timezone.utc)
            vat_data_flag, vat_shape = store_vat_data()
            vat_end_time = datetime.now(timezone.utc)
            if vat_data_flag:
                success_descriptions.append('VAT Data Stored Successfully')
                success_volume_list.append(vat_shape)
                success_time_taken_list.append(vat_end_time - vat_strt_time)
            else:
                overall_success = False
                failure_descriptions.append('VAT Data Storage Failed')
                failure_volume_list.append(vat_shape)
                failure_time_taken_list.append(vat_end_time - vat_strt_time)

        if GL_TRACKER_FLAG:
            gl_account_strt_time = datetime.now(timezone.utc)
            gl_account_tracker_flag, gl_account_shape = store_gl_account_tracker_data()
            gl_account_end_time = datetime.now(timezone.utc)
            if gl_account_tracker_flag:
                success_descriptions.append('GL Account Tracker Data Stored Successfully')
                success_volume_list.append(gl_account_shape)
                success_time_taken_list.append(gl_account_end_time - gl_account_strt_time)
            else:
                overall_success = False
                failure_descriptions.append('GL Account Tracker Data Storage Failed')
                failure_volume_list.append(gl_account_shape)
                failure_time_taken_list.append(gl_account_end_time - gl_account_strt_time)

        if VENDOR_DATA_FLAG:
            vend_strt_time = datetime.now(timezone.utc)
            vendor_data_flag, vendor_shape = store_vendor_data()
            vend_end_time = datetime.now(timezone.utc)
            if vendor_data_flag:
                success_descriptions.append('Vendor Data Stored Successfully')
                success_volume_list.append(vendor_shape)
                success_time_taken_list.append(vend_end_time - vend_strt_time)
            else:
                overall_success = False
                failure_descriptions.append('Vendor Data Storage Failed')
                failure_volume_list.append(vendor_shape)
                failure_time_taken_list.append(vend_end_time - vend_strt_time)

        if INV_TXN_MAP_FLAG:
            inv_txn_map_strt_time = datetime.now(timezone.utc)
            invoice_transaction_mapping_data_flag, invoice_txn_shape = store_invoice_transaction_mapping_data()
            inv_txn_map_end_time = datetime.now(timezone.utc)
            if invoice_transaction_mapping_data_flag:
                success_descriptions.append('Invoice Transaction Mapping Data Stored Successfully')
                success_volume_list.append(invoice_txn_shape)
                success_time_taken_list.append(inv_txn_map_end_time - inv_txn_map_strt_time)
            else:
                overall_success = False
                failure_descriptions.append('Invoice Transaction Mapping Data Storage Failed')
                failure_volume_list.append(invoice_txn_shape)
                failure_time_taken_list.append(inv_txn_map_end_time - inv_txn_map_strt_time)


        # # Send success email if there are successful operations
        # if overall_success:
        #     process_data_for_sending_internal_mail(
        #         subject='Data Upload Status - Success',
        #         stage=utils.DATA_UPLOAD_STAGE,
        #         is_success=True,
        #         description_list=success_descriptions,
        #         volume_list=success_volume_list,
        #         time_taken_list=success_time_taken_list,
        #         date_list=success_date_list * len(success_descriptions),
        #         historical_flow=True
        #     )

        # # Send failure email if there are failed operations
        # if failure_descriptions:
        #     process_data_for_sending_internal_mail(
        #         subject='Data Upload Status - Failure',
        #         stage=utils.DATA_UPLOAD_STAGE,
        #         is_success=False,
        #         description_list=failure_descriptions,
        #         volume_list=failure_volume_list,
        #         time_taken_list=failure_time_taken_list,
        #         date_list=failure_date_list * len(failure_descriptions),
        #         historical_flow=True
        #     )


        # Final status update and response
        if overall_success:
            # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=4 ) # Completed
            update_batch_upload_status_mapping(batch_id=g.batch_id, status_id=4) # Completed
            # src_load.update_end_time_for_batch_in_batch_metadata_table(batch_id=g.batch_id)  
            capture_log_message("All historical data storage operations completed successfully.")
            return jsonify({'process':'success','batch_id':g.batch_id,'has_new_data':bool(new_data_flag)}),200       
        else:
            error_message = "Historical Data processing completed with errors. Check mail for details."
            if not historical_data_flag:
                error_message = status_message
            capture_log_message(current_logger=g.error_logger,log_message=error_message,error_name=utils.OTHER_ERRORS)
            return jsonify({'error': error_message}),400
            









@hist_bp.route("/custom_hist_ap_zblock/<int:hist_id>", methods=["GET"])
def custom_hist_ap_zblock(hist_id):
    response_status = check_license_validation()
    is_date_valid = check_expiry_date()
    g.user_id = 1
    g.hist_id = hist_id
    # Historical Data flow Does not Have Any audit ID values , since there is no concept of 
    # audits in historical uploads, so giving dummy values for audit id
    g.audit_id =  int(f"{hist_id}") 
    g.client_id = os.getenv("CLIENT_ID",1)
    g.erp_id = os.getenv("ERP_ID",1)
    g.module_nm = "ZBLOCK"
    g.excel_flag = None
    if (response_status != 200) or (not is_date_valid):
        return jsonify({'error':'Invalid License. Please contact the administrator.'}),403
    
    
    initialise_logger(folder_path='zblock',audit_id=g.hist_id,historical_flow=True)

    capture_log_message(f"Data fetched from DB for hist id {g.hist_id} Client id :{g.client_id}, erp_id  is {g.erp_id} for module {g.module_nm}")
    capture_log_message(f"Setting status as Ongoing - 2 for hist id {g.hist_id} and user id {g.user_id}")
    # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=2) # Ongoing
    capture_log_message(log_message='Starting Historical data-zblock pipeline')
    update_current_module('upload')
    start_time = datetime.now(timezone.utc)
    files = []
    transactions = None
    encodings = ["utf-16","utf-8","CP1252","ISO-8859-1"]
    
    input_path =os.getenv("INPUT_FILES_FOLDER_PATH_AP",None)
    
    if input_path is not None:
        capture_log_message(f"Input files folder path from env file :{input_path}")
        input_folder_path = input_path
    else:
        input_folder_path = src_load.get_input_file_path_for_hist_id(hist_id=g.hist_id)
        capture_log_message(f"Input file folder path from DB:{input_folder_path}")
    if input_folder_path is not None:
        files_path  = os.path.join( os.getenv("UPLOADS","") , str(input_folder_path).lstrip('/')) 
        g.input_folder_path_for_historical_data = files_path
    else:
        capture_log_message(current_logger=g.error_logger,
                            log_message='Could not fetch input files path from client_historical_data table',
                            error_name=utils.NO_INPUT_FILES)
        return jsonify({'error':'Could not find input data files to read from the path'}),404
    capture_log_message(f'Input file path  is {files_path}')
    capture_log_message(log_message=' custom-hist-data-zblock START:{}'.format(start_time))
    from ap_sap_data_loader import get_ap_sap_data
    transactions = get_ap_sap_data()
    
    
    file_read_time = datetime.now(timezone.utc)

    src_status = {
        "status":-1,
        "module":g.module_nm,
        "audit_id":g.audit_id ,
        "client_id":g.client_id,
        "filename":'Hist client_ap_data.xlsx',
        "message":"FILE UPLOADED",
        "created_by":4
    }
    time_taken_to_upload = file_read_time - start_time
    if transactions is not None:
        capture_log_message(log_message=' Time taken to read ZBLOCK files:{}'.format(time_taken_to_upload))
        capture_log_message(current_logger=g.stage_logger,log_message=' Data Uploaded Successfully for invoice files',
                            start_time=start_time,end_time=file_read_time,
                            data_shape=transactions.shape,time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
  
        g.hist_upload_status = True
        time_taken_to_upload = file_read_time - start_time
        msg_success = "Data successfully uploaded for ZBLOCK processing. Number of records uploaded for ZBLOCK: {}".format(transactions.shape[0])
        capture_log_message(log_message=msg_success)
    else:
        
        time_taken_to_upload = file_read_time - start_time
        data = {"error": "Something went wrong. Check the uploads directory"}
        capture_log_message(current_logger=g.error_logger,
                            log_message='{}'.format(data["error"]),
                            start_time=start_time,end_time=file_read_time,
                            time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
        
        # process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
        #                                        date_list=[start_time],time_taken_list=[time_taken_to_upload],
        #                                        description_list=['Data Upload Failed'],historical_flow=True)
        return jsonify(data)  
        
    src_id = utils.create_job_query(src_status)
    capture_log_message(log_message='Data loaded into SRC_STATUS,Source id:{}'.format(src_id))

    # mappinggroup_id = utils.set_mappings(src_status['module'], mappings_csv,src_status['client_id'])
    # g.mappinggroup_id = utils.get_random_int(max_int=999999)
    # configs = utils.get_config("configs.json")
    # utils.set_config(configs, mappinggroup_id,"AP",src_status['client_id'])
    update_current_module('health_check')

    # Add Entry into batch Table
    batch_id = src_load.create_new_batch_entry(module_nm='ZBLOCK')
    if batch_id is None:
        error_message = 'Failed to create new batch entry'
        capture_log_message(current_logger=g.error_logger,log_message=error_message,error_name=utils.OTHER_ERRORS)
        return {'error':error_message},400
    else:
        capture_log_message(log_message='Batch entry created successfully with batch id:{}'.format(batch_id))
        g.batch_id = batch_id

    response = mainflow.do_preprocess_ap(transactions, src_id, src_status['audit_id'], src_status['client_id'])
   
    process_end_time= datetime.now(timezone.utc)
    time_taken_for_preprocessing = process_end_time - file_read_time
    if response["Result"]!="Success":
        message = "ZBLOCK Preprocessing has failed due to issues in the data. The processing was completed in {} seconds. Please verify the data.".format(time_taken_for_preprocessing)
        capture_log_message(log_message=message)
        return response.get('data',{'Process':'Failed'})
    else:
        message = "ZBLOCK Health check and Data Preprocessing has been completed in {} seconds.".format(time_taken_for_preprocessing)
        capture_log_message(log_message=message)
        df_data =  response.get('dataframe',pd.DataFrame())
        final_data = pd.DataFrame(df_data)
        # Store no. of rows present in the uploaded data for a batch in metadata table before checking for any duplicates
        data_start = final_data['POSTED_DATE'].min()
        data_end = final_data['POSTED_DATE'].max()
        src_load.add_no_of_rows_present_in_the_uploaded_data_in_batch(batch_id=g.batch_id, no_of_rows=final_data.shape[0],data_start= data_start, data_end=data_end)
        
        from hist_data.handle_hist_data import store_hist_data_in_parquet_file
        
        g.parqeut_storage_start =True
        historical_data_flag,status_message,new_data_flag = store_hist_data_in_parquet_file(batch_id=g.batch_id,df=final_data)
        
        # Check historical data storage
        if historical_data_flag:
            capture_log_message('Zblock Historical Data Storage completed successfully!')
            update_batch_upload_status_mapping(batch_id=g.batch_id, status_id=4) # Completed
            return jsonify({'process':'success','batch_id':g.batch_id,'has_new_data':bool(new_data_flag)}),200       
        else:
            error_message = f"Historical Data Storage Failed: {status_message}"
            capture_log_message(current_logger=g.error_logger,log_message=error_message,error_name=utils.OTHER_ERRORS)
            return jsonify({'error': error_message}),400
        




# @hist_bp.route("/custom_hist_gl/<int:hist_id>", methods=["GET"])
# def custom_hist_gl(hist_id):
#     response_status = check_license_validation()
#     is_date_valid = check_expiry_date()
#     g.user_id = 1
#     g.hist_id = hist_id
#     # Historical Data flow Does not Have Any audit ID values , since there is no concept of 
#     # audits in historical uploads, so giving dummy values for audit id
#     g.audit_id =  int(f"{hist_id}") 
#     g.client_id = os.getenv("CLIENT_ID",1)
#     g.erp_id = os.getenv("ERP_ID",1)
#     g.module_nm = "GL"
#     g.excel_flag = None
#     if (response_status != 200) or (not is_date_valid):
#         return jsonify({'error':'Invalid License. Please contact the administrator.'}),403
    
#     # client_id,erp_id = src_load.get_client_id_and_erp_id(hist_id=g.hist_id)
#     # if client_id is not None:
#     #     g.client_id = client_id
#     #     g.erp_id = erp_id
#     # else:
#     #     return jsonify({"error":"Something Went Wrong. please contact admininstrator"}),404
    
#     initialise_logger(folder_path='gl',audit_id=g.hist_id,historical_flow=True)

#     capture_log_message(f"Data fetched from DB for hist id {g.hist_id} Client id :{g.client_id}, erp_id  is {g.erp_id} for module {g.module_nm}")
#     capture_log_message(f"Setting status as Ongoing - 2 for hist id {g.hist_id} and user id {g.user_id}")
#     # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=2) # Ongoing
#     capture_log_message(log_message='Starting Historical data-gl pipeline')
#     update_current_module('upload')
#     start_time = datetime.now(timezone.utc)
#     files = []
#     transactions = None
#     gl_transactions=None
#     transactions_dataframes=[]
#     gl_mapping = None
#     descr = []
#     encodings = ["utf-16","utf-8","CP1252","ISO-8859-1"]
#     input_path =os.getenv("INPUT_FILES_FOLDER_PATH_AP",None)
#     if input_path is not None:
#         input_folder_path = input_path
#         capture_log_message(f"input file path from env is {input_folder_path}")
#     else:
#         input_folder_path = src_load.get_input_file_path_for_hist_id(hist_id=g.hist_id)
#         capture_log_message(f"Input file path from DB is {input_folder_path}")
#     if input_folder_path is not None:
#         files_path  = os.path.join( os.getenv("UPLOADS","") , str(input_folder_path).lstrip('/')) 
#         g.input_folder_path_for_historical_data = files_path
#     else:
#         capture_log_message(current_logger=g.error_logger,
#                             log_message='Could not fetch input files path from client_historical_data table',
#                             error_name=utils.NO_INPUT_FILES)
#         return jsonify({'error':'Could not find input data files to read from the path'}),404
#     capture_log_message(f'Input file path  is {files_path}')
#     capture_log_message(log_message=' custom-hist-data-gl START:{}'.format(start_time))

#     try:
#         files = os.listdir(files_path)
#         capture_log_message(log_message=f"files: {files}, len(files): {len(files)}")
#         if len(files)>0:
#             gl_files = [filename for filename in files if filename.lower().startswith("gl_")]
#             input_descr = 'Data Upload Failed.GL Input files not found.'
#             map_file = [filename for filename in files if filename.lower().startswith("mapping_")]
#             map_descr = 'Data Upload Failed. GL Mapping files not found.'
#             capture_log_message(log_message='List of {} files starting with GL:{}'.format(len(gl_files),gl_files))
#             capture_log_message(log_message='List of {} files starting with GL_Mapping:{}'.format(len(map_file),map_file))
#             descr = [input_descr if len(gl_files) == 0 else map_descr if len(map_file) == 0 else None]
#             if descr and descr[0] is None:
#                 descr.remove(None)

#             for input_file in gl_files:
#                 for encoding in encodings:
#                     try:
#                         path = files_path+'/'+input_file
#                         if input_file.lower().endswith(".xlsx"):
#                             df = pd.read_excel(path, engine="openpyxl")
#                             g.excel_flag = True
#                         else:
#                             df = pd.read_csv(path,encoding=encoding)
#                         capture_log_message(log_message='{} file data shape :{}'.format(input_file,df.shape))
#                         transactions_dataframes.append(df)
#                         break
#                     except Exception as e:
#                         capture_log_message(current_logger=g.error_logger,
#                                             log_message='Failed to read the file {} with encoding:{}'.format(input_file,encoding))
#                         capture_log_message(current_logger=g.error_logger,log_message=f"Error occurred:{str(e)}")
#             for maptemp in map_file:
#                 for encoding in encodings:
#                     try:
#                         path = files_path+'/'+maptemp
#                         gl_mapping = pd.read_csv(path,encoding=encoding)
#                         break
#                     except Exception as e:
#                         capture_log_message(current_logger=g.error_logger,log_message=f"Error Occurred :{str(e)}")
#                         capture_log_message(current_logger=g.error_logger,
#                                             log_message='Failed to read the file {} with encoding:{}'.format(maptemp,encoding))               
            
#             if len(transactions_dataframes)!=0:             
#                 transactions = pd.concat(transactions_dataframes,ignore_index=True)
#                 transactions.columns = transactions.columns.astype(str).str.strip()
#                 capture_log_message(log_message='Concatenated data frame for transactions, data shape:{}'.format(transactions.shape))
#                 df_format_check = any([True for each in df.columns if len(each)>100])
#                 capture_log_message(log_message=f"custom df columns: {transactions.columns}")
#                 if df_format_check:
#                     df_format_descr = "Unable to read data file! Please upload in csv format."
#                     descr.append(df_format_descr)
                
#             end_time_for_trans_df = datetime.now(timezone.utc)
            
#             historical_data_flag, err_msg = utils.check_valid_mapping_file(gl_mapping,"mapping_csv_mand_cols.json",module="GL")

#             if not historical_data_flag:
#                 descr.append(err_msg)
#                 capture_log_message(current_logger=g.error_logger,
#                                     log_message=f"Error Occurred :{err_msg}",
#                                     error_name=utils.MAPPING_FILE_ISSUE)
            
#             if descr:
#                 end_time_for_upload=datetime.now(timezone.utc)
#                 time_taken = end_time_for_upload - start_time
#                 data = {'error':f"{descr[0]}"}
#                 capture_log_message(current_logger=g.error_logger,
#                                     start_time=start_time,end_time=end_time_for_upload,
#                                     log_message=f"{descr[0]}",time_taken=time_taken,
#                                     error_name=utils.MAPPING_FILE_ISSUE)
                
#                 process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
#                                                         date_list=[start_time]*len(descr),time_taken_list=[time_taken]*len(descr),
#                                                         description_list=descr,historical_flow=True)
                
#                 return jsonify(data)   
#         else:
#             raise Exception    
#     except Exception as e:
#         end_time_for_upload=datetime.now(timezone.utc)
#         time_taken = end_time_for_upload - start_time
#         capture_log_message(current_logger=g.error_logger,log_message='{}'.format(e))
#         data = {"error": "Something went wrong. Check the uploads directory"}
#         capture_log_message(current_logger=g.error_logger,
#                             start_time=start_time,end_time=end_time_for_upload,
#                             log_message='Data Upload Failed.{}'.format(data["error"]),
#                             time_taken=time_taken,
#                             error_name=utils.NO_INPUT_FILES) 
        
#         process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
#                                                 date_list=[start_time],time_taken_list=[time_taken],
#                                                 description_list=['Data Upload Failed'],historical_flow=True)
#         return jsonify(data)
    
    
#     file_read_time = datetime.now(timezone.utc)

#     src_status = {
#         "status":-1,
#         "module":g.module_nm,
#         "audit_id":g.audit_id ,
#         "client_id":g.client_id,
#         "filename":'gl_transaction_file.csv',
#         "message":"FILE UPLOADED",
#         "created_by":4
#     } 
#     time_taken_to_upload = file_read_time - start_time
#     if transactions is not None:
#         capture_log_message(log_message=' Time taken to read GL files:{}'.format(time_taken_to_upload))
#         capture_log_message(current_logger=g.stage_logger,log_message=' Data Uploaded Successfully for invoice files',
#                             start_time=start_time,end_time=end_time_for_trans_df,
#                             data_shape=transactions.shape,time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
        
#         # date_list = [start_time]
#         # volume_list = [transactions.shape]
#         # time_taken_list = [time_taken_to_upload]
#         # description_list = ['GL data uploaded']
#         # process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=True,date_list=date_list,
#         #                                     volume_list=volume_list,time_taken_list=time_taken_list,
#         #                                     description_list=description_list,historical_flow=True)    
#         g.hist_upload_status = True
#         time_taken_to_upload = file_read_time - start_time
#         msg_success = "Data successfully uploaded for GL processing. Number of records uploaded for GL: {}".format(transactions.shape[0])
#         capture_log_message(log_message=msg_success)
#     else:
        
#         time_taken_to_upload = file_read_time - start_time
#         data = {"error": "Something went wrong. Check the uploads directory"}
#         capture_log_message(current_logger=g.error_logger,
#                             log_message='{}'.format(data["error"]),
#                             start_time=start_time,end_time=file_read_time,
#                             time_taken=time_taken_to_upload,error_name=utils.NO_INPUT_FILES) 
        
#         process_data_for_sending_internal_mail(subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
#                                                date_list=[start_time],time_taken_list=[time_taken_to_upload],
#                                                description_list=['Data Upload Failed'],historical_flow=True)
#         return jsonify(data)  
        
#     src_id = utils.create_job_query(src_status)
#     capture_log_message(log_message='Data loaded into SRC_STATUS,Source id:{}'.format(src_id))
#     # mappings = get_mappings("mappings.json")  
#     mappings_csv, redundant_mapping = utils.get_mappings_csv(gl_mapping, "db_mappings.json")
#     for col,col_value in redundant_mapping.items():
#         if col_value in transactions.columns:
#             transactions[col] = transactions[col_value]
#     mappinggroup_id = utils.set_mappings(src_status['module'], mappings_csv,src_status['client_id'])
#     configs = utils.get_config("configs.json")
#     utils.set_config(configs, mappinggroup_id,"GL",src_status['client_id'])
#     update_current_module('health_check')

#     # Add Entry into batch Table
#     batch_id = src_load.create_new_batch_entry()
#     if batch_id is None:
#         error_message = 'Failed to create new batch entry'
#         capture_log_message(current_logger=g.error_logger,log_message=error_message,error_name=utils.OTHER_ERRORS)
#         return {'error':error_message},400
#     else:
#         capture_log_message(log_message='Batch entry created successfully with batch id:{}'.format(batch_id))
#         g.batch_id = batch_id

#     response = mainflow.do_preprocess(mappinggroup_id,transactions,'gl_transaction_file.csv',src_id, src_status['audit_id'], src_status['client_id'])
   
#     process_end_time= datetime.now(timezone.utc)
#     time_taken_for_preprocessing = process_end_time - file_read_time
#     if response["Result"]!="Success":
#         message = "GL Preprocessing has failed due to issues in the data. The processing was completed in {} seconds. Please verify the data.".format(time_taken_for_preprocessing)
#         capture_log_message(log_message=message)
#         return response.get('data',{'Process':'Failed'})
#     else:
#         message = "GL Health check and Data Preprocessing has been completed in {} seconds.".format(time_taken_for_preprocessing)
#         capture_log_message(log_message=message)
#         df_data =  response.get('dataframe',pd.DataFrame())
#         final_data = pd.DataFrame(df_data)
#         # Store no. of rows present in the uploaded data for a batch in metadata table before checking for any duplicates
#         src_load.add_no_of_rows_present_in_the_uploaded_data_in_batch(batch_id=g.batch_id, no_of_rows=final_data.shape[0])
        
#         from hist_data.handle_hist_data import store_hist_data_in_parquet_file

#         data_upload_end_time = datetime.now(timezone.utc)
#         time_taken_for_data_storage = data_upload_end_time - process_end_time
#         g.parqeut_storage_start =True
#         historical_data_flag,status_message = store_hist_data_in_parquet_file(batch_id=g.batch_id,df=final_data)
        
#         capture_log_message("Status of storing historical data :{}".format(historical_data_flag))

#         if hasattr(g,'hist_upload_status') and hasattr(g,'hist_health_check_status'):
#             if g.hist_upload_status and g.hist_health_check_status and historical_data_flag:
#                 process_data_for_sending_internal_mail(
#                         subject='Data Upload Status',stage=utils.DATA_UPLOAD_STAGE,is_success=True,
#                         description_list=['Data upload Completed','Data Health Check Completed','Historical Data Stored Successfully'], 
#                         volume_list=[final_data.shape]*3,
#                         time_taken_list=[time_taken_to_upload, time_taken_for_preprocessing, time_taken_for_data_storage],
#                         date_list=[start_time]*3,
#                         historical_flow=True)
#             else:
#                 process_data_for_sending_internal_mail(
#                         subject='Data Upload Status', stage=utils.DATA_UPLOAD_STAGE, is_success=False,
#                         description_list=['Data Storage Failed'],
#                         volume_list=[final_data.shape],
#                         time_taken_list=[time_taken_for_data_storage],
#                         date_list=[start_time],
#                         historical_flow=True)
                
#         if historical_data_flag:
#             # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=4 ) # Completed
#             update_batch_upload_status_mapping(batch_id=g.batch_id, status_id=4) # Completed
#             src_load.update_end_time_for_batch_in_batch_metadata_table(batch_id=g.batch_id)  
#             return jsonify({'process':'success','batch_id':g.batch_id}),200       
#         else:
#             return jsonify({'error':status_message}),400
