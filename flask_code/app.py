# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 12:46:35 2023

@author: TVPC00022
"""

from dotenv import load_dotenv
load_dotenv()
from flask import Flask, g
from code1 import mainflow
from flask import jsonify
from flask import request
from code1 import src_load 
import os
from code1.logger import add_data_to_external_api_call, initialise_logger, capture_log_message, process_data_for_sending_external_mail, process_data_for_sending_internal_mail, update_current_module, update_total_time_taken_for_audit
from code1.logger import logger as root_logger
from flask_mailman import Mail
from datetime import datetime, timezone
from local_database import check_license_validation, external_api_call_to_send_suceess_data, get_database_credentials, update_batch_upload_status_mapping, update_endtime_for_current_run,check_expiry_date, update_historical_data_workflow_status
import utils
from multiprocessing import Process
from email_service import get_email_credentials, send_internal_stage_email, send_client_process_mail
from email_utils import normalize_duration_to_seconds,handle_stage1_pipeline_response,handle_stage2_pipeline_response,handle_stage3_pipeline_response
from utils import check_valid_mapping_file,create_job_query,get_config,get_mappings,get_mappings_csv,set_config,set_mappings
from hist_data.hist_data_flow import hist_bp

# from apscheduler.schedulers.background import BackgroundScheduler

from databases.sharding_tables import ShardingTables
from pipeline_data import PipelineData
import traceback
from mysql.connector import Error as MySQLError

from extensions import db, bcrypt, mysql_uri
from invoice_verification.api import invoice_blueprint
from sqlalchemy import create_engine
import requests


DEPLOYMENT_TYPE = os.getenv("DEPLOYMENT")

app = Flask(__name__)
app.register_blueprint(hist_bp)
app.register_blueprint(invoice_blueprint)

# Register SAP 3-Stage Pipeline Blueprint for reading raw sap data
from sap_data_pipeline.api import sap_pipeline_bp
app.register_blueprint(sap_pipeline_bp)

# credentials = get_database_credentials()
# DB_USERNAME = credentials["username"]
# DB_PASSWORD = credentials["password"]
# mysql_uri = "mysql+pymysql://{0}:{1}@{2}/{3}".format(
#    DB_USERNAME, DB_PASSWORD, os.getenv('DB_HOST'), os.getenv('DB_NAME')
# )
app.config['SQLALCHEMY_DATABASE_URI'] = mysql_uri
# engine = create_engine(mysql_uri)


# Initialize extensions with the app
db.init_app(app)
bcrypt.init_app(app)




app.secret_key = "secret key"
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
email_credentials = get_email_credentials()
app.config['MAIL_USERNAME'] = email_credentials.get("username")
app.config['MAIL_PASSWORD'] = email_credentials.get("password")
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_FROM')
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_DRIVER'] = os.getenv('MAIL_DRIVER')
app.config['MAIL_PORT'] = os.getenv('MAIL_PORT')
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL']= False

mail = Mail(app)

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)

# scheduler = BackgroundScheduler()
# scheduler.start()


def delete_input_data_files_for_hist_data_upload():
    # Dont Delete files for now
    pass
            
@app.errorhandler(Exception)
def handle_exception(error):

    if request.endpoint in ['hist_data.custom_hist_ap','hist_data.custom_hist_gl']:
            delete_input_data_files_for_hist_data_upload()
            if hasattr(g,'hist_id'):
                pass
                # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=5 ) # Failed
                # update_batch_upload_status_mapping(batch_id=g.batch_id,status_id=5) # Failed
            if hasattr(g,"error_logger"):
                if isinstance(error,MySQLError):
                    capture_log_message(current_logger=g.error_logger,
                                log_message= str(traceback.format_exc()),
                                error_name=utils.DB_CONNECTION_ISSUE)
                else:
                    g.error_logger.debug("Unhandled exception", exc_info=(type(error), error, error.__traceback__))    
                
            else:
                from code1.logger import logger
                logger.critical('*'*50)
                logger.critical("Unhandled exception", exc_info=(type(error), error, error.__traceback__))
            return  'Internal Error.Please contact the administrator',500
    else:
        if hasattr(g,"audit_id"):
            pass
            # update_audit_status(g.audit_id,5) # Mark Status as Failed
        if hasattr(g,"error_logger"):
            if isinstance(error,MySQLError):
                capture_log_message(current_logger=g.error_logger,
                            log_message= str(traceback.format_exc()), error_name=utils.DB_CONNECTION_ISSUE)
            else:
                g.error_logger.debug("Unhandled exception", exc_info=(type(error), error, error.__traceback__))
        else:
            from code1.logger import logger
            logger.critical('*'*50)
            logger.critical("Unhandled exception", exc_info=(type(error), error, error.__traceback__))
        
        return  'Internal Error.Please contact the administrator',500

@app.teardown_request
def teardown_request(exception):
    # This function runs after each request, regardless of an exception
    # Values in g are automatically cleaned up here, but you can still perform other teardown tasks
    pass
    
@app.after_request
def after_request(response):
    if request.endpoint =='invoice.invoice_analysis':
        # batch_id = request.view_args.get('batch_id') if request.view_args and 'batch_id' in request.view_args else None
        end_time = datetime.now(timezone.utc)
        if response.status_code == 200:
            # add_iv_data_to_external_api_call(key='run',json_value={'type_id':1})
            # add_iv_data_to_external_api_call(key='run',json_value={'call_id':3})
            # add_iv_data_to_external_api_call(key='run',json_value={'started_at': g.start_time.strftime("%Y-%m-%d %H:%M:%S")})
            # add_iv_data_to_external_api_call(key='run',json_value={'completed_at':end_time.strftime("%Y-%m-%d %H:%M:%S")})
            # add_iv_data_to_external_api_call(key='log',json_value={'volume':g.inv_volume})
            # add_iv_data_to_external_api_call(key='log',json_value={'date':end_time.strftime("%Y-%m-%d %H:%M:%S")})
            description = f" process completed"
            # add_iv_data_to_external_api_call(key='log',json_value={'description':description})
            # external_api_call_to_send_success_data()
        else:
            return response

    if request.endpoint == 'vendor_master':
         # batch_id = request.view_args.get('batch_id') if request.view_args and 'batch_id' in request.view_args else None
        end_time = datetime.now(timezone.utc)
        if response.status_code == 200:
            # add_iv_data_to_external_api_call(key='run',json_value={'type_id':1})
            # add_iv_data_to_external_api_call(key='run',json_value={'call_id':3})
            # add_iv_data_to_external_api_call(key='run',json_value={'started_at': g.start_time.strftime("%Y-%m-%d %H:%M:%S")})
            # add_iv_data_to_external_api_call(key='run',json_value={'completed_at':end_time.strftime("%Y-%m-%d %H:%M:%S")})
            # add_iv_data_to_external_api_call(key='log',json_value={'volume':g.inv_volume})
            # add_iv_data_to_external_api_call(key='log',json_value={'date':end_time.strftime("%Y-%m-%d %H:%M:%S")})
            # description = f" process completed"
            # add_iv_data_to_external_api_call(key='log',json_value={'description':description})
            # external_api_call_to_send_success_data()
            pass
        else:
            # process_data_for_sending_internal_mail(subject='Vendor Master Status',stage=utils.VENDOR_MASTER,is_success=False,
            #                                     description_list=['Internal Error Occured, Please Contact Administrator'])
            return response
        
    if request.endpoint in ['custom_pipeline_gl','custom_pipeline_ap','hist_data.custom_hist_ap',
                            'hist_data.custom_hist_gl','hist_data.custom_data_read_zblock', 'custom_zblock_data_ingestion']:
        
        # Delete temp tables and views
        obj = ShardingTables()
        if request.endpoint == "custom_pipeline_ap":
            obj.drop_views_for_each_audit(audit_id=g.audit_id,workflow='ap')
            pipe_clear_obj = PipelineData()
            pipe_clear_obj.clear_data(f'audit_{g.audit_id}')

        if request.endpoint == "custom_zblock_data_ingestion":
            pipe_clear_obj = PipelineData()
            pipe_clear_obj.clear_data(f'zblock_audit_{g.audit_id}')

        if request.endpoint == "custom_pipeline_gl":
             obj.drop_views_for_each_audit(audit_id=g.audit_id,workflow='gl')
            
        if request.endpoint in ['hist_data.custom_hist_ap','hist_data.custom_data_read_zblock']:
            obj.drop_temp_tables_for_each_audit(workflow='ap')
            delete_input_data_files_for_hist_data_upload()
            if response.status_code!=200 and response.status_code!=400:
                if hasattr(g,'hist_id') and hasattr(g,'batch_id'):
                    # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=5 ) # Failed
                    update_batch_upload_status_mapping(batch_id=g.batch_id,status_id=5) # Failed
                    # trigger error mail
                    # process_data_for_sending_internal_mail(subject='Data Storage Status',stage=utils.DATA_UPLOAD_STAGE,is_success=False,
                    #                             description_list=['Internal Error Occured, Please Contact Administrator'], historical_flow=True)        
            else:
                if hasattr(g,'batch_id') and hasattr(g,'parqeut_storage_start'):
                    if not g.parqeut_storage_start:
                        # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=5 ) # Failed
                        update_batch_upload_status_mapping(batch_id=g.batch_id,status_id=5) #failed   
            return response
        
        if request.endpoint == 'hist_data.custom_hist_gl':
            obj.drop_temp_tables_for_each_audit(workflow='gl')
            delete_input_data_files_for_hist_data_upload()
            if response.status_code!=200 and response.status_code!=400:
                if hasattr(g,'hist_id') and hasattr(g,'batch_id'):
                    # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=5 ) # Failed
                    update_batch_upload_status_mapping(batch_id=g.batch_id,status_id=5) # Failed
                    # trigger error mail
                    # process_data_for_sending_internal_mail(subject='Data Storage Status',stage=utils.DATA_INGESTION_STAGE,is_success=False,
                    #                             description_list=['Internal Error Occured, Please Contact Administrator'], historical_flow=True)        
            else:
                if hasattr(g,'batch_id') and hasattr(g,'parqeut_storage_start'):
                    if not g.parqeut_storage_start:
                        # update_historical_data_workflow_status(hist_data_id=g.hist_id,created_by=g.user_id,status_id=5 ) # Failed
                        update_batch_upload_status_mapping(batch_id=g.batch_id,status_id=5) #failed
            return response
            
        if response.status_code == 200:
            end_time = datetime.now(timezone.utc)
            update_endtime_for_current_run(end_time)
            total_time_taken = end_time - g.start_time
            total_seconds = total_time_taken.total_seconds()
            mm_ss_float = total_seconds // 60 + (total_seconds % 60) / 100
            mm_ss_float = round(mm_ss_float,2)
            capture_log_message(f'Total time taken:{mm_ss_float}')
            update_total_time_taken_for_audit(mm_ss_float)
            is_success = True  if g.final_status is not None else False
            # process_data_for_sending_external_mail(is_success=is_success,pipeline_stages=["DATA_UPLOAD","DATA_HEALTH_CHECK_STATUS","DATA_INGESTION","DATA_SCORING"],)
            if (g.final_status) and (g.current_module==4):
                # update_audit_status(g.audit_id,4) # Completed Successfully
                if '_ap' in request.endpoint:
                    add_data_to_external_api_call(key='run',json_value={'call_id':g.pipeline_id})    
                if '_gl' in request.endpoint:
                    add_data_to_external_api_call(key='run',json_value={'call_id':g.pipeline_id})
                add_data_to_external_api_call(key='run',json_value={'type_id':1})
                add_data_to_external_api_call(key='run',json_value={'completed_at':end_time.strftime("%Y-%m-%d %H:%M:%S")})
                add_data_to_external_api_call(key='log',json_value={'date':end_time.strftime("%Y-%m-%d %H:%M:%S")})
                description = g.CLIENT_NAME + " " + g.AUDIT_NAME + " " + str(g.pipeline_name).upper() +' Pipeline Completed'
                add_data_to_external_api_call(key='log',json_value={'description':description})
                external_api_call_to_send_suceess_data()
                
            else:
                if (g.current_module!=1):
                    pass
                    # update_audit_status(g.audit_id,5) # Failed
                g.general_logger.debug("Final status is not True, hence not making external api call to store values")
        else:
            pass
            # update_audit_status(g.audit_id,5)  # Failed
            # process_data_for_sending_internal_mail(subject='Status',stage=utils.DATA_INGESTION_STAGE,is_success=False,
            #                                     description_list=['Error Occurred, Please contact Administrator'])
    return response


#crete a route to test the flask app
@app.route("/test", methods=["GET"])
def test():
    return jsonify("Hello World")

@app.route("/testDB", methods=["GET"])
def testdb():
    test = src_load.test_db_connection()
    return jsonify(test)

               
@app.route("/fast_lang_detect_test", methods=["GET"])
def fast_lang_detect_test():
    from fast_langdetect import detect
    try:
        sample_text = "Bonjour tout le monde"
        detected_language = detect(sample_text, model="full")
        return jsonify({"detected_language": detected_language}),200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/paddle_ocr_test", methods=["GET"])
def paddle_ocr_test():
    from invoice_verification.Parameters.utils import find_sources_dir
    from invoice_verification.invoice_extraction.paddle_models import chinese_model
    try:
        import tempfile
        source_path = find_sources_dir()
        print("Sample Image Source Path:", source_path)
        image_path = os.path.join(str(source_path),'chinese_test_file.pdf')
        from pdf2image import convert_from_path
        images = convert_from_path(image_path, first_page=1, last_page=1)
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_image_file:
            temp_image_path = temp_image_file.name
            images[0].save(temp_image_path, 'PNG')
        
        result = chinese_model.predict(temp_image_path)
        print("PaddleOCR Process:", True if result else False)
        print("PaddleOCR Result:",type(result))
        if result:
            return jsonify({"message":"PaddleOCR Loading and Inference Test Successful","success": True}),200
        else:
            return jsonify({"message":"PaddleOCR Inference Failed","success": False}),500
    
    except Exception as e:
        return jsonify({"message":'Failed to load PaddleOCR model or run inference',
                        "error": str(e)}), 500

@app.route("/fastapi_app_test", methods=["GET"])
def fastapi_app_test():
    try:
        env_path = os.getenv('LLAMA_API_BASE_URL','None')
        if env_path =='None':
            return jsonify({"message":'NO Fast API URI in env files','env URI': 'LLAMA_API_BASE_URL'}),500
        api_url = os.path.join(env_path)
        response = requests.get(api_url, timeout=30)
        if response.status_code == 200:
            return jsonify({"message":"FastAPI App Health Check Successful",
                            "response": response.json()}),200
        else:
            return jsonify({"message":"FastAPI App Health Check FAILED",
                            "status_code": response.status_code,
                            "response": response.text}),500
    except Exception as e:
        return jsonify({"message":"FastAPI App Health Check FAILED",
                        "error": str(e)}),500

@app.route("/llama_status_test", methods=["GET"])
def llama_status_test():
    try:
        env_path = os.getenv('LLAMA_API_BASE_URL','None')
        if env_path =='None':
            return jsonify({"message":'NO Llama API URI in env files','env URI': 'LLAMA_API_BASE_URL'}),500
        # api_url =  os.path.join(env_path, "llama3-status")
        api_url = f"{env_path.rstrip('/')}/llama3-status"
        response = requests.get(api_url, timeout=30)
        if response.status_code == 200:
            return jsonify({"message":"Llama API Health Check Successful",
                            "response": response.json()}),200
        else:
            return jsonify({"Llama API Health Check Successful": False,
                            "status_code": response.status_code,
                            "response": response.text}),500
    except Exception as e:
        return jsonify({"Llama API Health Check Successful": False,
                        "error": str(e)}),500
 
@app.route('/test-send-email', methods=['GET'])
def test_send_email():
    """
    Simple test endpoint for email functionality
    
    Recipient is taken from TEST_EMAIL_RECIPIENT env variable or query parameter.
    Subject and body are hardcoded.
    
    Example: /test/send-email or /test/send-email?recipient=test@example.com
    """
    try:
        # Get recipient from query param or use default from ENV
        recipient =  os.getenv('DEFAULT_RECIPIENTS')
        subject = "Test Email"
        body = "This is a test email from the application"
        
        if not recipient:
            return jsonify({"error": "Recipient email is required. Set TEST_EMAIL_RECIPIENT env var or pass ?recipient=email"}), 400
        from email_service import send_email
        # Using the existing send_email function from email_service
        success, message = send_email(
            subject=subject,
            body=body,
            ishtml=False,
            recipients=[recipient]
        )
        
        if success:
            return jsonify({
                "status": "success",
                "message": f"Email sent successfully to {recipient}",
                "details": message
            }), 200
        else:
            return jsonify({
                "status": "failed",
                "message": "Failed to send email",
                "details": message
            }), 500
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500



def get_folder_path_for_client(client_id):
    try:
        with src_load.connect_to_database() as connection:
            with connection.cursor() as cursor:
                query = f""" select client_path from client_data_storage_mapping
                             where client_id = {client_id}"""
                cursor.execute(query)
                results = cursor.fetchall()
                if len(results)>0:
                    return results[0][0]
    except Exception as e:
        if hasattr(g,'error_logger'):
            g.error_logger.debug(str(e))
        return None


            



def make_api_calls(url, request_type, payload_data=None):
    """
    This function makes GET / POST request and returns the Response
    """
    try:
        with app.app_context():
            if request_type == 'GET':
                response = requests.get(url, verify=False)
            elif request_type == 'POST':
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, json=payload_data, headers=headers, verify=False)
            else:
                return None
            
            return response
    except Exception as e:
        capture_log_message(current_logger=g.error_logger, log_message=f'{e}')
        return None
    


@app.route('/ap-ingestion-and-scoring-flow/<int:batch_id>', methods=['GET'])
def ap_ingestion_and_scoring_flow(batch_id):
    if request.method == 'GET':
        from AP_Module.data_ingestion_and_scoring_flow import perform_ap_module_ingestion_and_scoring_flow
        return perform_ap_module_ingestion_and_scoring_flow(batch_id)
    else:
        return jsonify({"error": "Invalid request method"}), 405


def get_incoming_folder_path(run_timestamp=None):
    """
    Get the incoming folder path for SAP files.
    
    Args:
        run_timestamp: Optional datetime object in UTC
                      If provided, uses this timestamp instead of current time.
                      This ensures both AP and ZBLOCK pipelines read from the same
                      folder when scheduled for the same time.
    
    Returns:
        Path to incoming folder
    """
    base_path = os.getenv("UPLOADS", '/app/uploads')
    master_folder = os.getenv("MASTER_FOLDER", "dow_transformation")
    stfp_input_folder = 'sftp_batches'
    
    # Use provided timestamp or calculate from current time
    if run_timestamp is None:
        timestamp_folder_path = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_00")
    else:
        # run_timestamp is a datetime object, convert to folder format
        timestamp_folder_path = run_timestamp.strftime("%Y_%m_%d_%H_00")
    
    incoming_folder_path = os.path.join(base_path, master_folder, stfp_input_folder, timestamp_folder_path, "processed")
    # Normalize path to use consistent separators
    incoming_folder_path = os.path.normpath(incoming_folder_path)
    return incoming_folder_path

    

# Entry point for Z block Pipeline
# Data Read --> Ingestion --> Invoice Analysis
@app.route("/custom-zblock-pipeline/<int:batch_id>", methods=['GET'])
@app.route("/custom-zblock-pipeline/<int:batch_id>/<run_timestamp>", methods=['GET'])
def custom_zblock_pipeline(batch_id, run_timestamp=None):

    """
    This function is the entry point for Z block Pipeline
    Data Read --> Ingestion --> Invoice Analysis

    Steps:
    1. Read SAP raw data for Z block
    2. Ingest the data, perform preprocessing and health check and store data in parquet format
    3. Perform invoice analysis / Invoice Verification flow
    """
    pipeline_mode = "ZBLOCK"
    root_logger.info(f"Starting ZBLOCK Pipeline for Batch ID: {batch_id}")
    if request.method=='GET':
        try:
            # Step 1 : Call SAP data pipeline for Z Block

            # batch_id = src_load.create_new_batch_entry(module_nm=pipeline_mode)
            if batch_id is None:
                return jsonify({"message": "Failed to create new batch entry"}), 500

            # Use provided run_timestamp or calculate from current time
            if run_timestamp:
                # Convert URL format (YYYY_MM_DD_HH_MM) back to datetime
                try:
                    run_dt = datetime.strptime(run_timestamp, "%Y_%m_%d_%H_%M")
                    incoming_folder_path = get_incoming_folder_path(run_dt)
                except:
                    incoming_folder_path = get_incoming_folder_path()
            else:
                incoming_folder_path = get_incoming_folder_path()
            request_type = 'POST'
            request_url = f"{os.getenv('APP_URL')}/sap-raw-data-pipeline"
            request_payload_data = {"pipeline_mode":pipeline_mode, "incoming_folder": incoming_folder_path,"batch_id":batch_id}

            root_logger.info(f"ZBLOCK Pipeline - Step 1: Calling SAP Raw Data Pipeline API at {request_url} with payload: {request_payload_data}")
            
            root_logger.info(f"Incoming folder path for ZBLOCK Pipeline: {incoming_folder_path}")

            # Mark pipeline as RUNNING
            src_load.update_status_in_pipeline_run_table(
                batch_id=batch_id,
                pipeline=pipeline_mode,
                current_status=utils.RUNNING_STATUS,
                status_message="Pipeline started execution"
            )

            root_logger.info(f"ZBLOCK Pipeline - Batch ID {batch_id} marked as RUNNING in pipeline run table.")


            # Insert record into Pipeline run table
            # res = src_load.insert_new_record_in_pipeline_run_table(batch_id=batch_id,schedule_id=schedule_id,
            # current_step=utils.ZBLOCK_DATA_READ_STAGE,pipeline=pipeline_mode,current_status=utils.RUNNING_STATUS)

            res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                        current_step=utils.ZBLOCK_DATA_READ_STAGE,
                                                                        pipeline=pipeline_mode)
            
            response = make_api_calls(request_url, request_type, request_payload_data)
              # Use special handler for Stage 1 responses (handles all 6 return cases)
            # stage1_result = handle_stage1_pipeline_response(response, pipeline_mode="ZBLOCK")
            root_logger.info(f"ZBLOCK Pipeline - Step 1: Calling SAP Raw Data Pipeline API completed. Response received.")
            root_logger.info(f"ZBLOCK Pipeline - Step 1: Response Status Code: {response.status_code if response else 'No Response'}")
            root_logger.info(f"ZBLOCK Pipeline - Step 1: Response Content: {response.text if response else 'No Response'}")
            root_logger.info(f"ZBLOCK Pipeline - Step 1: Rwaw response object: {response}")

            

            api_stage1_result = {'status':'success','stage':'SAP Raw Data Read',
                         'total_duration':0,'description':"",'total_attachments':0,
                         'total_sap_files':0,'batch_id':batch_id,'metrics':{}}
          
            # root_logger.info(f"ZBLOCK Pipeline - Step 1: Stage 1 Result after handling response: {stage1_result}")
            # if stage1_result["status"] == "success":
            if response and response.status_code == 200:
                
                response_data = response.json()
                root_logger.info(f"ZBLOCK Pipeline - Step 1: ACTUAL API RESPONSE: {response_data}")
                api_stage1_result['status'] = response_data.get('final_status','Minor Error')
                api_stage1_result['total_duration'] = response_data.get('total_time_seconds',0)
                api_stage1_result['description'] = response_data.get('message','')
                api_stage1_result['total_attachments'] = response_data.get('stage1_result',{}).get('attachments',0)
                api_stage1_result['total_sap_files'] = response_data.get('stage1_result',{}).get('sap_files',0)


                email_obj = send_internal_stage_email(run_timestamp=run_timestamp,
                    handler_result=api_stage1_result,
                    batch_id=batch_id,
                    stage_num=1,
                    pipeline_mode="ZBLOCK",
                    status="success"
                )

                root_logger.info(f"ZBLOCK Pipeline - Step 1: Notification email {email_obj}")
                
                # Update the current step in Pipeline run table

                res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                        current_step=utils.ZBLOCK_DATA_INGESTION_STAGE,
                                                                        pipeline=pipeline_mode)
                root_logger.info(f"ZBLOCK Pipeline - Step 1: Updated current step to DATA INGESTION in pipeline run table for Batch ID {batch_id}.")
                # Step 2: Call Data Preprocessing and health Check pipeline to store raw SAP data in Parquet Format
                # CAPTURE Stage 2 timing (Stage 2 doesn't return duration)
                stage2_start_time = datetime.now(timezone.utc)
                
                request_type = 'GET'
                request_url = f"{os.getenv('APP_URL')}/custom_data_read_zblock/{batch_id}"
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Calling Data Ingestion API at {request_url}")
                response = make_api_calls(url=request_url, request_type=request_type)
                
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Calling Data Ingestion API completed. Response received.")
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Response Status Code: {response.status_code if response else 'No Response'}")
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Response Content: {response.text if response else 'No Response'}")
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Raw response object: {response}")

                # CALCULATE Stage 2 duration
                stage2_duration_seconds = (datetime.now(timezone.utc) - stage2_start_time).total_seconds()
                
                # Handle Stage 2 response
                # stage2_result = handle_stage2_pipeline_response(response, pipeline_mode="ZBLOCK")
                api_stage2_result = {'status':'success','stage':'Data Ingestion',
                             'duration_seconds':stage2_duration_seconds,
                             'no_of_acc_docs':0,
                             "region_wise_acc_docs_count":{},
                             "duplicate_acc_docs_count":0,
                             "duplicate_data_ref_ids":[],
                             "clearing_date_docs_skipped":0,
                             "ref_ids_with_clearing_date_skipped":[],
                             'duration_seconds':stage2_duration_seconds,
                         'batch_id':batch_id,'metrics':{}}
                root_logger.info(f"ZBLOCK Pipeline - Step 2: Stage 2 Result after handling response: {api_stage2_result}")
                if response and response.status_code == 200:
                    response_data= response.json() if response else {}
                    root_logger.info(f"ZBLOCK Pipeline - Step 2: ACTUAL API RESPONSE: {response_data}")
                    api_stage2_result['no_of_acc_docs'] = response_data.get('acc_doc_count', 0)
                    api_stage2_result['region_wise_acc_docs_count'] = response_data.get('region_wise_acc_doc_count')
                    api_stage2_result['duplicate_acc_docs_count'] = response_data.get('duplicate_data_count', 0)

                    # Defensive parsing for duplicate ref ids (string or list)
                    _dup_ids = response_data.get('duplicate_data_ref_ids', "")
                    if isinstance(_dup_ids, list):
                        api_stage2_result['duplicate_data_ref_ids'] = _dup_ids
                    elif isinstance(_dup_ids, str) and _dup_ids.strip():
                        api_stage2_result['duplicate_data_ref_ids'] = [s.strip() for s in _dup_ids.split(',') if s.strip()]
                    else:
                        api_stage2_result['duplicate_data_ref_ids'] = []

                    # Defensive parsing for ref ids skipped due to clearing date
                    _ref_ids_skipped = response_data.get('ref_ids_with_clearing_date_skipped', "")
                    if isinstance(_ref_ids_skipped, list):
                        api_stage2_result['ref_ids_with_clearing_date_skipped'] = _ref_ids_skipped
                    elif isinstance(_ref_ids_skipped, str) and _ref_ids_skipped.strip():
                        api_stage2_result['ref_ids_with_clearing_date_skipped'] = [s.strip() for s in _ref_ids_skipped.split(',') if s.strip()]
                    else:
                        api_stage2_result['ref_ids_with_clearing_date_skipped'] = []

                    api_stage2_result['clearing_date_docs_skipped'] = len(api_stage2_result['ref_ids_with_clearing_date_skipped'])

                    # Extras for future use (not used in emails now)
                    api_stage2_result['has_new_data'] = response_data.get('has_new_data', False)
                    api_stage2_result['row_count'] = response_data.get('row_count', 0)
                    
                    
                    email_obj = send_internal_stage_email(
                        run_timestamp=run_timestamp,
                        handler_result=api_stage2_result,
                        batch_id=batch_id,
                        stage_num=2,
                        pipeline_mode="ZBLOCK",
                        status="success"
                    )

                    root_logger.info(f"ZBLOCK Pipeline - Step 2: Notification email {email_obj}")
                    
                    # Update the current step in Pipeline run table
                    res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                        current_step=utils.Z_BLOCK_INVOICE_VERIFICATION_STAGE,
                                                                        pipeline=pipeline_mode)
                    
                    root_logger.info(f"ZBLOCK Pipeline - Step 2: Updated current step to INVOICE VERIFICATION in pipeline run table for Batch ID {batch_id}.")
                    # # Step 2a: Call Z block ingestion ( commenting entire flow, not needed for z block)
                    # request_url = f"{os.getenv('APP_URL')}/custom_zblock_data_ingestion/{batch_id}"
                    # request_type = 'GET'
                    # response = make_api_calls(request_type=request_type,url=request_url)

                    # Step 3: Call Invoice Verification Module
                    # CAPTURE Stage 3 timing (before API call)
                    stage3_start_time = datetime.now(timezone.utc)
                    
                    # invoice_analysis/<int:batch_id>
                    request_url = f"{os.getenv('APP_URL')}/invoice_analysis/{batch_id}"
                    request_type = 'GET'
                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Calling Invoice Verification API at {request_url}")
                    response = make_api_calls(request_type=request_type,url=request_url)

                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Calling Invoice Verification API completed. Response received.")
                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Response Status Code: {response.status_code if response else 'No Response'}")
                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Response Content: {response.text if response else 'No Response'}")
                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Raw response object: {response}")
                    stage3_end_time = datetime.now(timezone.utc)
                    stage3_total_duration = (stage3_end_time - stage3_start_time).total_seconds()
                    # Handle Stage 3 response
                    # stage3_result = handle_stage3_pipeline_response(response, pipeline_mode="ZBLOCK")
                    api_stage3_result = {
                        'status':'success',
                        'stage':'Invoice Verification',
                        'duration_seconds':stage3_total_duration,
                        'batch_id':batch_id,
                        'total_invoices_processed':0,
                        'total_invoices_failed':0,
                    }
                    root_logger.info(f"ZBLOCK Pipeline - Step 3: Stage 3 Result after handling response: {api_stage3_result}")
                    
                    if response and response.status_code == 200:
                        response_data = response.json() if response else {}
                        api_stage3_result['total_invoices_processed'] = response_data.get('total_invoices',0)
                        api_stage3_result['total_invoices_failed'] = response_data.get('failed_invoices',0)
                        email_obj = send_internal_stage_email(
                            run_timestamp=run_timestamp,
                            handler_result=api_stage3_result,
                            batch_id=batch_id,
                            stage_num=3,
                            pipeline_mode="ZBLOCK",
                            status="success"
                        )
                        root_logger.info(f"ZBLOCK Pipeline - Step 3: Notification email {email_obj}")
                        
                        # Send external email to client with all 3 stages status
                        client_email_obj = send_client_process_mail(
                            stage1_result=api_stage1_result,
                            stage2_result=api_stage2_result,
                            stage3_result=api_stage3_result,
                            pipeline_mode="ZBLOCK",
                            batch_id=batch_id,
                            status="success",
                            run_timestamp=run_timestamp
                        )
                        root_logger.info(f"ZBLOCK Pipeline - Client notification email {client_email_obj}")
                        
                        # Update the flow in Pipeline run table
                        res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                           pipeline=pipeline_mode,
                                                                           current_status=utils.SUCCESS_STATUS,
                                                                           status_message="Z block pipeline completed successfully"
                                                                           )
                        return jsonify({"message": "Z block pipeline completed successfully"}), 200

                    else:
                        email_obj = send_internal_stage_email(
                            run_timestamp=run_timestamp,
                            handler_result=api_stage3_result,
                            batch_id=batch_id,
                            stage_num=3,
                            pipeline_mode="ZBLOCK",
                            status="failure"
                        )
                        root_logger.info(f"ZBLOCK Pipeline - Step 3: Notification email {email_obj}")
                        
                        # Send external email to client with all 3 stages status (failure)
                        client_email_obj = send_client_process_mail(
                            stage1_result=api_stage1_result,
                            stage2_result=api_stage2_result,
                            stage3_result=api_stage3_result,
                            pipeline_mode="ZBLOCK",
                            batch_id=batch_id,
                            status="failure",
                            run_timestamp=run_timestamp
                        )
                        root_logger.info(f"ZBLOCK Pipeline - Client notification email (failure) {client_email_obj}")
                        
                         # Update the flow in Pipeline run table
                        res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                           pipeline=pipeline_mode,
                                                                           current_status=utils.FAILED_STATUS,
                                                                           status_message="Invoice Verification Pipeline failed"
                                                                           )
                        return jsonify({"message": "Invoice Verification Pipeline failed"}), 500
                else:
                    api_stage2_result['status'] = 'failure'
                    response_data = response.json() if response else {}
                    root_logger.error(f"ZBLOCK Pipeline - Step 2: API ERROR Response: {response_data}")
                    root_logger.error(f"ZBLOCK Pipeline - Step 2: Response Status Code: {response.status_code if response else 'No Response'}")
                    api_stage2_result['description'] = response.json().get('message','Data Ingestion failed') if response else 'No response from Data Ingestion API'
                    email_obj = send_internal_stage_email(
                            run_timestamp=run_timestamp,
                        handler_result=api_stage2_result,
                        batch_id=batch_id,
                        stage_num=2,
                        pipeline_mode="ZBLOCK",
                        status="failure"
                    )
                    root_logger.info(f"ZBLOCK Pipeline - Step 2: Notification email {email_obj}")

                    # Send external email to client with Stage 1 success + Stage 2 failure (no Stage 3 executed)
                    client_email_obj = send_client_process_mail(
                        stage1_result=api_stage1_result,
                        stage2_result=api_stage2_result,
                        stage3_result=None,
                        pipeline_mode="ZBLOCK",
                        batch_id=batch_id,
                        status="failure",
                        run_timestamp=run_timestamp
                    )
                    root_logger.info(f"ZBLOCK Pipeline - Client notification email (Stage 2 failure) {client_email_obj}")

                    res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                       pipeline=pipeline_mode,
                                                                       current_status=utils.FAILED_STATUS,
                                                                       status_message="Z block data preprocessing and health check failed")
                    return jsonify({"message": "Z block data preprocessing and health check failed"}), 500
            else:
                email_obj = send_internal_stage_email(
                    run_timestamp=run_timestamp,
                    handler_result=api_stage1_result,
                    batch_id=batch_id,
                    stage_num=1,
                    pipeline_mode="ZBLOCK",
                    status="failure"
                )
                root_logger.info(f"ZBLOCK Pipeline - Step 1: Notification email {email_obj}")
                
                # Send external email to client with Stage 1 failure (no Stage 2/3 executed)
                client_email_obj = send_client_process_mail(
                    stage1_result=api_stage1_result,
                    stage2_result=None,
                    stage3_result=None,
                    pipeline_mode="ZBLOCK",
                    batch_id=batch_id,
                    status="failure",
                    run_timestamp=run_timestamp
                )
                root_logger.info(f"ZBLOCK Pipeline - Client notification email (Stage 1 failure) {client_email_obj}")
                
                 # Update the flow in Pipeline run table
                res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                   pipeline=pipeline_mode,
                                                                   current_status=utils.FAILED_STATUS,
                                                                   status_message="Z block Raw SAP data Pipeline failed" )
                return jsonify({"message": "Z block Raw SAP data Pipeline failed"}), 500
            # If all steps succeed, return a success response
           
   
        except Exception as e:
            # Create minimal error result dict for email notification
            error_result = {
                "status": "error",
                "stage": "exception",
                "duration_seconds": 0,
                "batch_id": batch_id,
                "message": "Exception occurred during pipeline execution",
                "error": str(e),
                "metrics": {}
            }
            email_obj = send_internal_stage_email(
                run_timestamp=run_timestamp,
                handler_result=error_result,
                batch_id=batch_id,
                stage_num=1,
                pipeline_mode="ZBLOCK",
                status="failure"
            )
            root_logger.info(f"ZBLOCK Pipeline - EXCEPTION: Notification email {email_obj}")
            
            # Send external email to client with exception (no valid stage results)
            client_email_obj = send_client_process_mail(
                stage1_result=error_result,
                stage2_result=None,
                stage3_result=None,
                pipeline_mode="ZBLOCK",
                batch_id=batch_id,
                status="failure",run_timestamp=run_timestamp
            )
            root_logger.info(f"ZBLOCK Pipeline - Client notification email (exception) {client_email_obj}")
            
            res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                               pipeline=pipeline_mode,
                                                                current_status=utils.FAILED_STATUS,
                                                                status_message="Exception occurred while processing Z block Pipeline"
                                                                )
            logger = g.error_logger if hasattr(g,'error_logger') else None
            capture_log_message(current_logger=logger, log_message=f'{e}')
            return jsonify({"error": "Failed to process the request"}), 500
        
    else:
        root_logger.info(f"ZBLOCK Pipeline - Invalid request method: {request.method} for Batch ID: {batch_id}")
        res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                        pipeline=pipeline_mode,
                                                        current_status=utils.FAILED_STATUS,
                                                        status_message="Invalid request method for Z block pipeline")
        return jsonify({"error": "Invalid request method"}), 405


# Entry point for AP Pipeline
# Data Read --> Ingestion --> Scoring --> Duplicate invoices module
@app.route("/custom-ap-pipeline/<int:batch_id>", methods=['GET'])
@app.route("/custom-ap-pipeline/<int:batch_id>/<run_timestamp>", methods=['GET'])
def custom_ap_pipeline(batch_id, run_timestamp=None):


    try:
        root_logger.info(f"Starting AP Pipeline for Batch ID: {batch_id}")
        # Step 1 : Call SAP data pipeline for AP
        pipeline_mode = "AP"
        # batch_id = src_load.create_new_batch_entry(module_nm=pipeline_mode)
        if batch_id is None:
            return jsonify({"message": "Failed to create new batch entry"}), 500
        
        # Use provided run_timestamp or calculate from current time
        if run_timestamp:
            # Convert URL format (YYYY_MM_DD_HH_MM) back to datetime
            try:
                run_dt = datetime.strptime(run_timestamp, "%Y_%m_%d_%H_%M")
                incoming_folder_path = get_incoming_folder_path(run_dt)
            except:
                incoming_folder_path = get_incoming_folder_path()
        else:
            incoming_folder_path = get_incoming_folder_path()
        request_type = 'POST'
        request_url = f"{os.getenv('APP_URL')}/sap-raw-data-pipeline"
        request_payload_data = {"pipeline_mode":pipeline_mode, "incoming_folder": incoming_folder_path,"batch_id":batch_id} 

        root_logger.info(f"AP Pipeline - Step 1: Calling SAP Raw Data Pipeline API at {request_url} with payload: {request_payload_data}")
        root_logger.info(f"Incoming folder path for AP Pipeline: {incoming_folder_path}")


        # Mark pipeline as RUNNING
        src_load.update_status_in_pipeline_run_table(
            batch_id=batch_id,
            pipeline=pipeline_mode,
            current_status=utils.RUNNING_STATUS,
            status_message="Pipeline started execution"
        )

        root_logger.info(f"AP Pipeline - Batch ID {batch_id} marked as RUNNING in pipeline run table.")

        # # Insert record into Pipeline run table
        # res = src_load.insert_new_record_in_pipeline_run_table(batch_id=batch_id,schedule_id=schedule_id,
        # current_step=utils.AP_DATA_READ_STAGE,pipeline=pipeline_mode,current_status=utils.RUNNING_STATUS)

        # update Current step in Pipeline run table
        res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                 current_step=utils.AP_DATA_READ_STAGE,
                                                                 pipeline=pipeline_mode)

        response = make_api_calls(request_url, request_type, request_payload_data)
        root_logger.info(f"AP Pipeline - Step 1: Calling SAP Raw Data Pipeline API completed. Response received.")
        root_logger.info(f"AP Pipeline - Step 1: Response Status Code: {response.status_code if response else 'No Response'}")  
        root_logger.info(f"AP Pipeline - Step 1: Response Content: {response.text if response else 'No Response'}") 
        root_logger.info(f"AP Pipeline - Step 1: Raw response object: {response}")


        # Use special handler for Stage 1 responses (handles all 6 return cases)
        # from email_utils import handle_stage1_pipeline_response
        # stage1_result = handle_stage1_pipeline_response(response, pipeline_mode="AP")
        api_stage1_result = {'status':'success','stage':'SAP Raw Data Read',
                         'total_duration':0,'description':"",'total_attachments':0,
                         'total_sap_files':0,'batch_id':batch_id,'metrics':{}}
        root_logger.info(f"AP Pipeline - Step 1: Stage 1 Result after handling response: {api_stage1_result}")
        # if stage1_result["status"] == "success":
        if response and response.status_code == 200:
            response_data = response.json()
            api_stage1_result['status'] = response_data.get('final_status','Minor Error')
            api_stage1_result['total_duration'] = response_data.get('total_time_seconds',0)
            api_stage1_result['description'] = response_data.get('message','')
            api_stage1_result['total_attachments'] = response_data.get('stage1_result',{}).get('attachments',0)
            api_stage1_result['total_sap_files'] = response_data.get('stage1_result',{}).get('sap_files',0)


            
            email_obj = send_internal_stage_email(
                handler_result=api_stage1_result,
                batch_id=batch_id,
                stage_num=1,
                pipeline_mode="AP",
                status="success",
                run_timestamp=run_timestamp
            )
            root_logger.info(f"AP Pipeline - Step 1: Notification email {email_obj}")
            root_logger.info(f"AP Pipeline - Step 1: Notification email sent for Batch ID {batch_id}")

            # update Current step in Pipeline run table
            res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                            current_step=utils.DATA_INGESTION_STAGE,
                                                                            pipeline=pipeline_mode)
            
            root_logger.info(f"AP Pipeline - Step 1: Updated current step to DATA INGESTION in pipeline run table for Batch ID {batch_id}.")
            
            
            # Step 2: Call Data Preprocessing and health Check pipeline to store raw SAP data in Parquet Format
            # CAPTURE Stage 2 timing (Stage 2 doesn't return duration)
            stage2_start_time = datetime.now(timezone.utc)
            
            request_type = 'GET'
            request_url = f"{os.getenv('APP_URL')}/custom_hist_ap/{batch_id}"

            root_logger.info(f"AP Pipeline - Step 2: Calling Data Ingestion API at {request_url}")

            response = make_api_calls(url=request_url, request_type=request_type)
            
            root_logger.info(f"AP Pipeline - Step 2: Calling Data Ingestion API completed. Response received.")
            root_logger.info(f"AP Pipeline - Step 2: Response Status Code: {response.status_code if response else 'No Response'}")
            root_logger.info(f"AP Pipeline - Step 2: Response Content: {response.text   if response else 'No Response'}")
            root_logger.info(f"AP Pipeline - Step 2: Raw response object: {response }")

            # CALCULATE Stage 2 duration
            stage2_duration_seconds = (datetime.now(timezone.utc) - stage2_start_time).total_seconds()
            
            # Handle Stage 2 response
            # stage2_result = handle_stage2_pipeline_response(response, pipeline_mode="AP")
            api_stage2_result = {'status':'success','stage':'Data Ingestion',
                             'duration_seconds':stage2_duration_seconds,
                             'no_of_acc_docs':0,
                             "region_wise_acc_docs_count":{},
                                'duration_seconds':stage2_duration_seconds,
                         'batch_id':batch_id,'metrics':{}}
            root_logger.info(f"AP Pipeline - Step 2: Stage 2 Result after handling response: {api_stage2_result}")

            
            if response and response.status_code == 200:
                response_data= response.json() if response else {}
                root_logger.info(f"AP Pipeline - Step 2: ACTUAL API RESPONSE: {response_data}")
                api_stage2_result['no_of_acc_docs'] = response_data.get('acc_doc_count',0)
                api_stage2_result['region_wise_acc_docs_count'] = response_data.get('region_wise_acc_doc_count')
                api_stage2_result['no_of_acc_docs'] = response_data.get('acc_doc_count',0)
                api_stage2_result['region_wise_acc_docs_count'] = response_data.get('region_wise_acc_doc_count')
                api_stage2_result['duplicate_acc_docs_count'] = response_data.get('duplicate_data_count',0)
                api_stage2_result['duplicate_data_ref_ids'] = response_data.get('duplicate_data_ref_ids',"")
                if api_stage2_result['duplicate_data_ref_ids']!="":
                    api_stage2_result['duplicate_data_ref_ids'] = api_stage2_result['duplicate_data_ref_ids'].split(',')
                api_stage2_result['ref_ids_with_clearing_date_skipped'] = response_data.get('ref_ids_with_clearing_date_skipped',"")
                if api_stage2_result['ref_ids_with_clearing_date_skipped']!="":
                    api_stage2_result['ref_ids_with_clearing_date_skipped'] = api_stage2_result['ref_ids_with_clearing_date_skipped'].split(',')
                api_stage2_result['clearing_date_docs_skipped'] = len(api_stage2_result['ref_ids_with_clearing_date_skipped'] ) if api_stage2_result['ref_ids_with_clearing_date_skipped']!="" else 0
                
                email_obj = send_internal_stage_email(
                    run_timestamp=run_timestamp,
                    handler_result=api_stage2_result,
                    batch_id=batch_id,
                    stage_num=2,
                    pipeline_mode="AP",
                    status="success"
                )

                root_logger.info(f"AP Pipeline - Step 2: Notification email {email_obj}")
                
                # update Current step in Pipeline run table
                res = src_load.update_current_step_in_pipeline_run_table(batch_id=batch_id,
                                                                            current_step=utils.DATA_SCORING_STAGE,
                                                                            pipeline=pipeline_mode)
                
                root_logger.info(f"AP Pipeline - Step 2: Updated current step to DATA SCORING in pipeline run table for Batch ID {batch_id}.")
                # Step 3: Call AP Ingestion and Scoring Module
                # CAPTURE Stage 3 timing (before API call)
                stage3_start_time = datetime.now(timezone.utc)

                request_url = f"{os.getenv('APP_URL')}/ap-ingestion-and-scoring-flow/{batch_id}"
                request_type = 'GET'

                root_logger.info(f"AP Pipeline - Step 3: Calling AP Ingestion and Scoring API at {request_url}")

                response = make_api_calls(request_type=request_type,url=request_url)
                root_logger.info(f"AP Pipeline - Step 3: Calling AP Ingestion and Scoring API completed. Response received.")
                root_logger.info(f"AP Pipeline - Step 3: Response Status Code: {response.status_code if response else 'No Response'}")
                root_logger.info(f"AP Pipeline - Step 3: Response Content: {response.text   if response else 'No Response'}")
                root_logger.info(f"AP Pipeline - Step 3: Raw response object: {response }")
                stage3_duration_seconds = (datetime.now(timezone.utc) - stage3_start_time).total_seconds()
                # Handle Stage 3 response
                # stage3_result = handle_stage3_pipeline_response(response, pipeline_mode="AP")
                api_stage3_result = {
                    'status':'success',
                    'stage':'AP Ingestion and Scoring',
                    'duration_seconds':stage3_duration_seconds,
                    'batch_id':batch_id,

                }
                root_logger.info(f"AP Pipeline - Step 3: Stage 3 Result after handling response: {api_stage3_result}")
                
                
                if response and response.status_code == 200:
                    email_obj = send_internal_stage_email(
                        run_timestamp=run_timestamp,
                        handler_result=api_stage3_result,
                        batch_id=batch_id,
                        stage_num=3,
                        pipeline_mode="AP",
                        status="success"
                    )
                    root_logger.info(f"AP Pipeline - Step 3: Notification email {email_obj}")
                    
                    # Send external email to client with all 3 stages status
                    client_email_obj = send_client_process_mail(
                        stage1_result=api_stage1_result,
                        stage2_result=api_stage2_result,
                        stage3_result=api_stage3_result,
                        pipeline_mode="AP",
                        batch_id=batch_id,
                        status="success",run_timestamp=run_timestamp
                    )
                    root_logger.info(f"AP Pipeline - Client notification email {client_email_obj}")
                    
                    # If all steps succeed, return a success response
                    res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                       pipeline=pipeline_mode,
                                                                       current_status=utils.SUCCESS_STATUS,
                                                                       status_message="AP Pipeline completed successfully"
                                                                       )
                    return jsonify({"message": "AP pipeline completed successfully"}), 200
                else:

                    email_obj = send_internal_stage_email(
                        run_timestamp=run_timestamp,
                        handler_result=api_stage3_result,
                        batch_id=batch_id,
                        stage_num=3,
                        pipeline_mode="AP",
                        status="failure"
                    )
                    root_logger.info(f"AP Pipeline - Step 3: Notification email {email_obj}")
                    
                    # Send external email to client with all 3 stages status (failure)
                    client_email_obj = send_client_process_mail(
                        stage1_result=api_stage1_result,
                        stage2_result=api_stage2_result,
                        stage3_result=api_stage3_result,
                        pipeline_mode="AP",
                        batch_id=batch_id,
                        status="failure",run_timestamp=run_timestamp
                    )
                    root_logger.info(f"AP Pipeline - Client notification email (failure) {client_email_obj}")
                    
                    res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                          pipeline=pipeline_mode,
                                                                          current_status=utils.FAILED_STATUS,
                                                                          status_message="AP Ingestion and Scoring Pipeline failed"
                                                                          )
                    return jsonify({"message": "AP Ingestion and Scoring Pipeline failed"}), 500
            else:
                api_stage2_result['status'] = 'failure'
                response_data = response.json() if response else {}
                api_stage2_result['description'] = response_data.get('message','AP Data Ingestion Pipeline failed')

                email_obj = send_internal_stage_email(
                    run_timestamp=run_timestamp,
                    handler_result=api_stage2_result,
                    batch_id=batch_id,
                    stage_num=2,
                    pipeline_mode="AP",
                    status="failure"
                )
                root_logger.info(f"AP Pipeline - Step 2: Notification email {email_obj}")
                
                # Send external email to client with Stage 1 success + Stage 2 failure (no Stage 3 executed)
                client_email_obj = send_client_process_mail(
                    stage1_result=api_stage1_result,
                    stage2_result=api_stage2_result,
                    stage3_result=None,
                    pipeline_mode="AP",
                    batch_id=batch_id,
                    status="failure",run_timestamp=run_timestamp
                )
                root_logger.info(f"AP Pipeline - Client notification email (Stage 2 failure) {client_email_obj}")
                
                res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                      pipeline=pipeline_mode,
                                                                      current_status=utils.FAILED_STATUS,
                                                                      status_message="AP data preprocessing and health check failed"
                                                                      )
                return jsonify({"message": "AP data preprocessing and health check failed"}), 500
        else:
            response_data = response.json() if response else {}
            api_stage1_result['status'] = 'failure'
            api_stage1_result['description'] = response_data.get('message','SAP Raw Data Pipeline failed due to unknown error')
            response_data = response.json() if response else {}
            api_stage1_result['status'] = 'failure'
            api_stage1_result['description'] = response_data.get('message','SAP Raw Data Pipeline failed due to unknown error')
            email_obj = send_internal_stage_email(
                run_timestamp=run_timestamp,
                handler_result=api_stage1_result,
                batch_id=batch_id,
                stage_num=1,
                pipeline_mode="AP",
                status="failure"
            )
            root_logger.info(f"AP Pipeline - Step 1: Notification email {email_obj}")
            
            # Send external email to client with Stage 1 failure (no Stage 2/3 executed)
            client_email_obj = send_client_process_mail(
                stage1_result=api_stage1_result,
                stage2_result=None,
                stage3_result=None,
                pipeline_mode="AP",
                batch_id=batch_id,
                status="failure",run_timestamp=run_timestamp
            )
            root_logger.info(f"AP Pipeline - Client notification email (Stage 1 failure) {client_email_obj}")
            
            res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                                  pipeline=pipeline_mode,
                                                                  current_status=utils.FAILED_STATUS,
                                                                  status_message="AP Raw SAP data Pipeline failed"
                                                                  )
            return jsonify({"message": "AP Raw SAP data Pipeline failed"}), 500

    except Exception as e:
        logger = g.error_logger if hasattr(g, 'error_logger') else None
        capture_log_message(current_logger=logger, log_message=f'{e}')
        # Create minimal error result dict for email notification
        error_result = {
            "status": "error",
            "stage": "exception",
            "duration_seconds": 0,
            "batch_id": batch_id,
            "message": "Exception occurred during pipeline execution",
            "error": str(e),
            "metrics": {}
        }
        email_obj = send_internal_stage_email(
            run_timestamp=run_timestamp,
            handler_result=error_result,
            batch_id=batch_id,
            stage_num=1,
            pipeline_mode="AP",
            status="failure"
        )
        root_logger.info(f"AP Pipeline - Step EXCEPTION: Notification email {email_obj}")
        
        # Send external email to client with exception (no valid stage results)
        client_email_obj = send_client_process_mail(
            stage1_result=error_result,
            stage2_result=None,
            stage3_result=None,
            pipeline_mode="AP",
            batch_id=batch_id,
            status="failure",run_timestamp=run_timestamp
        )
        root_logger.info(f"AP Pipeline - Client notification email (exception) {client_email_obj}")
        
        res = src_load.update_status_in_pipeline_run_table(batch_id=batch_id,
                                                              pipeline=pipeline_mode,
                                                              current_status=utils.FAILED_STATUS,
                                                              status_message="Exception occurred while processing AP E2E Pipeline"
                                                              )
        return jsonify({"error": "Failed to process the AP E2E Pipeline request"}), 500


@app.route("/pipeline/enqueue", methods=['POST'])
def pipeline_enqueue():
    """
    POST endpoint to enqueue a pipeline for execution.
    
    Request JSON:
    {
        "pipeline": "ZBLOCK" or "AP",
        "run_datetime": "2026-01-22T14:00:00+00:00"
    }
    
    Responses:
    - 200: Success - Pipeline enqueued with batch_id and run_id
    - 400: Invalid input (pipeline or run_datetime)
    - 409: Duplicate - Pipeline already enqueued for this batch_id/date combination
    - 500: Server error
    """
    try:
        # Parse JSON request
        data = request.get_json()
        root_logger.info(f"Pipeline Enqueue endpoint called")
        root_logger.info(f"Received pipeline enqueue request: {data}")
        
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400
        
        # Extract and validate required fields
        pipeline = data.get('pipeline')
        run_datetime = data.get('run_datetime')
        
        root_logger.info(f"Enqueueing pipeline: {pipeline} for run_datetime: {run_datetime}")

        if not pipeline or not run_datetime:
            return jsonify({"error": "Missing required fields: 'pipeline' and 'run_datetime'"}), 400
        
        # Call enqueue function
        result = src_load.enqueue_pipeline(pipeline, run_datetime)
        root_logger.info(f"Enqueue pipeline result: {result}")
        
        # Map result to HTTP response
        if result['success']:
            return jsonify({
                "success": True,
                "message": result['message'],
                "batch_id": result['batch_id'],
                "run_id": result['run_id']
            }), result['status_code']
        else:
            return jsonify({
                "success": False,
                "error": result['message'],
                "batch_id": result['batch_id'],
                "run_id": result['run_id']
            }), result['status_code']
            
    except Exception as e:
        root_logger.error(f"Failed to process enqueue request: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Failed to process enqueue request: {str(e)}",
            "batch_id": None,
            "run_id": None
        }), 500


def execute_pipeline_in_background(run_id, batch_id, pipeline, run_timestamp_str):
    """
    Background process function to execute pipeline asynchronously
    
    Args:
        run_id: Pipeline run ID
        batch_id: Batch ID
        pipeline: Pipeline type (AP or ZBLOCK)
        run_timestamp_str: Timestamp in URL format (YYYY_MM_DD_HH_MM)
    """
    try:
        root_logger.info(f"[Background Process] Executing {pipeline} pipeline: run_id={run_id}, batch_id={batch_id}, run_timestamp={run_timestamp_str}")
        # Build request URL based on pipeline type
        if pipeline == "AP":
            if run_timestamp_str:
                request_url = f"{os.getenv('APP_URL')}/custom-ap-pipeline/{batch_id}/{run_timestamp_str}"
            else:
                request_url = f"{os.getenv('APP_URL')}/custom-ap-pipeline/{batch_id}"
        elif pipeline == "ZBLOCK":
            if run_timestamp_str:
                request_url = f"{os.getenv('APP_URL')}/custom-zblock-pipeline/{batch_id}/{run_timestamp_str}"
            else:
                request_url = f"{os.getenv('APP_URL')}/custom-zblock-pipeline/{batch_id}"
        else:
            root_logger.info(f" Unknown pipeline type: {pipeline}")
            return
        

        root_logger.info(f"[Background Process] Built request URL: {request_url}")
        
        # Execute pipeline in background
        root_logger.info(f"[Background Process] Starting {pipeline} pipeline: run_id={run_id}, batch_id={batch_id}")
        response = make_api_calls(request_url, 'GET', None)
        
        if response and response.status_code == 200:
            root_logger.info(f" [Background Process] {pipeline} pipeline completed successfully: run_id={run_id}")
        else:
            error_msg = response.json().get('error', 'Unknown error') if response else 'No response'
            root_logger.info(f" [Background Process] {pipeline} pipeline failed: {error_msg}")
            
    except Exception as e:
        root_logger.error(f" [Background Process] Error executing {pipeline} pipeline: {str(e)}")
        root_logger.error(traceback.format_exc())


@app.route("/pipeline/process-queue", methods=['GET'])
def pipeline_process_queue():
    """
    GET endpoint to process the FIFO queue of pending pipeline runs in BACKGROUND.
    
    Logic:
    1. Check if ANY pipeline is currently RUNNING
       → If yes: Return 429 (Too Soon) - block until completion
    2. If no RUNNING: Get oldest PENDING pipeline_run record
       → If found: Start background process (returns immediately)
       → If not found: Return 200 (queue empty)
    
    Responses:
    - 202: Pipeline started in background (ACCEPTED)
    - 200: Queue is empty - no pending pipeline runs
    - 429: Pipeline already running - cannot start new execution
    - 500: Server error
    """
    try:
        root_logger.info(f"Pipeline Process Queue endpoint called")

        # CHECK 1: Is any pipeline currently RUNNING?
        running_count = src_load.check_running_pipeline_count()
        root_logger.info(f"Current running pipeline count: {running_count}")
        if running_count > 0:
            return jsonify({
                "success": False,
                "message": "Pipeline already running - cannot start new execution",
                "queue_processed": False,
                "run_id": None,
                "batch_id": None,
                "pipeline": None,
                "running_count": running_count
            }), 429  # 429 Too Soon
        
        # CHECK 2: Get next PENDING pipeline run from queue
        pending_run = src_load.get_next_pending_pipeline_run()
        
        if not pending_run:
            root_logger.info("Queue is empty - no pending pipeline runs")
            return jsonify({
                "success": True,
                "message": "Queue is empty - no pending pipeline runs",
                "queue_processed": False,
                "run_id": None,
                "batch_id": None,
                "pipeline": None
            }), 200
        
        # Extract details
        run_id = pending_run['run_id']
        batch_id = pending_run['batch_id']
        pipeline = pending_run['pipeline']
        
        root_logger.info(f"Starting background execution for pipeline: {pipeline}, run_id: {run_id}, batch_id: {batch_id}")
        # FETCH the scheduled run_date from pipeline_run table
        run_date = src_load.get_run_date_for_pipeline_run(run_id)
        root_logger.info(f"Fetched run_date for run_id {run_id}: {run_date}")
        # Convert run_date to URL format (YYYY_MM_DD_HH_MM)
        if run_date:
            run_timestamp_str = run_date.strftime("%Y_%m_%d_%H_%M")
        else:
            run_timestamp_str = None
        
        root_logger.info(f"Converted run_date to URL format: {run_timestamp_str}")

        root_logger.info(f"Spawning background process for pipeline execution")
        # ✅ START BACKGROUND PROCESS (don't wait for pipeline to complete)
        process = Process(target=execute_pipeline_in_background, 
                         args=(run_id, batch_id, pipeline, run_timestamp_str))
        process.start()
        
        # ✅ RETURN IMMEDIATELY with 202 Accepted (processing started in background)
        return jsonify({
            "success": True,
            "message": f"Pipeline {pipeline} started in background",
            "queue_processed": True,
            "run_id": run_id,
            "batch_id": batch_id,
            "pipeline": pipeline,
            "status": "PROCESSING_IN_BACKGROUND"
        }), 202  # 202 Accepted
            
    except Exception as e:
        root_logger.error(f"Failed to process queue request: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Failed to process queue request: {str(e)}",
            "queue_processed": False,
            "run_id": None,
            "batch_id": None,
            "pipeline": None
        }), 500


if __name__ == "__main__":
    # app.debug = True
    response_status = check_license_validation()
    is_date_valid = check_expiry_date()

    if (response_status == 200) and (is_date_valid):
        app.run(host="0.0.0.0",port='5005',threaded = False, processes = 10)
        # app.run(host="0.0.0.0",port='5005',threaded = True) #windows only
        # app.run(debug=True,host="0.0.0.0",port='5005',threaded = False, processes = 10,ssl_context=("server.crt", "server.key"))
    else:
        print('Invalid License. Please contact the administrator.')


















































































# # ENTRY POINT FOR AP FLOW
# @app.route("/custom-hist-data-ap/<int:hist_id>",methods=['GET'])
# def custom_pipeline(hist_id):
#     try:
#         batch_id = None
#         if request.method=='GET':
#             res = run_custom_hist_data_load_ap(hist_id)
#             if (res.status_code == 200) and res.json().get('batch_id',False):
#                 batch_id = res.json().get('batch_id')


#             if os.getenv("AP_FLOW") == 'True':
#                 if (res.status_code == 200):
#                     next_url = f"{os.getenv('APP_URL')}/custom_pipeline_ap/{batch_id}"
#                     res = requests.get(next_url)

#         return res.json() if res.status_code == 200 else jsonify({"error": "Failed to process the request"}), res.status_code
#     except Exception as e:
#         if hasattr(g,'error_logger'):
#             g.error_logger.debug(f'Error Occurred in custom_pipeline: {str(e)}')
#         return jsonify({"error": "Internal Server Error"}), 500

















# @app.route("/custom_zblock_data_ingestion/<int:batch_id>", methods = ['GET'])
# def zblock_data_ingestion(batch_id):
    
#     response_status = check_license_validation()
#     is_date_valid = check_expiry_date()
#     g.audit_id = batch_id
#     g.client_id = os.getenv("CLIENT_ID",1)
#     g.module_nm = "ZBLOCK"
    
#     if (response_status != 200) or (not is_date_valid):
#         return jsonify({'error':'Invalid License. Please contact the administrator.'}),403
#     initialise_logger('zblock',audit_id=g.audit_id)

#     capture_log_message(log_message='Starting zblock ingestion and DB storage pipeline')

#     start_time = datetime.now(timezone.utc)
    
#     capture_log_message(log_message=' custom-client-zblock START:{}'.format(start_time))
    
#     from Ingestor.zblock_ingestor import Zblock_Ingestor
#     Zblock = Zblock_Ingestor(g.audit_id, g.client_id)
#     ingestion_status = Zblock.run_job()

                
#     process_end_time= datetime.now(timezone.utc)

#     if ingestion_status:
#        message = 'ZBLOCK Data Ingestion and scoring has been completed in {} seconds.You can view the results in dashboard'.format(process_end_time-start_time)
#        response_data = [message]
#        time_taken_for_zblock_ingestion = g.zblock_ingestion_end_time - start_time
#        time_taken_for_db_storage = process_end_time - g.zblock_ingestion_end_time
 
#        capture_log_message(f"{message} Zblock Ingestion Time Taken: {time_taken_for_zblock_ingestion}, Zblock DB storage Time Taken: {time_taken_for_db_storage}")
#        data = {
#            "Ingestion_and_db_storage_response" : response_data,
#            "batch_id" : batch_id
#        }
#     else:
#        response_data = ["Failed: Zblock Data Ingestion and DB storage has failed"]
#        capture_log_message(f"{response_data}")
#        data = {
#            'Ingestion_and_db_storage_response': response_data
#        } 
#     end_time = datetime.now(timezone.utc)
    
#     capture_log_message(current_logger=g.stage_logger,log_message=' custom-client-zblock Completed',
#                         start_time=start_time,end_time=end_time,time_taken=end_time-start_time)
#     capture_log_message(log_message=' custom-client-zblock completed' , start_time=start_time,
#                         end_time=end_time,time_taken=end_time-start_time)  
    
#     # send_client_process_mail(subject='Status mail',is_gl=False,is_success=True,data = g.data_for_external_mail)
#     return jsonify(data)









# def run_custom_hist_data_load_ap (hist_id):
#      with app.app_context():
#         import requests
#         response = requests.get(f"{os.getenv('APP_URL')}/custom_hist_ap/{hist_id}", verify=False)
#         return response


# def run_custom_data_read_zblock (hist_id):
#      with app.app_context():
#         import requests
#         response = requests.get(f"{os.getenv('APP_URL')}/custom_data_read_zblock/{hist_id}", verify=False)
#         return response