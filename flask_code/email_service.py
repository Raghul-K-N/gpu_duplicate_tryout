import os
from flask import render_template
from flask_mailman import EmailMessage
from enum import Enum
from secret_manager import get_credentials
from functools import lru_cache

# Enumeration for data processing stages
class Stage(Enum):
    DATA_UPLOAD = "Data upload"
    DATA_HEALTH_CHECK_STATUS = "Data health check"
    DATA_INGESTION = "Data ingestion"
    DATA_SCORING = "Data scoring"
    INVOICE_VALIDATION = "Invoice validation"
    VENDOR_MASTER = "Vendor master"

# Email template names
INTERNAL_STAGE_EMAIL = "internal-stage-email.html"
CLIENT_PROCESS_EMAIL = "client-process-email.html"

FAILURE_ICON = "https://thinkriskdevblob.blob.core.windows.net/email-static-files/email-failure-icon.png"
SUCCESS_ICON = "https://thinkriskdevblob.blob.core.windows.net/email-static-files/email-success-icon.png"
THINKRISK_LOGO = "https://thinkrisk.ai/static/751571fef81e1b5d32e5c78bab80c2eb/8dcf5/tr-logo.png"



def convert_time_into_human_readable_format(seconds):
    try:
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
    except Exception as e:
        return "N/A"

def is_null_like(x):
    """
    Return True for None, empty strings, or textual 'none'/'null' (case-insensitive).
    """
    if x is None:
        return True
    if isinstance(x, str):
        return x.strip().lower() in ("", "none", "null")
    if isinstance(x, (list, tuple, set, dict)) and len(x) == 0:
        return True
    return False

def skip_null_values(lst):
    """
    Return a new list with all 'null-like' values removed.
    Uses is_null_like to decide what to skip.
    """
    return [x for x in lst if not is_null_like(x)]

def preview_ids(ids, limit=5):
    """
    Return a string preview of the first `limit` items in the list.
    If more than `limit`, append 'and N more'.
    """
    if not ids:
        return "N/A"
    total = len(ids)
    preview = ", ".join(str(x) for x in ids[:limit])
    if total > limit:
        preview = f"{preview} and {total - limit} more"
    return preview

@lru_cache(maxsize=None)
def get_email_credentials():
    return get_credentials("MAIL")
        
def get_email_username():
    return get_email_credentials()['username']

def send_email(subject, body, ishtml=False, recipients = None):
    """
    Sends an email with the specified subject, body, and optional HTML formatting.

    Args:
        subject (str): The subject line of the email.
        body (str): The content of the email.
        ishtml (bool, optional): Whether the body content is HTML formatted. Defaults to False.

    Returns:
        tuple: A tuple containing a boolean indicating success and a message.
    """

    try:       
        # return True,'Success'
        if not recipients:
            recipients = str(os.getenv('DEFAULT_RECIPIENTS')).split(',')
        email = EmailMessage( 
            subject, body=body, from_email=os.getenv('MAIL_FROM'), to=recipients)
        if ishtml:
            email.content_subtype = "html"
        email.send()
        return True, "Email sent successfully"

    except Exception as e:
        return False, f"Failed to send the email: {str(e)}"


# def send_internal_stage_mail(subject, stage, process, is_success, data):
#     """
#     Sends an internal email notification about a processing stage.

#     Args:
#         subject (str): The subject line of the email.
#         stage (str): The name of the processing stage.
#         is_gl (bool): Whether the process is GL or AP.
#         is_success (bool): Whether the stage was successful or failed.
#         data (dict[]): Table data to include in the email content.

#     Returns:
#         tuple: The return value of the `send_email` function, indicating success or failure.
#     """

#     INTERNAL_EMAIL_RECIPIENTS = str(os.getenv('INTERNAL_EMAIL_RECIPIENTS')).split(',')
#     content = render_template(
#         INTERNAL_STAGE_EMAIL,
#         stage=Stage[stage].value,  # Retrieve stage name from enumeration
#         key=Stage[stage].name,
#         status="success" if is_success else "failed",
#         icon = SUCCESS_ICON if is_success else FAILURE_ICON,
#         logo = THINKRISK_LOGO,
#         process=process,
#         data=data,
#     )
#     return send_email(subject, content, True, INTERNAL_EMAIL_RECIPIENTS)


def send_client_process_mail(run_timestamp , status,stage1_result=None, stage2_result=None, stage3_result=None, pipeline_mode=None, batch_id=None):
    """
    Sends a client email notification with status of pipeline stages.
    Called at the end of MASTER API execution (success or failure at any stage).
    
    Args:
        run_timestamp (str): Timestamp of the pipeline run
        stage2_result (dict): Result dict from Stage 2 handler (or None if not executed)
        stage3_result (dict): Result dict from Stage 3 handler (or None if not executed)
        pipeline_mode (str): "AP" or "ZBLOCK" (for selecting recipients)
        batch_id (int): Batch ID for the pipeline run
    
    Returns:
        tuple: The return value of the `send_email` function, indicating success or failure.
    """
    from datetime import datetime
    
    send_client_mail = os.getenv('SEND_CLIENT_EMAILS','NO').upper()
    if str(send_client_mail) != 'YES':
        return True, "Client email sending is disabled via environment variable."
    

    # Define stage names mapping for the pipeline
    stage_names = {
        1: "Data Upload",
        2: "Data Health Check",
        3: "Data Ingestion & Scoring" if pipeline_mode == "AP" else "Invoice Verification"
    }
    
    # Build pipeline based on which stage results are provided
    pipeline = []
    if stage3_result is None:
        if stage2_result is None:
            pipeline = [stage_names[1]]
        else:
            pipeline = [stage_names[1], stage_names[2]]
    else:
        pipeline = [stage_names[1], stage_names[2], stage_names[3]]
    
    # Build unified data array with all stages (success and failure)
    data = []
    failed_stage = None
    overall_status = "success"
    
    # Process all stage results
    for stage_num, result in enumerate([stage1_result, stage2_result, stage3_result], 1):
        if result is None:
            continue
        
        stage_status = status
        
        # Track first failure
        if stage_status == "failure" and failed_stage is None:
            failed_stage = {"key": stage_num, "value": stage_names[stage_num]}
            overall_status = "failure"
        
        # Build row data
        row = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "name": stage_names[stage_num],
            "status": stage_status,
            "record_count": result.get("metrics", {}).get("record_count", "N/A"),
            "description": result.get("message", ""),
            "time_taken": f"{result.get('duration_seconds', 0):.2f}s"
        }
        
        # Add failure details if failed
        if stage_status == "failure":
            row["reason"] = result.get("error", "Unknown error")
            row["failed_count"] = result.get("metrics", {}).get("failed_count", 0)
            row["total_count"] = result.get("metrics", {}).get("total_count", 0)
        
        data.append(row)
    
    # Determine recipients based on pipeline mode
    if pipeline_mode == "AP":
        CLIENT_EMAIL_RECIPIENTS = str(os.getenv('AP_FLOW_EMAIL_RECIPIENTS')).split(',')
    else:  # ZBLOCK
        CLIENT_EMAIL_RECIPIENTS = str(os.getenv('INVOICE_VERIFICATION_EMAIL_RECIPIENTS')).split(',')
    
    # Render template with formatted data
    content = render_template(
        CLIENT_PROCESS_EMAIL,
        status=overall_status,
        process=pipeline_mode,
        icon=SUCCESS_ICON if overall_status == "success" else FAILURE_ICON,
        logo=THINKRISK_LOGO,
        pipeline=pipeline,
        data=data,
        failed_stage=failed_stage
    )
    
    # Send email with appropriate subject
    subject = f"Pipeline Completion: {pipeline_mode} Flow  - {run_timestamp} -  {'Success' if overall_status == 'success' else 'Failed'}"
    return send_email(subject, content, True, CLIENT_EMAIL_RECIPIENTS)



from datetime import datetime,timezone
def send_internal_stage_email(handler_result, batch_id, stage_num, pipeline_mode,status,run_timestamp):
    """
    Send internal email with stage metrics based on standardized handler result.
    
    This function takes the normalized output from handle_stage1/2/3_pipeline_response()
    and renders it using the internal-stage-email.html template.
    
    Args:
        handler_result (dict): Output from handle_stageN_pipeline_response()
        batch_id (int): Batch ID
        stage_num (int): Stage number (1, 2, or 3)
        pipeline_mode (str): "ZBLOCK" or "AP"
    
    Returns:
        tuple: (success: bool, message: str)
    """

    send_internal_emails = os.getenv('SEND_INTERNAL_EMAILS','NO').upper()
    if str(send_internal_emails) != 'YES':
        return True, "Internal email sending is disabled via environment variable."
    
    
    try:

        if stage_num == 1:
            stage = handler_result.get('stage', 'SAP Raw Data Read')
            status = status
            data = []
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            time_taken = handler_result.get('total_duration',0)
            time_taken_str = convert_time_into_human_readable_format(time_taken)
            default_description = "SAP Raw Data Read completed successfully." if status == "success" else "SAP Raw Data Read failed."
            main_description = handler_result.get('description', default_description)
            total_attachment_count = handler_result.get('total_attachments', 0)
            total_sap_files_count = handler_result.get('total_sap_files', 0)
            data.append({
                "date": date,
                "no_of_records": total_attachment_count+ total_sap_files_count,
                "time_taken": time_taken_str,
                "description": main_description,
                "success": True if status == "success" else False,
            })
            data.append({
                "date": date,
                "no_of_records": total_sap_files_count,
                "time_taken": time_taken_str,
                "description": "Number of SAP files processed.",
                "success": True if status == "success" else False,
            })
            data.append({
                "date": date,
                "no_of_records": total_attachment_count,
                "time_taken": time_taken_str,
                "description": "Number of attachments processed.",
                "success": True if status == "success" else False,
            })

        elif stage_num == 2:
            stage = handler_result.get('stage', 'Data Ingestion')
            status = status
            data = []
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            time_taken = handler_result.get('duration_seconds',0)
            time_taken_str = convert_time_into_human_readable_format(time_taken)
            default_description = "Data Ingestion completed successfully." if status == "success" else "Data Ingestion failed."
            main_description = handler_result.get('description', default_description)
            no_of_acc_docs = handler_result.get('no_of_acc_docs', 0)

            # Accept either naming coming from app or elsewhere (plural/singular)
            _regionwise = handler_result.get('region_wise_acc_docs_count',
                            handler_result.get('region_wise_acc_doc_count', {}))
            region_wise_acc_docs_count = _regionwise or {}

            duplicate_acc_docs_count = handler_result.get('duplicate_acc_docs_count', 0)
            clearing_date_docs_skipped = handler_result.get('clearing_date_docs_skipped', 0)

            # Defensive parsing: accept list or comma-separated string for ref IDs
            _ref_ids_skipped = handler_result.get('ref_ids_with_clearing_date_skipped', [])
            if isinstance(_ref_ids_skipped, str):
                ref_ids_with_clearing_date_skipped = [s.strip() for s in _ref_ids_skipped.split(',') if s.strip()]
            elif isinstance(_ref_ids_skipped, list):
                ref_ids_with_clearing_date_skipped = _ref_ids_skipped
            else:
                ref_ids_with_clearing_date_skipped = []

            _dup_ids = handler_result.get('duplicate_data_ref_ids', [])
            if isinstance(_dup_ids, str):
                ref_ids_with_duplicates_skipped = [s.strip() for s in _dup_ids.split(',') if s.strip()]
            elif isinstance(_dup_ids, list):
                ref_ids_with_duplicates_skipped = _dup_ids
            else:
                ref_ids_with_duplicates_skipped = []

                # Remove null-like values from the ID lists
                ref_ids_with_duplicates_skipped = skip_null_values(ref_ids_with_duplicates_skipped)
                ref_ids_with_clearing_date_skipped = skip_null_values(ref_ids_with_clearing_date_skipped)

            data.append({
                "date": date,
                "no_of_records": no_of_acc_docs,
                "time_taken": time_taken_str,
                "description": main_description,
                "success": True if status == "success" else False,
            })

            data.append({
                "date": date,
                "no_of_records": no_of_acc_docs,
                "time_taken": time_taken_str,
                "description": f"Region-wise ACC document counts: {region_wise_acc_docs_count}",
                "success": True if status == "success" else False,
            })
            if pipeline_mode=='ZBLOCK':
                    data.append({
                        "date": date,
                        "no_of_records": duplicate_acc_docs_count,
                        "time_taken": time_taken_str,
                        "description": f"Number of duplicate ACC documents skipped: {duplicate_acc_docs_count}. Ref IDs: {preview_ids(ref_ids_with_duplicates_skipped)}",
                        "success": True if status == "success" else False,
                    })
                    data.append({
                        "date": date,
                        "no_of_records": clearing_date_docs_skipped,
                        "time_taken": time_taken_str,
                        "description": f"Number of ACC documents skipped due to clearing date present: {clearing_date_docs_skipped}. Ref IDs: {preview_ids(ref_ids_with_clearing_date_skipped)}",
                        "success": True if status == "success" else False,
                    })

        elif stage_num == 3:
            default_stage = 'Invoice Verification' if pipeline_mode == 'ZBLOCK' else 'AP Ingestion and Scoring'
            stage = handler_result.get('stage', default_stage)
            status = status
            data = []
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            time_taken = handler_result.get('duration_seconds',0)
            time_taken_str = convert_time_into_human_readable_format(time_taken)
            default_zblock_desc = "Invoice Verification completed successfully." if status == "success" else "Invoice Verification failed."
            default_ap_desc = "AP Ingestion and Scoring completed successfully." if status == "success" else "AP Ingestion and Scoring failed."
            default_description = default_zblock_desc if pipeline_mode == 'ZBLOCK' else default_ap_desc
            
            # Extract record count based on pipeline mode
            if pipeline_mode == 'ZBLOCK':
                no_of_records = handler_result.get('total_invoices', 0)
            else:  # AP
                no_of_records = handler_result.get('total_records', 0)

            data.append({
                "date": date,
                "no_of_records": no_of_records,
                "time_taken": time_taken_str,
                "description": default_description,
                "success": True if status == "success" else False,
            })

        
        # Render template with handler result data
        INTERNAL_EMAIL_RECIPIENTS = str(os.getenv('INTERNAL_EMAIL_RECIPIENTS')).split(',')
        
        content = render_template(
            INTERNAL_STAGE_EMAIL,
            stage=stage,
        
            status=status,
            icon=SUCCESS_ICON if status == "success" else FAILURE_ICON,
            logo=THINKRISK_LOGO,
            process=pipeline_mode,
            data=data
        )
        
        # Create email subject
        status_text = "SUCCESS" if status == "success" else "FAILED"
        subject = f"{pipeline_mode}  Flow-   Time {run_timestamp} - {status_text} - Batch {batch_id}"
        
        # Send email
        success, msg = send_email(subject, content, ishtml=True, recipients=INTERNAL_EMAIL_RECIPIENTS)
        
        return success, msg
        
    except Exception as e:
        return False, f"Error sending internal stage email: {str(e)}"
