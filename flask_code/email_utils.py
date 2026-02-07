"""
Email utility functions for data normalization and formatting.
Provides helper functions for converting various data formats to standardized forms.
"""

from datetime import timedelta
import re


def handle_stage1_pipeline_response(response, pipeline_mode):
    """
    Special handler for Stage 1 (/sap-raw-data-pipeline) responses.
    
    Stage 1 returns 6 different response types across 2 templates:
    - Template A (Returns 1-5): {"status": "failure", "error": "...", [total_time_seconds]}
    - Template B (Return 6): {"status": "success", "run_id": "...", "stage1_result": {...}, ...}
    
    This function handles ALL cases safely without KeyError.
    
    Args:
        response: requests.Response object from Stage 1 API call
        pipeline_mode: str, "ZBLOCK" or "AP"
    
    Returns:
        dict with keys:
        {
            "status": "success" | "failure" | "partial",
            "stage": "ZBLOCK_DATA_READ_STAGE" | "AP_DATA_READ_STAGE",
            "duration_seconds": float,
            "run_id": str or None,
            "message": str,
            "error": str or None,
            "error_type": str or None,
            "extracted_data": dict or None
        }
    """
    
    # Initialize default result structure
    result = {
        "status": "unknown",
        "stage": "ZBLOCK_DATA_READ_STAGE" if pipeline_mode == "ZBLOCK" else "AP_DATA_READ_STAGE",
        "duration_seconds": 0,
        "run_id": None,
        "message": "",
        "error": None,
        "error_type": None,
        "extracted_data": None
    }
    
    # CASE 1: No response or connection failed
    if not response:
        result["status"] = "failure"
        result["error"] = "No response from Stage 1 API"
        result["message"] = "Stage 1 failed: API connection error"
        return result
    
    # CASE 2: HTTP status is unexpected
    if response.status_code not in [200, 400, 403, 404, 500]:
        result["status"] = "failure"
        result["error"] = f"Unexpected HTTP status {response.status_code}"
        result["message"] = f"Stage 1 failed: HTTP {response.status_code}"
        return result
    
    # CASE 3: Response is not valid JSON
    try:
        response_json = response.json()
    except Exception as e:
        result["status"] = "failure"
        result["error"] = f"Invalid JSON response: {str(e)}"
        result["message"] = "Stage 1 failed: Response parsing error"
        return result
    
    # Check status field first (only guaranteed key across all responses)
    response_status = response_json.get("status", "unknown")
    
    # TEMPLATE A: Error Response (Returns 1-5)
    # Structure: {"status": "failure", "error": "...", [total_time_seconds]}
    if response_status == "failure":
        result["status"] = "failure"
        result["error"] = response_json.get("error", "Unknown error")
        result["duration_seconds"] = response_json.get("total_time_seconds", 0)
        result["message"] = f"Stage 1 failed: {result['error']}"
        
        # Categorize which return case this is based on HTTP status
        if response.status_code == 400:
            # Returns 1-4: Validation errors
            if "Request body is required" in result["error"]:
                result["error_type"] = "RETURN_1_NO_REQUEST_BODY"
            elif "pipeline_mode must be" in result["error"]:
                result["error_type"] = "RETURN_2_INVALID_PIPELINE_MODE"
            elif "incoming_folder is required" in result["error"]:
                result["error_type"] = "RETURN_3_MISSING_FOLDER_PARAM"
            elif "Incoming folder does not exist" in result["error"]:
                result["error_type"] = "RETURN_4_FOLDER_NOT_FOUND"
            else:
                result["error_type"] = "VALIDATION_ERROR_UNKNOWN"
        
        elif response.status_code == 500:
            # Return 5: Exception during pipeline
            result["error_type"] = "RETURN_5_EXCEPTION_DURING_EXECUTION"
            result["status"] = "partial"  # Mark as partial since some execution occurred
        
        return result
    
    # TEMPLATE B: Success Response (Return 6)
    # Structure: {"status": "success", "run_id": "...", "stage1_result": {...}, ...}
    elif response_status == "success":
        result["status"] = "success"
        result["run_id"] = response_json.get("run_id")
        result["duration_seconds"] = response_json.get("total_time_seconds", 0)
        result["message"] = response_json.get("message", "Stage 1 completed successfully")
        
        # Extract Stage 1 specific data
        stage1_result = response_json.get("stage1_result", {})
        # Always populate extracted_data for success case (even if API returns empty stage1_result)
        result["extracted_data"] = {
            "sap_files": stage1_result.get("sap_files", 0),
            "attachments": stage1_result.get("attachments", 0),
            "total_files": stage1_result.get("total_files", 0)
        }
        
        return result
    
    # UNEXPECTED: Unknown status value
    else:
        result["status"] = "unknown"
        result["error"] = f"Unexpected status value: {response_status}"
        result["message"] = f"Stage 1 returned unexpected status: {response_status}"
        return result


def handle_stage2_pipeline_response(response, pipeline_mode):
    """
    Handler for Stage 2 (/custom_hist_ap/ or /custom_data_read_zblock/) responses.
    
    Stage 2 returns 2 different response types:
    - Success: {"status": "success", "process": "AP"|"ZBLOCK", "batch_id": int, "duration": str, 
                "row_count": int, "acc_doc_count": int, "region_wise_acc_doc_count": dict|str}
    - Error: {"status": "error", "message": str}
    
    This function handles both cases safely without KeyError.
    
    Args:
        response: requests.Response object from Stage 2 API call
        pipeline_mode: str, "ZBLOCK" or "AP"
    
    Returns:
        dict with keys:
        {
            "status": "success" | "error",
            "stage": "ZBLOCK_DATA_INGESTION_STAGE" | "AP_DATA_INGESTION_STAGE",
            "duration_seconds": float,
            "batch_id": int or None,
            "message": str,
            "error": str or None,
            "metrics": {
                "row_count": int or None,
                "acc_doc_count": int or None,
                "region_wise_breakdown": dict or None
            }
        }
    """
    
    # Initialize default result structure
    result = {
        "status": "unknown",
        "stage": "ZBLOCK_DATA_INGESTION_STAGE" if pipeline_mode == "ZBLOCK" else "AP_DATA_INGESTION_STAGE",
        "duration_seconds": 0,
        "batch_id": None,
        "message": "",
        "error": None,
        "metrics": {
            "row_count": None,
            "acc_doc_count": None,
            "region_wise_breakdown": None
        }
    }
    
    # CASE 1: No response or connection failed
    if not response:
        result["status"] = "error"
        result["error"] = "No response from Stage 2 API"
        result["message"] = "Stage 2 failed: API connection error"
        return result
    
    # CASE 2: HTTP status is unexpected
    if response.status_code not in [200, 400, 500]:
        result["status"] = "error"
        result["error"] = f"Unexpected HTTP status {response.status_code}"
        result["message"] = f"Stage 2 failed: HTTP {response.status_code}"
        return result
    
    # CASE 3: Response is not valid JSON
    try:
        response_json = response.json()
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"Invalid JSON response: {str(e)}"
        result["message"] = "Stage 2 failed: Response parsing error"
        return result
    
    # Check for success/error - API returns "process" field not "status" for success
    response_status = response_json.get("status")  # Check status field for errors
    response_process = response_json.get("process")  # Check process field for success
    
    # ERROR CASE: Status is "error"
    if response_status == "error":
        result["status"] = "error"
        result["error"] = response_json.get("message", "Unknown error")
        result["message"] = f"Stage 2 failed: {result['error']}"
        # Batch ID not available in error response
        return result
    
    # SUCCESS CASE: process field is "success" (not status field)
    elif response_process == "success":
        result["status"] = "success"
        result["batch_id"] = response_json.get("batch_id")
        result["message"] = f"Stage 2 completed successfully for batch {result['batch_id']}"
        
        # Extract and normalize duration
        try:
            duration_raw = response_json.get("duration")
            if duration_raw:
                result["duration_seconds"] = normalize_duration_to_seconds(
                    duration_raw, 
                    stage_num=2
                )
        except Exception as e:
            # Duration parsing failed, but success still counts
            result["duration_seconds"] = 0
        
        # Extract metrics
        result["metrics"]["row_count"] = response_json.get("row_count")
        result["metrics"]["acc_doc_count"] = response_json.get("acc_doc_count")
        
        # Parse region-wise breakdown (could be dict or string)
        region_data = response_json.get("region_wise_acc_doc_count")
        if isinstance(region_data, dict):
            result["metrics"]["region_wise_breakdown"] = region_data
        elif isinstance(region_data, str):
            # Try to parse string format
            result["metrics"]["region_wise_breakdown"] = parse_regional_breakdown(region_data)
        else:
            result["metrics"]["region_wise_breakdown"] = None
        
        return result
    
    # UNEXPECTED: Unknown status value
    else:
        result["status"] = "unknown"
        result["error"] = f"Unexpected status value: {response_status}"
        result["message"] = f"Stage 2 returned unexpected status: {response_status}"
        return result


def handle_stage3_pipeline_response(response, pipeline_mode):
    """
    Handler for Stage 3 responses.
    
    ZBLOCK Stage 3 (/invoice_analysis/<batch_id>):
    - Success: {"status": "success", "batch_id": int, "total_invoices": int, "successful": int, 
                "failed": int, "processing_time": "HH:MM:SS.mmm"}
    - Error: {"status": "error", "error": str}
    
    AP Stage 3 (/ap-ingestion-and-scoring-flow/<batch_id>):
    - Success: {"status": "success", "message": [...], "batch_id": int, "duration": str, 
                "acc_doc_count": null}
    - Error: {"status": "error", "message": [...], "batch_id": int}
    
    This function handles both pipelines' responses safely without KeyError.
    
    Args:
        response: requests.Response object from Stage 3 API call
        pipeline_mode: str, "ZBLOCK" or "AP"
    
    Returns:
        dict with keys:
        {
            "status": "success" | "error",
            "stage": "ZBLOCK_INVOICE_VERIFICATION_STAGE" | "AP_DATA_SCORING_STAGE",
            "pipeline": "ZBLOCK" | "AP",
            "duration_seconds": float,
            "batch_id": int or None,
            "message": str,
            "error": str or None,
            "metrics": {
                # ZBLOCK-specific
                "total_invoices": int or None,
                "successful": int or None,
                "failed": int or None,
                "success_rate": float or None,  # Calculated as successful/total_invoices
                
                # AP-specific
                "acc_doc_count": int or None,
                
                # Both
                "processing_time": str or None
            }
        }
    """
    
    # Initialize default result structure
    result = {
        "status": "unknown",
        "stage": "ZBLOCK_INVOICE_VERIFICATION_STAGE" if pipeline_mode == "ZBLOCK" else "AP_DATA_SCORING_STAGE",
        "pipeline": pipeline_mode,
        "duration_seconds": 0,
        "batch_id": None,
        "message": "",
        "error": None,
        "metrics": {
            "total_invoices": None,
            "successful": None,
            "failed": None,
            "success_rate": None,
            "acc_doc_count": None,
            "processing_time": None
        }
    }
    
    # CASE 1: No response or connection failed
    if not response:
        result["status"] = "error"
        result["error"] = "No response from Stage 3 API"
        result["message"] = "Stage 3 failed: API connection error"
        return result
    
    # CASE 2: HTTP status is unexpected
    if response.status_code not in [200, 400, 500]:
        result["status"] = "error"
        result["error"] = f"Unexpected HTTP status {response.status_code}"
        result["message"] = f"Stage 3 failed: HTTP {response.status_code}"
        return result
    
    # CASE 3: Response is not valid JSON
    try:
        response_json = response.json()
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"Invalid JSON response: {str(e)}"
        result["message"] = "Stage 3 failed: Response parsing error"
        return result
    
    # Check status field
    response_status = response_json.get("status", "unknown")
    
    # ERROR CASE: Status is "error"
    if response_status == "error":
        result["status"] = "error"
        result["error"] = response_json.get("error") or response_json.get("message")
        result["batch_id"] = response_json.get("batch_id")
        
        # Format error message
        if isinstance(result["error"], list):
            result["message"] = f"Stage 3 failed: {result['error'][0] if result['error'] else 'Unknown error'}"
        else:
            result["message"] = f"Stage 3 failed: {result['error']}"
        
        return result
    
    # SUCCESS CASE: Status is "success"
    elif response_status == "success":
        result["status"] = "success"
        result["batch_id"] = response_json.get("batch_id")
        result["message"] = "Stage 3 completed successfully"
        
        # Extract duration and normalize
        try:
            if pipeline_mode == "ZBLOCK":
                # ZBLOCK uses "processing_time" field
                duration_raw = response_json.get("processing_time")
            else:  # AP
                # AP uses "duration" field
                duration_raw = response_json.get("duration")
            
            if duration_raw:
                result["duration_seconds"] = normalize_duration_to_seconds(
                    duration_raw, 
                    stage_num=3
                )
                result["metrics"]["processing_time"] = duration_raw
        except Exception as e:
            # Duration parsing failed, but success still counts
            result["duration_seconds"] = 0
        
        # Extract metrics based on pipeline type
        if pipeline_mode == "ZBLOCK":
            # ZBLOCK-specific metrics
            result["metrics"]["total_invoices"] = response_json.get("total_invoices")
            result["metrics"]["successful"] = response_json.get("successful")
            result["metrics"]["failed"] = response_json.get("failed")
            
            # Calculate success rate if we have the numbers
            total = result["metrics"]["total_invoices"]
            successful = result["metrics"]["successful"]
            if total and total > 0 and successful is not None:
                result["metrics"]["success_rate"] = (successful / total) * 100
                result["message"] = f"ZBLOCK: {successful}/{total} invoices verified successfully ({result['metrics']['success_rate']:.1f}%)"
        
        else:  # AP
            # AP-specific metrics
            result["metrics"]["acc_doc_count"] = response_json.get("acc_doc_count")
            
            # Handle message field (AP returns array)
            message_data = response_json.get("message")
            if isinstance(message_data, list) and len(message_data) > 0:
                result["message"] = message_data[0]
            elif isinstance(message_data, str):
                result["message"] = message_data
        
        return result
    
    # UNEXPECTED: Unknown status value
    else:
        result["status"] = "unknown"
        result["error"] = f"Unexpected status value: {response_status}"
        result["message"] = f"Stage 3 returned unexpected status: {response_status}"
        return result


def normalize_duration_to_seconds(duration_value, stage_num=None) -> float:
    """
    Convert duration to seconds regardless of input format.
    
    Handles multiple input formats:
    - float/int: Already in seconds (Stage 1)
    - timedelta: Python timedelta object (Stage 2 - calculated manually)
    - string: HH:MM:SS.mmm format (Stage 3 - from API response)
    
    Args:
        duration_value: Raw duration value from various sources
        stage_num (int, optional): Which stage (1, 2, or 3) for context/debugging
        
    Returns:
        float: Duration in seconds (e.g., 125.5)
        
    Raises:
        ValueError: If format is unrecognized
        
    Examples:
        # Stage 1 - Float seconds
        normalize_duration_to_seconds(125.5, stage_num=1)
        # Returns: 125.5
        
        # Stage 2 - Timedelta object
        from datetime import timedelta
        td = timedelta(minutes=2, seconds=5, milliseconds=500)
        normalize_duration_to_seconds(td, stage_num=2)
        # Returns: 125.5
        
        # Stage 3 - String format HH:MM:SS.mmm
        normalize_duration_to_seconds("00:02:05.500", stage_num=3)
        # Returns: 125.5
    """
    try:
        # Already float or int (Stage 1)
        if isinstance(duration_value, (float, int)):
            return float(duration_value)
        
        # Timedelta object (Stage 2)
        elif isinstance(duration_value, timedelta):
            return duration_value.total_seconds()
        
        # String format HH:MM:SS.mmm (Stage 3)
        elif isinstance(duration_value, str):
            return _parse_time_string_to_seconds(duration_value)
        
        else:
            raise ValueError(f"Unsupported duration format: {type(duration_value)}")
            
    except Exception as e:
        raise ValueError(
            f"Failed to normalize duration for stage {stage_num}: {str(e)}"
        )


def _parse_time_string_to_seconds(time_str: str) -> float:
    """
    Parse time string in HH:MM:SS.mmm format to seconds.
    
    Args:
        time_str (str): Time string like "01:45:23.456" or "2:5:3.5"
        
    Returns:
        float: Total seconds
        
    Examples:
        _parse_time_string_to_seconds("01:45:23.456")
        # Returns: 6323.456
        
        _parse_time_string_to_seconds("00:02:05.500")
        # Returns: 125.5
        
        _parse_time_string_to_seconds("0:0:45")
        # Returns: 45.0
    """
    if not time_str or not isinstance(time_str, str):
        raise ValueError(f"Invalid time string: {time_str}")
    
    time_str = time_str.strip()
    
    # Match HH:MM:SS or HH:MM:SS.mmm format
    # Allows variable digit count for each component
    pattern = r'^(\d+):(\d+):(\d+(?:\.\d+)?)$'
    match = re.match(pattern, time_str)
    
    if not match:
        raise ValueError(f"Time string does not match HH:MM:SS format: {time_str}")
    
    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = float(match.group(3))
    
    # Calculate total seconds
    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    
    return total_seconds


def format_duration(seconds: float) -> str:
    """
    Convert seconds to human-readable format (e.g., "2m 5s" or "45s").
    
    Assumes input is already in seconds (normalized).
    
    Args:
        seconds (float): Duration in seconds
        
    Returns:
        str: Formatted duration string
        
    Examples:
        format_duration(125.5) -> "2m 5s"
        format_duration(45.2) -> "45s"
        format_duration(3661.5) -> "1h 1m 1s"
    """
    try:
        seconds = float(seconds)
        
        if seconds < 60:
            return f"{int(seconds)}s"
        
        hours = int(seconds // 3600)
        remaining = seconds % 3600
        minutes = int(remaining // 60)
        secs = int(remaining % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        else:
            return f"{minutes}m {secs}s"
    except (ValueError, TypeError):
        return "N/A"


def parse_regional_breakdown(breakdown_str: str) -> dict:
    """
    Parse regional breakdown string and return as dictionary.
    
    Args:
        breakdown_str (str): String like "acc_doc_count_per_region: North: 150, South: 200, East: 89"
        
    Returns:
        dict: Dictionary with regions as keys and counts as values
        
    Examples:
        parse_regional_breakdown("acc_doc_count_per_region: North: 150, South: 200")
        # Returns: {"North": 150, "South": 200}
        
        parse_regional_breakdown("acc_doc_count_per_region, North: 150, South: 200")
        # Returns: {"North": 150, "South": 200}
    """
    try:
        if not breakdown_str or breakdown_str.strip() == "":
            return {}
        
        # Remove the prefix if present (anything before first region data)
        if ":" in breakdown_str:
            parts = breakdown_str.split(":", 1)
            if len(parts) == 2:
                breakdown_str = parts[1]
        
        regional_dict = {}
        # Split by comma to get region-count pairs
        pairs = breakdown_str.split(",")
        
        for pair in pairs:
            if ":" in pair:
                region, count = pair.split(":", 1)
                region = region.strip()
                count = count.strip()
                try:
                    regional_dict[region] = int(count)
                except ValueError:
                    continue
        
        return regional_dict
    except Exception as e:
        return {}


def sanitize_error_message(error: str, is_internal: bool = True) -> str:
    """
    Sanitize error message based on audience (internal vs external).
    
    Args:
        error (str): Raw error message/exception string
        is_internal (bool): If True, returns detailed error for developers.
                           If False, returns generic error for clients.
        
    Returns:
        str: Sanitized error message appropriate for audience
        
    Examples (Internal):
        sanitize_error_message("FileNotFoundError: /uploads/sap_data.csv", is_internal=True)
        # Returns: "FileNotFoundError: /uploads/sap_data.csv"
        
    Examples (External):
        sanitize_error_message("FileNotFoundError: /uploads/sap_data.csv", is_internal=False)
        # Returns: "Data source unavailable"
    """
    if not error or error.strip() == "":
        return "Unknown error occurred" if is_internal else "Processing error occurred"
    
    error_str = str(error).lower().strip()
    
    if is_internal:
        # Return full error for developers
        return str(error)
    else:
        # Return generic, user-friendly message for clients
        generic_messages = {
            "filenotfounderror": "Data source unavailable",
            "not found": "Data source unavailable",
            "connectionerror": "System connection error",
            "timeout": "Processing took too long",
            "null": "Data validation failed",
            "empty": "No valid data to process",
            "value error": "Invalid data format",
            "type error": "Data processing error",
            "database": "System database error",
            "permission": "Access denied",
        }
        
        for error_key, generic_msg in generic_messages.items():
            if error_key in error_str:
                return generic_msg
        
        # Default generic message
        return "Processing encountered an error. Please contact support."


def extract_metrics_from_response(response_json: dict, stage_num: int) -> dict:
    """
    Extract and structure metrics from stage API response.
    
    Args:
        response_json (dict): Response from stage endpoint (from response.json())
        stage_num (int): Stage number (1, 2, or 3)
        
    Returns:
        dict: Structured metrics dictionary with normalized durations
        
    Expected Response Format by Stage:
        Stage 1: {"status": "success", "stage1_result": {...}, "duration_seconds": 120}
        Stage 2: {"process": "success", "has_new_data": true, ...metrics...}
        Stage 3: {"status": "success", "processing_time": "01:45:23.456", ...metrics...}
    """
    try:
        metrics = {
            "stage": stage_num,
            "status": "unknown",
            "duration_seconds": 0.0,  # Always normalized to float seconds
            "error": None,
            "raw_data": response_json
        }
        
        # Get status
        if "status" in response_json:
            metrics["status"] = response_json.get("status", "unknown")
        elif "process" in response_json:
            metrics["status"] = "success" if response_json.get("process") == "success" else "failure"
        else:
            metrics["status"] = "unknown"
        
        # Get duration (already normalized by caller OR will be normalized by caller)
        if "duration_seconds" in response_json:
            metrics["duration_seconds"] = normalize_duration_to_seconds(
                response_json.get("duration_seconds"), stage_num=stage_num
            )
        elif "total_time_seconds" in response_json:
            metrics["duration_seconds"] = normalize_duration_to_seconds(
                response_json.get("total_time_seconds"), stage_num=stage_num
            )
        elif "processing_time" in response_json:
            # Stage 3 format
            metrics["duration_seconds"] = normalize_duration_to_seconds(
                response_json.get("processing_time"), stage_num=stage_num
            )
        
        # Get error if present
        if "error" in response_json:
            metrics["error"] = response_json.get("error")
        elif "message" in response_json and "failed" in str(response_json.get("message", "")).lower():
            metrics["error"] = response_json.get("message")
        
        return metrics
    except Exception as e:
        return {
            "stage": stage_num,
            "status": "error",
            "duration_seconds": 0.0,
            "error": str(e),
            "raw_data": response_json
        }
