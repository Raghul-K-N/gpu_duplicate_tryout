from invoice_verification.invoice_extraction.llm.utils import extract_invoice_currency_from_raw_response, \
    extract_params_manually_from_textlines, filter_ocr_noise_hybrid, validate_extracted_gl_account_numbers_list \
    , swap_vendor_bill_to_details_if_needed
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
import time
import requests
import json
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np
import os

# Define function to detect and fix European format
def fix_european_format(val: str) -> str:
    """
    Detects European number format where:
    - Dot (.) is thousand separator
    - Comma (,) is decimal separator
    
    Detection: If comma appears after the last dot position-wise,
    it's European format (e.g., "1.234,56" or "55.00,45")
    
    Handles:
    - Leading/trailing whitespace
    - Space as thousand separator (European: "1 234,56")
    - Multiple commas/dots (disambiguates by position and count)
    """
    if pd.isna(val) or val == '' or val == 'nan' or val == 'None':
        return val
    
    # Strip leading/trailing whitespace
    val = val.strip()
    
    if val == '':
        return val
    
    # Handle space as thousand separator (European format: "1 234,56")
    # Count spaces and separators to determine format
    comma_count = val.count(',')
    dot_count = val.count('.')
    space_count = val.count(' ')
    
    # Remove spaces that act as thousand separators
    if space_count > 0:
        val = val.replace(' ', '')
    
    last_dot = val.rfind('.')
    last_comma = val.rfind(',')
    
    # European format: comma appears after dot (dot must exist AND comma position > dot position)
    # Examples: "1.234,56", "55.00,45", "1.234.567,89"
    if last_dot != -1 and last_comma > last_dot:
        # Remove all dots (thousand separators)
        # Replace comma with dot (decimal separator)
        val = val.replace('.', '').replace(',', '.')
    # US format: dot appears after comma or no comma at all
    # Examples: "1,234.56", "55,000.45"
    elif last_dot > last_comma or (last_dot != -1 and last_comma == -1):
        # Remove all commas (thousand separators)
        val = val.replace(',', '')
    # Only comma, no dot: AMBIGUOUS - need to check digits after comma
    # Examples: "75,420" (US) vs "75,42" (European)
    elif last_comma != -1 and last_dot == -1:
        # Count digits after the comma
        digits_after_comma = len(val) - last_comma - 1
        
        # Get the part before the last comma to check pattern
        before_comma = val[:last_comma]
        
        # Handle multiple commas: "10,555,42" or "10,555,420"
        if comma_count > 1:
            # Multiple commas suggest thousand separators
            # Check if it's really US (remove commas) or European (treat last comma as decimal)
            # Heuristic: if last segment has exactly 3 digits → likely US thousand seps
            # If last segment has 1-2 digits → likely European decimal
            if digits_after_comma == 3:
                val = val.replace(',', '')
            elif digits_after_comma <= 2:
                # Likely European: replace ALL commas except last with nothing, 
                # then replace last comma with dot
                parts = val.split(',')
                val = ''.join(parts[:-1]) + '.' + parts[-1]
            else:
                # More than 3 digits after comma → treat as decimal
                parts = val.split(',')
                val = ''.join(parts[:-1]) + '.' + parts[-1]
        elif digits_after_comma == 3:
            # Exactly 3 digits after comma
            # To be a US thousand separator, the part BEFORE comma must NOT be 
            # just a long string of digits without structure
            # Heuristic: if before_comma matches pattern "digit,digit,digit" (with commas),
            # it's already shown thousand sep structure. If it's just raw digits,
            # it's likely a European decimal with large integer part
            if ',' not in before_comma and len(before_comma) > 3:
                # No comma structure before, many digits before comma
                # Example: "123456,789" - more likely European decimal (123456.789)
                val = val.replace(',', '.')
            else:
                # Has proper structure or short integer part
                # Example: "75,420" or "1,420" - likely US thousand separator
                val = val.replace(',', '')
        elif digits_after_comma <= 2:
            # 1-2 digits after comma → European decimal separator
            val = val.replace(',', '.')
        else:
            # More than 3 digits after comma → Treat as decimal
            val = val.replace(',', '.')
    # Only dot, no comma: already correct
    else:
        val = val.replace(',', '')
    
    return val

class LlamaAPI():
    """
    A class to interact with a Language Model for extracting key fields from invoice text lines.
    """

    def __init__(self, account_document: str, 
                text_lines: List,
                sap_row: SAPRow,
                invoice_type: str 
                ) -> None:
        self.account_document = account_document
        self.text_lines = text_lines
        self.sap_row = sap_row
        self.invoice_type = invoice_type
        llama_api_base_path = os.getenv("LLAMA_API_BASE_URL", "").rstrip('/')
        api_endpoint = f"{llama_api_base_path}/v1/fetch-invoice-keyfields"
        self.API_ENDPOINT = api_endpoint
        self.api_key = "Llama-api/version-0.1"
        self.timeout = 800
        self.response_field_names = {"gl_account": "gl_account_number",
                                    "invoice_number": "invoice_number",
                                    "stamp_number": "stamp_number",
                                    "invoice_amount": "invoice_amount",
                                    "vendor_name": "vendor_name",
                                    "remit_to_address": "vendor_address",
                                    "bill_to_entity": "legal_entity_name",
                                    "bill_to_address": "legal_entity_address",
                                    "invoice_currency": "invoice_currency",
                                    "payment_method": "payment_method",
                                    "banking_account_number": "bank_account_number",
                                    "bank_name": "bank_name",
                                    "account_holder_name": "bank_account_holder_name",
                                    "payment_terms": "payment_terms",
                                    "invoice_date": "invoice_date",
                                    "vat_tax_code": "vat_tax_code",
                                    "vat_amount": "vat_tax_amount",
                                    "invoice_receipt_date": "invoice_receipt_date",
                                    "transaction_type": "transaction_type"
                                    }
        


    # LLAMA API CALL HANDLING
    def _prepare_headers(self, api_key: Optional[str] = None) -> Dict[str, str]:
        """Prepare request headers"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        if api_key:
            if not isinstance(api_key, str) or not api_key.strip():
                log_message("API key must not be empty", error_logger=True)
                raise ValueError("API key must be a non-empty string")
            headers['Authorization'] = f'Bearer {api_key.strip()}'
        return headers
    
    def _prepare_payload(self, text_lines: List[str]) -> Dict[str, Any]:
        """Prepare request payload"""
        return {
            "lines": text_lines,
            "filename": str(self.account_document)
        }
    
    def _validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response"""
        # Check for HTTP errors
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            log_message(f"Received response with status code: {response.status_code},response body: {response.text}", error_logger=True)
            status_code = response.status_code
            error_body = response.text
            
            # Handle specific HTTP status codes
            if status_code == 400:
                error_msg = f"Bad Request: Invalid data format. Server response: {error_body}"
                error_type = 'bad_request'
            elif status_code == 401:
                error_msg = "Unauthorized: Invalid or missing authentication credentials"
                error_type = 'unauthorized'
            elif status_code == 403:
                error_msg = "Forbidden: Access denied. Check API permissions"
                error_type = 'forbidden'
            elif status_code == 404:
                error_msg = f"Not Found: API endpoint {self.API_ENDPOINT} not found"
                error_type = 'not_found'
            elif status_code == 413:
                error_msg = "Payload Too Large: Request data exceeds server limits"
                error_type = 'payload_too_large'
            elif status_code == 422:
                error_msg = f"Unprocessable Entity: {error_body}"
                error_type = 'unprocessable_entity'
            elif status_code == 429:
                retry_after = response.headers.get('Retry-After', 'unknown')
                error_msg = f"Rate Limited: Too many requests. Retry after: {retry_after}"
                error_type = 'rate_limited'
            elif status_code == 502:
                error_msg = "Bad Gateway: Server is temporarily unavailable"
                error_type = 'bad_gateway'
            elif status_code == 503:
                error_msg = "Service Unavailable: Server is temporarily down"
                error_type = 'service_unavailable'
            elif status_code == 504:
                error_msg = "Gateway Timeout: Server took too long to respond"
                error_type = 'gateway_timeout'
            elif 500 <= status_code < 600:
                error_msg = f"Server Error ({status_code}): Internal server error"
                error_type = 'server_error'
            else:
                error_msg = f"HTTP Error ({status_code}): {str(e)}"
                error_type = 'http_error'
            log_message(f"Error Occured After Llama API call: error type: {error_type} and error msg: {error_msg}", error_logger=True)
            raise
        
        # Validate content type
        content_type = response.headers.get('content-type', '').lower()
        if 'application/json' not in content_type:
            log_message("Content type is not valid", error_logger=True)
            raise
    
    def _parse_response(self, response: requests.Response) -> Any:
        """Parse and validate JSON response"""
        try:
            if not response.content:
                raise
            
            json_response = response.json()
            
            # Basic response validation
            if json_response is None:
                raise
            
            return json_response
            
        except json.JSONDecodeError as e:
            # Try to get some context around the error
            log_message(f"Error Occured After llama API call: Json Decoder error", error_logger=True)

        except UnicodeDecodeError as e:
            raise
    
    def make_api_call(self) -> Any:
        """
        Make secure API call with comprehensive error handling
        
        Args:
            text_lines: List of text lines or single string to send
            api_key: Optional API key for authentication
            
        Returns:
            JSON response data from the API
            
        Raises:
            APIRequestError: For all API-related errors
            ValueError: For input validation errors
            TypeError: For type-related errors
        """
        start_time = time.time()
        
        try:

            # Filter out Noisy data lines before sending to LLM API
            filtered_lines = filter_ocr_noise_hybrid(
                                        lines=self.text_lines,
                                        prose_block_min_lines=3,
                                        min_long_length=50,
                                        window_size=12,
                                        max_numeric_ratio=0.2,
                                        max_prose_windows_before_stop=3,
                                            )
            
            log_message(f"Before Filtering, No of lines: {len(self.text_lines)}, After Filtering, No of lines: {len(filtered_lines)}")

            # Validate inputs
            headers = self._prepare_headers(self.api_key)
            payload = self._prepare_payload(text_lines=filtered_lines)
            log_message(f"No of lines sent to Llama API: {len(filtered_lines)}")

            for line in filtered_lines:
                log_message(f"{line}")
            # Log request info
            log_message(f"Making API request to {self.API_ENDPOINT}")
            # log_message(f"Payload: {payload}")

            # Call llama api 
            response = requests.post(
                                self.API_ENDPOINT,
                                headers=headers,
                                json=payload,
                                timeout=self.timeout
                            )
            
            log_message(f"Received response with status code: {response.status_code}")
            log_message("Response from API Call Headers & values")
           
            # Validate response
            self._validate_response(response)
            
            # Parse response
            json_response = self._parse_response(response)
            try:
                for k, v in json_response.items():
                    log_message(f"{k}: {v}")
            except Exception as e:
                log_message(f"Error logging response headers: {str(e)}", error_logger=True)
            
            # Log success
            execution_time = time.time() - start_time
            log_message(f"API call successful in {execution_time:.2f}s")
            log_message(f"json response from llama: {json_response}")
            return json_response['invoice_data'], json_response['raw_response']
            
        except (ValueError, TypeError):
            log_message("Value error or Type error Occured", error_logger=True)
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            # Catch any other unexpected errors
            execution_time = time.time() - start_time
            log_message(f"Unexpected error after {execution_time:.2f}s: {str(e)}", error_logger=True)
            raise




    def llama_output_formatting(self, response) -> Dict:
        converted_dict: Dict = {self.response_field_names.get(k, k): v for k, v in response.items()}
        # legal_entity_dict: Dict = {
        #     "legal_entity": '\n'.join(filter(None, [converted_dict["legal_entity_name"], converted_dict["legal_entity_address"]])) or None
        # }
        # merged: Dict = {**converted_dict, **legal_entity_dict}
        merged: Dict = {**converted_dict}


        # Length wise pruning for all fields, to avoid extremely long values / incorrect values
        max_length_dict = {
            "vendor_name": 255,
            "legal_entity_name": 255,
            "payment_terms": 255,
            "payment_method": 255,
            }
        

        # if Value exceeds max length, set to None
        for key, max_len in max_length_dict.items():
            if key in merged and merged[key] is not None:
                if isinstance(merged[key], list):
                    val_str = ' '.join(map(str, merged[key]))
                else:
                    val_str = str(merged[key])
                if len(val_str) > max_len:
                    log_message(f"Field '{key}' exceeded max length of {max_len}.")
                    merged[key] = val_str[:max_len-1]  # trim only max length fields



    

        # Additional Logic for Invoice amount and VAT tax amount
        # Amounts are in String format, either normal amount where thousand separator is comma or European format where thousand separator is dot
        try:
            invoice_amount = merged.get("invoice_amount")
            if invoice_amount:
                cleaned_amount = fix_european_format(invoice_amount)
                final_amount= pd.to_numeric(cleaned_amount,errors='raise')
                log_message(f"Formatted invoice amount: {final_amount}")
                merged["invoice_amount"] = final_amount
        except Exception as e:
            log_message(f"Error Occured during amount formatting: {str(e)}", error_logger=True)

        try:
            vat_amount = merged.get("vat_tax_amount")
            if vat_amount:
                cleaned_vat_amount = fix_european_format(vat_amount)
                final_vat_amount= pd.to_numeric(cleaned_vat_amount,errors='raise')
                log_message(f"Formatted VAT tax amount: {final_vat_amount}")
                merged["vat_tax_amount"] = final_vat_amount
        except Exception as e:
            log_message(f"Error Occured during VAT amount formatting: {str(e)}", error_logger=True)

        
        def is_empty_or_none(v):
            """Safely check if value is None or empty"""
            if v is None or v == "" or v == 'None':
                return True
            # Handle containers
            if isinstance(v, (list, dict)) and len(v) == 0:
                return True
            # Handle numpy arrays
            if isinstance(v, np.ndarray):
                return v.size == 0
            # Handle NaN
            if isinstance(v, (float, np.floating)) and np.isnan(v):
                return True
            return False
               
        none_count =0
        non_null_count =0
        for k,v in merged.items():
            if is_empty_or_none(v):
                none_count +=1
            else:
                non_null_count +=1
 
        log_message(f"LLAMA extracted fields - Non null count: {non_null_count}, None count: {none_count}")
 
        return {k: v for k, v in merged.items()}


    def extract_fields(self) -> Dict:
        response,raw_response = self.make_api_call()
        extracted_dict: Dict = self.llama_output_formatting(response=response)

        extracted_dict = swap_vendor_bill_to_details_if_needed(
            sap_row=self.sap_row,
            extracted_dict=extracted_dict,)

        manual_extracted_dict = extract_params_manually_from_textlines(sap_row=self.sap_row,
                                                                text_lines=self.text_lines,
                                                                invoice_type=self.invoice_type)
        
        if manual_extracted_dict:
            log_message(f"Manually extracted fields: {manual_extracted_dict}")
            # Override with manually extracted fields
            for key,value in manual_extracted_dict.items():
                llm_value = extracted_dict.get(key)
                if llm_value is None and value is not None and value != '':
                    extracted_dict[key] = value
                    log_message(f"Field '{key}' overridden with manually extracted value: {value}")

        result = extract_invoice_currency_from_raw_response(actual_response=extracted_dict,
                                                                 raw_response=raw_response,
                                                                 sap_row=self.sap_row)
        currency_value, reason = result if result else (None, None)
        log_message(f"Invoice currency extraction result: {currency_value}, Reason: {reason}")
        if currency_value is not None:
            extracted_dict['invoice_currency'] = currency_value


        # Validate the extracted GL account numbers list
        gl_account_value = extracted_dict.get('gl_account_number')
        if gl_account_value and isinstance(gl_account_value, list):
            validated_gl_accounts = validate_extracted_gl_account_numbers_list(gl_account_value)
            extracted_dict['gl_account_number'] = validated_gl_accounts
            log_message(f"Validated GL account numbers: {validated_gl_accounts}")

        if self.sap_row.vendor_code == "1021768" and self.sap_row.region == "EMEAI":
            log_message("manually Overriding - For Vendor Code 1021768 in EMEAI region - Setting invoice currency to AED")
            extracted_dict['invoice_currency'] = ["AED"]

        return extracted_dict
    

