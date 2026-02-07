from typing import List, Dict, Any
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
from invoice_verification.invoice_extraction.llm.llm import LlamaAPI

def get_llama_result(text_lines: List[Any], 
                    account_document: str,
                    sap_row: SAPRow,
                    invoice_type: str
                    ) -> Dict[str,Any]:
    """
    This functions calls the LLAMA API
    and it formats the output
    
    Returns:
        Dict
    """
    log_message(f"Started Llama API process for account_document: {account_document}")
    # Get LLAMA API Result
    llama_api: LlamaAPI = LlamaAPI(account_document=account_document,
                        text_lines=text_lines,sap_row=sap_row,invoice_type=invoice_type)
    llama_result: Dict[str,Any] = llama_api.extract_fields()

    return llama_result
