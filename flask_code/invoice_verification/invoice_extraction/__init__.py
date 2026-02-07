from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
from invoice_verification.invoice_extraction.ocr import ocr_extractor
from invoice_verification.invoice_extraction.pdf_plumber import pdf_extractor
from invoice_verification.invoice_extraction.llm import get_llama_result
from invoice_verification.invoice_extraction.helper import check_file_type
from invoice_verification.invoice_extraction.language_detector import detect_language_from_image
from typing import List, Dict, Any, Tuple


def extract_text_lines(
    account_document: str,
    file_path: str,
    vendor_code: str,
    return_checkbox_radio_mappings: bool = False
) -> Tuple[List[Any], Dict]:
    """
    This function checks the file type and
    extracts text lines using PDFPlumber or PaddleOCR.

    Args:
        account_document: Identifier for the account document.
        file_path: Path to the file to be processed.

    Returns:
        A list of extracted text lines (strings).
    """
    # log_message("Checking File type")
    # OCR_FLAG: bool = check_file_type(file_name=file_path)

    log_message(f"Started Extracting Text lines for Account Document: {account_document}")
    result: List[str] = []

    detect_lang = detect_language_from_image(image_path=file_path)
    log_message(f"Detected Language for the Input file: {detect_lang}")

    result, checkbox_radiobutton_mappings = ocr_extractor(file_path=file_path, 
                                                          detect_lang=detect_lang,
                                                          return_checkbox_radio_mappings=return_checkbox_radio_mappings,
                                                          vendor_code=vendor_code)
    # if OCR_FLAG:
    #     log_message("File is an Image or Scanned PDF")
    #     log_message("OCR Extraction process Started")
    #     result = ocr_extractor(file_path=file_path)
    # else:
    #     log_message("File is a Proper PDF")
    #     log_message("PDFPlumber Extraction process Started")
    #     result = pdf_extractor(file_path=file_path)

    # if result:
    #     for idx, line in enumerate(result):
    #         log_message(f"Line No: {idx + 1}, Line: {line}")
    # else:
    #     log_message(f"Extracted text lines is empty, result is {result}")

    if not result:
        log_message(f"Extracted text lines is empty, result is {result}")
    
    return result, checkbox_radiobutton_mappings



def get_llama_api_result(
    account_document: str,
    text_lines: List[Any],
    sap_row: SAPRow,
    invoice_type: str = "invoice"
) -> Dict[str, Any]:
    """
    This function calls the LLAMA API
    and post-processes the output.

    Args:
        account_document: Path or identifier for the account document.
        text_lines: List of text lines extracted from the document.

    Returns:
        A dictionary containing the processed Llama API result.
    """
    log_message(f"Started Llama API Process for account_document: {account_document}")
    result: Dict[str, Any] = get_llama_result(
        account_document=account_document,
        text_lines=text_lines,
        sap_row=sap_row,
        invoice_type=invoice_type
    )
    return result