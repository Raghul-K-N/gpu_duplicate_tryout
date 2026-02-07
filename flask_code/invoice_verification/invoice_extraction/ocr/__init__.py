from invoice_verification.invoice_extraction.ocr.ocr import extract_text_lines_from_image_using_ocr
from invoice_verification.logger.logger import log_message
from typing import List, Dict, Tuple

def ocr_extractor(file_path: str,
                detect_lang: str,
                return_checkbox_radio_mappings: bool,
                vendor_code: str
                ) -> Tuple[List, Dict]:
    """
    This function calls the PaddleOCR
    processing function

    Returns:
        List[]
    """

    log_message(f"Calling PaddleOCR for file: {file_path}")
    ocr_result, checkbox_radiobutton_mappings = extract_text_lines_from_image_using_ocr(file_path=file_path,
                                                               detect_lang=detect_lang,
                                                               return_checkbox_radio_mappings=return_checkbox_radio_mappings,
                                                               vendor_code=vendor_code)

    return ocr_result, checkbox_radiobutton_mappings