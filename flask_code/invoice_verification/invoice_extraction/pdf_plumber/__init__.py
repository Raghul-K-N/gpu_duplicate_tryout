from invoice_verification.invoice_extraction.pdf_plumber.pdfplumber import extract_text_lines_from_pdf
from invoice_verification.logger.logger import log_message
from typing import List

def pdf_extractor(file_path: str) -> List:
    """
    This function calls the PDFPlumber
    processing function

    Returns:
        List[]
    """

    log_message(f"Calling PDFPlumber for file: {file_path}")
    ocr_result: List = extract_text_lines_from_pdf(file_path=file_path)

    return ocr_result