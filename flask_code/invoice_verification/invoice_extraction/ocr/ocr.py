import pdfplumber
from invoice_verification.invoice_extraction.ocr.helper import merge_text_with_spaces, adapt_paddle_result, LATIN_LANGS
from invoice_verification.invoice_extraction.ocr.voucher_ops import extract_form_data
from invoice_verification.logger.logger import log_message
from invoice_verification.Parameters.constants import CITI_BANK_VENDOR_CODES
from invoice_verification.invoice_extraction.paddle_models import chinese_model
from invoice_verification.invoice_extraction.helper import get_ocr_model_for_language
from typing import List, Tuple, Dict
from datetime import datetime
import numpy as np

import os
import multiprocessing
logical_core_count = os.cpu_count() or multiprocessing.cpu_count()
NO_OF_WORKERS = logical_core_count - 1 if logical_core_count and int(logical_core_count) > 1 else 1

SUPPORTED_LANGS = {'en'}.union(LATIN_LANGS)

THRESHOLD_PDF_PAGES_FOR_LARGE_PDF = 3
MAX_PAGES_TO_PROCESS_FOR_LARGE_PDF = 3

def extract_text_lines_from_image_using_ocr(file_path:str,
                                            detect_lang: str,
                                            return_checkbox_radio_mappings: bool,
                                            vendor_code: str
                                            ) -> Tuple[List, Dict]:
    """
    Extract text lines from invoice copy from image using  PADDLE OCR.

    Args:
        image_path (_type_): Path of the image file.
        
    Returns:
        list: List of extracted text lines.
    """
    try:
        log_message(f"Loading the PaddleOCR model for OCR processing,for language: {detect_lang} and file: {file_path} ")
        ocr = get_ocr_model_for_language(detect_lang)
    except Exception as e:
        log_message(f"Error occured while Loading the PaddleOCR model so loading Default OCR, error: {e}", error_logger=True)
        ocr = chinese_model

    try:
        results = []  

        # Check if file is a PDF first!
        if not file_path.lower().endswith('.pdf'):
            # Not a PDF - process as image normally
            log_message(f"Image file detected, processing normally")
            results = ocr.predict(file_path,
                                    text_det_limit_side_len=960,
                                    text_det_limit_type='max',
                                    text_det_thresh=0.2,
                                    text_det_box_thresh=0.45,
                                    text_det_unclip_ratio=1.6,
                                    text_rec_score_thresh=0.3
                                )
        else:
            # Check if the PDF has a lot of pages
            with pdfplumber.open(file_path) as pdf:
                no_of_pages = len(pdf.pages)
                log_message(f"PDF has {no_of_pages} pages.")
                if no_of_pages <= THRESHOLD_PDF_PAGES_FOR_LARGE_PDF:
                    log_message("Running full OCR on all pages of the PDF...")
                    start_time = datetime.now()
                    results = ocr.predict(file_path,
                                            text_det_limit_side_len=960,
                                            text_det_limit_type='max',
                                            text_det_thresh=0.2,
                                            text_det_box_thresh=0.45,
                                            text_det_unclip_ratio=1.6,
                                            text_rec_score_thresh=0.3
                                        )
                    end_time = datetime.now()
                    log_message(f"OCR processing completed in: {end_time - start_time} seconds")
                else:
                    log_message(f"{file_path} is a large PDF. Running OCR only on the first {MAX_PAGES_TO_PROCESS_FOR_LARGE_PDF} pages...")
                    # Here we would implement logic to extract only first N pages
                    from pdf2image import convert_from_path
                    if vendor_code in CITI_BANK_VENDOR_CODES:
                        images = []
                        v1 = convert_from_path(file_path, first_page=1, last_page=1,dpi=200)
                        v2 = convert_from_path(file_path, first_page=no_of_pages, last_page=no_of_pages,dpi=200)
                        images.extend(v1)
                        images.extend(v2)
 
                    else:
                        images = convert_from_path(file_path, first_page=1, last_page=MAX_PAGES_TO_PROCESS_FOR_LARGE_PDF,dpi=200)
                    log_message(f"No of pages converted to images for OCR: {len(images)} out of {no_of_pages} ")
                    all_results = []
                    start_time = datetime.now()
                    for idx, image in enumerate(images,start=1):
                        ocr_result = ocr.predict(np.array(image),
                                                text_det_limit_side_len=960,
                                                text_det_limit_type='max',
                                                text_det_thresh=0.2,
                                                text_det_box_thresh=0.45,
                                                text_det_unclip_ratio=1.6,
                                                text_rec_score_thresh=0.3
                                            )
                        if ocr_result:
                            all_results.extend(ocr_result)


                    results = all_results
                    log_message(f"OCR processing completed for first {MAX_PAGES_TO_PROCESS_FOR_LARGE_PDF} pages.")
                    end_time = datetime.now()
                    log_message(f"OCR processing completed in: {end_time - start_time} seconds")


    except Exception as e:
        log_message(f"Error during OCR processing: {e}", error_logger=True)
        return [], {}
    
    try:
        log_message("Started Converting result from version-5 to version-4 format")
        converted_result = adapt_paddle_result(results)
    except Exception as e:
        log_message(f"Error during OCR result conversion: {e}", error_logger=True)
        return [], {}
    
    try:
        # TODO: Test/Debug converted result for Voucher checkbox
        if return_checkbox_radio_mappings:
            log_message(f"Extracting checkbox and radiobutton mappings from OCR results")
            start_time = datetime.now()
            checkbox_radiobutton_mappings = extract_form_data(pdf_path=file_path,
                                                              ocr_result=converted_result)
            end_time = datetime.now()
            log_message(f"Checkbox and Radiobutton extraction completed in: {end_time - start_time}")
        else:
            checkbox_radiobutton_mappings = {}
    except Exception as e:
        log_message(f"Error during checkbox and radiobutton extraction: {e}", error_logger=True)
        return [], {}
    
    try:
        text_lines: List = merge_text_with_spaces(converted_result)
    except Exception as e:
        log_message(f"Error during merging text segments: {e}", error_logger=True)
        return [], checkbox_radiobutton_mappings
    
    return text_lines, checkbox_radiobutton_mappings