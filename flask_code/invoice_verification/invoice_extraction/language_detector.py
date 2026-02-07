#!/usr/bin/env python3
"""
Language Detection Module
------------------------
Detects the language of text in images/PDFs before OCR processing.
"""
import tempfile
from typing import List
import time
# from langdetect import detect, LangDetectException
from fast_langdetect import detect
from langdetect.lang_detect_exception import ErrorCode
from invoice_verification.logger.logger import log_message
from invoice_verification.invoice_extraction.ocr.helper import merge_text_with_spaces
from invoice_verification.Parameters.utils import remove_page_markers
from invoice_verification.invoice_extraction.ocr.helper import adapt_paddle_result, merge_text_with_spaces
from invoice_verification.Parameters.utils import remove_page_markers
from invoice_verification.invoice_extraction.paddle_models import chinese_model
import os
import pdfplumber

import os
import multiprocessing
logical_core_count = os.cpu_count() or multiprocessing.cpu_count()
NO_OF_WORKERS = logical_core_count - 1 if logical_core_count and int(logical_core_count) > 1 else 1

# Map of language codes between langdetect and PaddleOCR
# LANG_MAP = {
#          # ========================================
#          # DEDICATED MODELS (10 languages)
#          # ========================================
#          'zh-cn': 'ch',           # Chinese Simplified -> PP-OCRv5_server_rec
#          'zh-tw': 'chinese_cht',  # Chinese Traditional -> PP-OCRv3_mobile_rec
#          'ja': 'japan',           # Japanese -> PP-OCRv5_server_rec
#          'ko': 'korean',          # Korean -> korean_PP-OCRv5_mobile_rec
#          'en': 'en',              # English -> en_PP-OCRv5_mobile_rec
#          'el': 'el',              # Greek -> el_PP-OCRv5_mobile_rec
#          'th': 'th',              # Thai -> th_PP-OCRv5_mobile_rec
#          'ta': 'ta',              # Tamil -> ta_PP-OCRv5_mobile_rec
#          'te': 'te',              # Telugu -> te_PP-OCRv5_mobile_rec
#          'ka': 'ka',              # Georgian -> ka_PP-OCRv3_mobile_rec

#          # ========================================
#          # LATIN SCRIPT LANGUAGES (49 languages)
#          # All use: latin_PP-OCRv5_mobile_rec
#          # ========================================
#          'af': 'af',              # Afrikaans
#          'az': 'az',              # Azerbaijani
#          'bs': 'bs',              # Bosnian
#          'ca': 'ca',              # Catalan
#          'cs': 'cs',              # Czech
#          'cy': 'cy',              # Welsh
#          'da': 'da',              # Danish
#          'de': 'de',              # German
#          'es': 'es',              # Spanish
#          'et': 'et',              # Estonian
#          'eu': 'eu',              # Basque
#          'fi': 'fi',              # Finnish
#          'fr': 'fr',              # French
#          'ga': 'ga',              # Irish
#          'gl': 'gl',              # Galician
#          'hr': 'hr',              # Croatian
#          'hu': 'hu',              # Hungarian
#          'id': 'id',              # Indonesian
#          'is': 'is',              # Icelandic
#          'it': 'it',              # Italian
#          'ku': 'ku',              # Kurdish (Latin script)
#          'la': 'la',              # Latin
#          'lb': 'lb',              # Luxembourgish
#          'lt': 'lt',              # Lithuanian
#          'lv': 'lv',              # Latvian
#          'mi': 'mi',              # Maori
#          'ms': 'ms',              # Malay
#          'mt': 'mt',              # Maltese
#          'nb': 'no',              # Norwegian Bokmål -> Norwegian
#          'nl': 'nl',              # Dutch
#          'nn': 'no',              # Norwegian Nynorsk -> Norwegian
#          'no': 'no',              # Norwegian
#          'oc': 'oc',              # Occitan
#          'pl': 'pl',              # Polish
#          'pt': 'pt',              # Portuguese
#          'qu': 'qu',              # Quechua
#          'rm': 'rm',              # Romansh
#          'ro': 'ro',              # Romanian
#          'sk': 'sk',              # Slovak
#          'sl': 'sl',              # Slovenian
#          'sq': 'sq',              # Albanian
#          'sr': 'rs_latin',        # Serbian (Latin script)
#          'sv': 'sv',              # Swedish
#          'sw': 'sw',              # Swahili
#          'tl': 'tl',              # Tagalog/Filipino
#          'tr': 'tr',              # Turkish
#          'uz': 'uz',              # Uzbek
#          'vi': 'vi',              # Vietnamese

#          # ========================================
#          # ARABIC SCRIPT LANGUAGES (6 languages)
#          # All use: arabic_PP-OCRv5_mobile_rec
#          # ========================================
#          'ar': 'ar',              # Arabic
#          'fa': 'fa',              # Persian/Farsi
#          'ur': 'ur',              # Urdu
#          'ps': 'ps',              # Pashto
#          'sd': 'sd',              # Sindhi
#          'ug': 'ug',              # Uyghur

#          # ========================================
#          # EAST SLAVIC LANGUAGES (3 languages)
#          # All use: eslav_PP-OCRv5_mobile_rec
#          # ========================================
#          'ru': 'ru',              # Russian
#          'uk': 'uk',              # Ukrainian
#          'be': 'be',              # Belarusian

#          # ========================================
#          # CYRILLIC SCRIPT LANGUAGES (7 languages)
#          # All use: cyrillic_PP-OCRv5_mobile_rec
#          # ========================================
#          'bg': 'bg',              # Bulgarian
#          'kk': 'kk',              # Kazakh
#          'ky': 'ky',              # Kyrgyz
#          'mk': 'mk',              # Macedonian
#          'mn': 'mn',              # Mongolian
#          'tg': 'tg',              # Tajik
#          'tt': 'tt',              # Tatar

#          # ========================================
#          # DEVANAGARI SCRIPT LANGUAGES (5 languages)
#          # All use: devanagari_PP-OCRv5_mobile_rec
#          # ========================================
#          'hi': 'hi',              # Hindi
#          'mr': 'mr',              # Marathi
#          'ne': 'ne',              # Nepali
#          'bn': 'ne',              # Bengali -> use Nepali (closest Devanagari)
#          'sa': 'sa',              # Sanskrit
#      }


#     'en': 'en',       # English
#     'de': 'german',   # German
#     'fr': 'fr',       # French
#     'zh': 'ch',       # Chinese
#     'ja': 'japan',    # Japanese
#     'ko': 'korean',   # Korean
#     'ru': 'ru',       # Russian
#     'es': 'es',       # Spanish
#     'pt': 'pt',       # Portuguese
#     'it': 'it',       # Italian
#     'ar': 'ar',       # Arabic
#     'hi': 'hi',       # Hindi
#     'th': 'th',       # Thai
#     'vi': 'vi',       # Vietnamese
#     'nl': 'nl',       # Dutch
#     'sv': 'sv',       # Swedish
#     'fi': 'fi',       # Finnish
#     'no': 'no',       # Norwegian
#     'da': 'da',       # Danish
#     'cs': 'cs',       # Czech
#     'pl': 'pl',       # Polish
#     'hu': 'hu',       # Hungarian
#     'tr': 'tr',       # Turkish
#     'el': 'el',       # Greek
#     'bg': 'bg',       # Bulgarian
#     'ro': 'ro',       # Romanian
#     'uk': 'uk',       # Ukrainian
#     'fa': 'fa',       # Persian
#     'id': 'id',       # Indonesian
#     'ms': 'ms',       # Malay
#     'ta': 'ta',       # Tamil
#     'te': 'te',       # Telugu
#     'ur': 'ur',       # Urdu
#     'bn': 'bn',       # Bengali
# }

# Default language to use if detection fails
DEFAULT_LANGUAGE = 'ch'

# Languages supported by PaddleOCR
# SUPPORTED_LANGUAGES = {
#     'ch', 'chinese_cht', 'japan', 'korean', 'en', 'el', 'th', 'ta', 'te', 'ka',
#     'af', 'az', 'bs', 'ca', 'cs', 'cy', 'da', 'de', 'es', 'et', 'eu', 'fi',
#     'fr', 'ga', 'gl', 'hr', 'hu', 'id', 'is', 'it', 'ku', 'la', 'lb', 'lt',
#     'lv', 'mi', 'ms', 'mt', 'no', 'nl', 'oc', 'pl', 'pt', 'qu', 'rm', 'ro',
#     'sk', 'sl', 'sq', 'rs_latin', 'sv', 'sw', 'tl', 'tr', 'uz', 'vi',
#     'ar', 'fa',  'ur',  'ps',  'sd',  'ug',
#     'ru',  'uk',  'be',
#     'bg',  'kk',  'ky',  'mk',  'mn',  'tg',  'tt',
#     'hi',  'mr',  'ne',  'sa'
#     # Note: Bengali ('bn') uses Nepali model
# }

LANG_MAP = {
    # ========================================
    # DEDICATED MODELS (10 languages)
    # ========================================
    'zh': 'ch',              # Chinese -> PP-OCRv5_server_rec (handles both simplified & traditional)
    'ja': 'japan',           # Japanese -> PP-OCRv5_server_rec
    'ko': 'korean',          # Korean -> korean_PP-OCRv5_mobile_rec
    'en': 'en',              # English -> en_PP-OCRv5_mobile_rec
    'el': 'el',              # Greek -> el_PP-OCRv5_mobile_rec
    'th': 'th',              # Thai -> th_PP-OCRv5_mobile_rec
    'ta': 'ta',              # Tamil -> ta_PP-OCRv5_mobile_rec
    'te': 'te',              # Telugu -> te_PP-OCRv5_mobile_rec
    'ka': 'ka',              # Georgian -> ka_PP-OCRv3_mobile_rec

    # ========================================
    # LATIN SCRIPT LANGUAGES (49 languages)
    # All use: latin_PP-OCRv5_mobile_rec
    # ========================================
    'af': 'af', 'az': 'az', 'bs': 'bs', 'ca': 'ca', 'cs': 'cs',
    'cy': 'cy', 'da': 'da', 'de': 'de', 'es': 'es', 'et': 'et',
    'eu': 'eu', 'fi': 'fi', 'fr': 'fr', 'ga': 'ga', 'gl': 'gl',
    'hr': 'hr', 'hu': 'hu', 'id': 'id', 'is': 'is', 'it': 'it',
    'ku': 'ku', 'la': 'la', 'lb': 'lb', 'lt': 'lt', 'lv': 'lv',
    'mi': 'mi', 'ms': 'ms', 'mt': 'mt', 'nl': 'nl', 'no': 'no',
    'oc': 'oc', 'pl': 'pl', 'pt': 'pt', 'qu': 'qu', 'rm': 'rm',
    'ro': 'ro', 'sk': 'sk', 'sl': 'sl', 'sq': 'sq', 'sr': 'rs_latin',
    'sv': 'sv', 'sw': 'sw', 'tl': 'tl', 'tr': 'tr', 'uz': 'uz', 'vi': 'vi',

    # ========================================
    # ARABIC SCRIPT LANGUAGES (6 languages)
    # All use: arabic_PP-OCRv5_mobile_rec
    # ========================================
    'ar': 'ar', 'fa': 'fa', 'ur': 'ur', 'ps': 'ps', 'sd': 'sd', 'ug': 'ug',

    # ========================================
    # EAST SLAVIC LANGUAGES (3 languages)
    # All use: eslav_PP-OCRv5_mobile_rec
    # ========================================
    'ru': 'ru', 'uk': 'uk', 'be': 'be',

    # ========================================
    # CYRILLIC SCRIPT LANGUAGES (7 languages)
    # All use: cyrillic_PP-OCRv5_mobile_rec
    # ========================================
    'bg': 'bg', 'kk': 'kk', 'ky': 'ky', 'mk': 'mk',
    'mn': 'mn', 'tg': 'tg', 'tt': 'tt',

    # ========================================
    # DEVANAGARI SCRIPT LANGUAGES (5 languages)
    # All use: devanagari_PP-OCRv5_mobile_rec
    # ========================================
    'hi': 'hi', 'mr': 'mr', 'ne': 'ne', 'bn': 'ne', 'sa': 'sa',
}

SUPPORTED_LANGUAGES = set(LANG_MAP.values())

# SUPPORTED_LANGUAGES = {
#     'en',        # English
#     'ch',        # Chinese
#     'german',    # German
#     'fr',        # French
#     'japan',     # Japanese
#     'korean',    # Korean
#     'ru',        # Russian
#     'es',        # Spanish
#     'pt',        # Portuguese
#     'it',        # Italian
#     'ar',        # Arabic
#     'hi',        # Hindi
#     'th',        # Thai
#     'vi',        # Vietnamese
#     'nl',        # Dutch
#     'sv',        # Swedish
#     'fi',        # Finnish
#     'no',        # Norwegian
#     'da',        # Danish
#     'cs',        # Czech
#     'pl',        # Polish
#     'hu',        # Hungarian
#     'tr',        # Turkish
#     'el',        # Greek
#     'bg',        # Bulgarian
#     'ro',        # Romanian
#     'uk',        # Ukrainian
#     'fa',        # Persian
#     'id',        # Indonesian
#     'ms',        # Malay
#     'ta',        # Tamil
#     'te',        # Telugu
#     'ur',        # Urdu
#     'bn',        # Bengali
# }

def get_paddle_language(detected_lang: str) -> str:
    """
    Convert langdetect language code to PaddleOCR language code
    
    Args:
        detected_lang (str): Language code from langdetect
        
    Returns:
        str: Corresponding PaddleOCR language code or default language
    """
    return LANG_MAP.get(detected_lang, DEFAULT_LANGUAGE)

def detect_language_from_text(text: str) -> str:
    """
    Detect language from text using langdetect
    
    Args:
        text (str): Text to detect language from
        
    Returns:
        str: PaddleOCR language code
    """
    log_message("LANGUAGE DETECTION: Analyzing text for language...")
    
    if not text or len(text.strip()) < 10:
        log_message("WARNING: Text too short for reliable language detection, using default language")
        log_message("Text too short for reliable language detection")
        return DEFAULT_LANGUAGE
    
    try:
        # Detect language
        fast_lang_detector = detect(text)[0]
        detected_lang = fast_lang_detector.get('lang', DEFAULT_LANGUAGE)
        log_message(f"LANGUAGE DETECTION: Raw detected language code: {detected_lang}")
        log_message(f"Detected language: {detected_lang}")
        
        # Map to PaddleOCR language code
        paddle_lang = get_paddle_language(detected_lang)
        log_message(f"LANGUAGE DETECTION: Mapped to PaddleOCR language: {paddle_lang}")
        
        # Check if language is supported by PaddleOCR
        if paddle_lang not in SUPPORTED_LANGUAGES:
            log_message(f"WARNING: Detected language {detected_lang} not supported by PaddleOCR, using {DEFAULT_LANGUAGE}")
            log_message(f"Detected language {detected_lang} not supported by PaddleOCR, using {DEFAULT_LANGUAGE}")
            return DEFAULT_LANGUAGE
            
        return paddle_lang
        
    except Exception as e:
        log_message(f"ERROR: Language detection failed: {str(e)}")
        log_message(f"Language detection error: {str(e)}")
        return DEFAULT_LANGUAGE

# update the code to handle multi page PDFs by extracting text from the first page, and use pdf2image to convert to image for OCR
def perform_initial_ocr(image_path: str) -> str:
    """
    Perform initial OCR with default language to get text for language detection
    
    Args:
        image_path (str): Path to the image file
        
    Returns:
        str: Extracted text for language detection
    """
    log_message("LANGUAGE DETECTION: Performing initial OCR scan for language detection...")
    log_message(f"Performing initial OCR on {image_path} for language detection")
    
    try:
        # Initialize PaddleOCR with minimal settings for quick processing
        log_message("LANGUAGE DETECTION: Initializing PaddleOCR with Chinese as default language...")
        
        # Check if its ending with .pdf
        if image_path.lower().endswith('.pdf'):
            
            with pdfplumber.open(image_path) as pdf:
                no_of_pages = len(pdf.pages)
                log_message(f"LANGUAGE DETECTION: PDF has {no_of_pages} pages")

                extracted_text = ""
                MIN_TEXT_THRESHOLD = 100

                # Loop through pages to extract sufficient text

                from pdf2image import convert_from_path
                max_pages_to_check = min(no_of_pages, 2)  # Check up to first 2 pages
                images = convert_from_path(image_path, first_page=1, last_page=max_pages_to_check, dpi=200)

                for page_num in range(no_of_pages):
                    # page = pdf.pages[page_num]
                    # page_text = page.extract_text()

                    # if page_text:
                    #     extracted_text += " " + page_text.strip()
                    #     log_message(f"LANGUAGE DETECTION: Extracted text from page {page_num + 1}")

                    # # Check if we have enough text (excluding spaces)
                    # if len(extracted_text.replace(" ", "")) >= MIN_TEXT_THRESHOLD:
                    #     log_message(f"LANGUAGE DETECTION: Sufficient text extracted from {page_num + 1} page(s)")
                    #     log_message(f"Extracted text length: {extracted_text.strip()}")
                    #     return extracted_text.strip()

                # If text extraction from all pages didn't meet threshold, try OCR on first page
                    if len(extracted_text.replace(" ", "")) < MIN_TEXT_THRESHOLD:
                        log_message("LANGUAGE DETECTION: Insufficient text from PDF pages, proceeding with OCR on first page image")

                        if not images:
                            log_message("Failed to convert first page of PDF to image for OCR")
                            return ""
                        
                        # Save the first page image to a temporary file
                        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_image_file:
                            temp_image_path = temp_image_file.name
                            images[page_num].save(temp_image_path, 'PNG')
                            log_message(f"LANGUAGE DETECTION: Converted {page_num + 1} page of PDF to image for OCR, {temp_image_path}")
                        
                        # Perform OCR
                        log_message("LANGUAGE DETECTION: Running initial OCR scan...")
                        start_time = time.time()

                        results = chinese_model.predict(
                                temp_image_path,
                                text_det_limit_side_len=960,
                                text_det_limit_type='max',
                                text_det_thresh=0.2,
                                text_det_box_thresh=0.45,
                                text_det_unclip_ratio=1.6,
                                text_rec_score_thresh=0.3
                            )
                        processing_time = time.time() - start_time
                        log_message(f"LANGUAGE DETECTION: Initial OCR completed in {processing_time:.2f} seconds")
                        
                        # Clean up temporary image file
                        try:
                            os.remove(temp_image_path)
                        except Exception as e:
                            pass

                        extracted = ""
                        if results and len(results) > 0:
                            adapted_result = adapt_paddle_result(results)
                            merged_text = merge_text_with_spaces(adapted_result)
                            extracted = remove_page_markers(merged_text)
                            log_message("OCR Result:\n" + "\n".join(extracted))

                        # Extract text from results
                        ocr_text = " ".join(extracted) if results and len(results) > 0 else ""
                        
                        # Check if OCR text meets threshold
                        if len(ocr_text.replace(" ", "")) >= MIN_TEXT_THRESHOLD:
                            return ocr_text.strip()
                        
                        # If still not enough text, return empty string
                        log_message(f"LANGUAGE DETECTION: Text length ({len(ocr_text.replace(' ', ''))}) below threshold ({MIN_TEXT_THRESHOLD}), returning empty")
                        continue

                return extracted_text.strip()
        else:
            # For non-PDF files, perform OCR directly
            log_message("LANGUAGE DETECTION: Running initial OCR scan on image file...")
            start_time = time.time()

            results = chinese_model.predict(
                        image_path,
                        text_det_limit_side_len=960,
                        text_det_limit_type='max',
                        text_det_thresh=0.2,
                        text_det_box_thresh=0.45,
                        text_det_unclip_ratio=1.6,
                        text_rec_score_thresh=0.3
                    )
            processing_time = time.time() - start_time
            log_message(f"LANGUAGE DETECTION: Initial OCR completed in {processing_time:.2f} seconds")
            log_message(f"Initial OCR completed in {processing_time:.2f} seconds")
        
            extracted = ""
            if results and len(results) > 0:
                adapted_result = adapt_paddle_result(results)
                merged_text = merge_text_with_spaces(adapted_result)
                extracted = remove_page_markers(merged_text)
                log_message("OCR Result:\n" + "\n".join(extracted))

            # Extract text from results
            extracted_text = " ".join(extracted) if results and len(results) > 0 else ""
            text_count = len(extracted_text) if results and len(results) > 0 else 0
        # text_count = 0
        # if results:
        #     for sublist in results:
        #         if not sublist:
        #             continue
        #         for item in sublist:
        #             text = item[1][0]  # Text content
        #             # confidence = item[1][1]  # Confidence score
        #             extracted_text += text + " "
        #             text_count += 1

        log_message(f"LANGUAGE DETECTION: OCR extracted {extracted_text.strip()}")
        
        log_message(f"LANGUAGE DETECTION: Extracted {text_count} text segments from image")
        if text_count == 0:
            log_message("LANGUAGE DETECTION: No text was extracted from the image!")
        
        return extracted_text.strip()
    
    except Exception as e:
        log_message(f"ERROR in initial OCR for file {image_path}: {str(e)},",error_logger=True)
        import traceback
        log_message(traceback.format_exc(), error_logger=True)
        return ""

def detect_language_from_image(image_path: str) -> str:
    """
    Detect language from an image by performing initial OCR and analyzing text
    
    Args:
        image_path (str): Path to the image file
        
    Returns:
        str: Detected language code for PaddleOCR
    """
    log_message("\n" + "=" * 50)
    log_message(f"LANGUAGE DETECTION: Starting language detection for {image_path}")
    log_message("=" * 50)
    
    # Check if file exists
    if not os.path.exists(image_path):
        log_message(f"ERROR: Image file not found: {image_path}")
        log_message(f"Image file not found: {image_path}")
        return DEFAULT_LANGUAGE
    
    # Perform initial OCR to get text
    log_message("LANGUAGE DETECTION: Performing initial OCR scan...")
    extracted_text = perform_initial_ocr(image_path)
    
    if not extracted_text:
        log_message(f"WARNING: No text extracted from {image_path}, using default language: {DEFAULT_LANGUAGE}")
        log_message(f"No text extracted from {image_path}, using default language")
        return DEFAULT_LANGUAGE
    
    log_message(f"LANGUAGE DETECTION: Extracted text sample: '{extracted_text[:100]}...'" if len(extracted_text) > 100 else f"LANGUAGE DETECTION: Extracted text: '{extracted_text}'")
    
    # Detect language from extracted text
    detected_lang = detect_language_from_text(extracted_text)
    log_message(f"LANGUAGE DETECTION: Detected language: {detected_lang}")
    log_message("=" * 50 + "\n")
    
    return detected_lang

def detect_language_from_sample(sample_text: List[str]) -> str:
    """
    Detect language from a sample of text lines
    
    Args:
        sample_text (List[str]): List of text lines
        
    Returns:
        str: Detected language code for PaddleOCR
    """
    if not sample_text:
        return DEFAULT_LANGUAGE
    
    # Combine text lines
    combined_text = " ".join(sample_text)
    
    # Detect language
    return detect_language_from_text(combined_text)
