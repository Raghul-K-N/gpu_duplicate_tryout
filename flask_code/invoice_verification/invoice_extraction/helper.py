from invoice_verification.invoice_extraction.paddle_models import chinese_model, latin_model, cyrillic_model, \
    arabic_model, devangiri_model, korean_model, th_model, el_model, te_model, ta_model, enslavic_model
from paddleocr import PaddleOCR
import pdfplumber
import os

LATIN_LANGS = [
    "af",
    "az",
    "bs",
    "cs",
    "cy",
    "da",
    "de",
    "es",
    "et",
    "fr",
    "ga",
    "hr",
    "hu",
    "id",
    "is",
    "it",
    "ku",
    "la",
    "lt",
    "lv",
    "mi",
    "ms",
    "mt",
    "nl",
    "no",
    "oc",
    "pi",
    "pl",
    "pt",
    "ro",
    "rs_latin",
    "sk",
    "sl",
    "sq",
    "sv",
    "sw",
    "tl",
    "tr",
    "uz",
    "vi",
    "french",
    "german",
    "fi",
    "eu",
    "gl",
    "lb",
    "rm",
    "ca",
    "qu",
]
ARABIC_LANGS = ["ar", "fa", "ug", "ur", "ps", "ku", "sd", "bal"]
ESLAV_LANGS = ["ru", "be", "uk"]
CYRILLIC_LANGS = [
    "ru",
    "rs_cyrillic",
    "be",
    "bg",
    "uk",
    "mn",
    "abq",
    "ady",
    "kbd",
    "ava",
    "dar",
    "inh",
    "che",
    "lbe",
    "lez",
    "tab",
    "kk",
    "ky",
    "tg",
    "mk",
    "tt",
    "cv",
    "ba",
    "mhr",
    "mo",
    "udm",
    "kv",
    "os",
    "bua",
    "xal",
    "tyv",
    "sah",
    "kaa",
]
DEVANAGARI_LANGS = [
    "hi",
    "mr",
    "ne",
    "bh",
    "mai",
    "ang",
    "bho",
    "mah",
    "sck",
    "new",
    "gom",
    "sa",
    "bgc",
]

def get_ocr_model_for_language(detected_lang: str):
    """
    Get the PaddleOCR directory based on the detected language.

    Args:
        detected_lang (str): The detected language code.

    Returns:
        PaddleOCR: The corresponding PaddleOCR model.
    """
    rec_model = chinese_model
    if detected_lang in ("ch", "chinese_cht", "japan"):
        rec_model = chinese_model
    elif detected_lang == "en":
        rec_model = latin_model
    elif detected_lang in LATIN_LANGS:
        rec_model = latin_model
    elif detected_lang in ESLAV_LANGS:
        rec_model = enslavic_model
    elif detected_lang in ARABIC_LANGS:
        rec_model = arabic_model
    elif detected_lang in CYRILLIC_LANGS:
        rec_model = cyrillic_model
    elif detected_lang in DEVANAGARI_LANGS:
        rec_model = devangiri_model
    elif detected_lang == "korean":
        rec_model = korean_model
    elif detected_lang == "th":
        rec_model = th_model
    elif detected_lang == "el":
        rec_model = el_model
    elif detected_lang == "te":
        rec_model = te_model
    elif detected_lang == "ta":
        rec_model = ta_model

    return rec_model


def is_scanned_pdf(path):
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text and text.strip():
                return False  
            else:
                return False
        return True             # No text found on any page


def check_file_type(file_name):
    """
    Check if the file is a PDF or an image.
    
    Args:
        file_name (str): The name of the file to check.
    
    Returns:
        bool: True if the file is a Image, False otherwise.
    """
    # Extract the file extension
    _, ext = os.path.splitext(file_name)
    # Check the extension
    if ext.lower() == '.pdf':
        # Check if the PDF is scanned
        if is_scanned_pdf(file_name):
            # log_message(f"File {file_name} is a scanned PDF.")
            return True
        else:
            return False
    else:
        return ext.strip().lower() in ['.jpg', '.png', '.bmp']