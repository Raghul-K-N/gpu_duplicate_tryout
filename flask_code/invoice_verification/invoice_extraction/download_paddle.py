from paddleocr import PaddleOCR
import os

cache_dir = os.environ.get("PADDLE_PDX_CACHE_HOME",
                        os.path.join(os.path.expanduser("~"), ".paddlex"))

print(f"PaddleOCR cache directory: {cache_dir}")

languages = ["ch", "korean", "ar", "german", "ru", "rs_cyrillic","hi", "th", "el", "te", "ta"]

for lang in languages:

    ocr = PaddleOCR(
        # Language and version
        lang=lang,
        # lang='ch',
        ocr_version='PP-OCRv5',

        # CRITICAL: Text Detection Parameters to prevent edge cutting
        text_det_limit_side_len=960,  # Use 'max' type, so this limits maximum side
        text_det_limit_type='max',  # CHANGED: 'max' prevents upscaling, preserves original
        text_det_thresh=0.2,  # LOWER = more sensitive (was 0.3)
        text_det_box_thresh=0.45,  # LOWER = accept more edge boxes (was 0.6)
        text_det_unclip_ratio=1.6,  # HIGHER expansion for edge text (was 1.5)

        # Text Recognition Parameters  
        text_rec_score_thresh=0.3,
        text_recognition_batch_size=6,

        # Preprocessing (recommended for documents)
        use_doc_orientation_classify=False,  # Disable if not needed (adds processing)
        use_doc_unwarping=False,  # Disable if images are flat
        use_textline_orientation=True,  # Keep for rotated text

        # Performance
        enable_mkldnn=True,
        device='cpu'
    )
    print(f"OCR initialized for language: {lang}")

print(f"All PaddleOCR models initialized in {cache_dir}.")
print("Please Copy the --- Official Models Directory --- from above path to the Deployment Environment to avoid downloading models at runtime.")