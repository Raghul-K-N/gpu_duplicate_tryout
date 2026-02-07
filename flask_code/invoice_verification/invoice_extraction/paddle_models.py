from paddleocr import PaddleOCR
import os
from dotenv import load_dotenv
load_dotenv()
models_directory = os.getenv("PADDLE_OCR_MODELS_PATH", "paddle_models/")

# Store models as None initially for lazy initialization
_chinese_model = None
_latin_model = None
_arabic_model = None
_korean_model = None
_enslavic_model = None
_cyrillic_model = None
_devangiri_model = None
_th_model = None
_te_model = None
_ta_model = None
_el_model = None

def _get_chinese_model():
    global _chinese_model
    if _chinese_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('PP-OCRv5_server_rec', '{models_directory}/PP-OCRv5_server_rec')")
        _chinese_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_recognition_model_name="PP-OCRv5_server_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_dir=f"{models_directory}/PP-OCRv5_server_rec",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _chinese_model

class _LazyModel:
    def __init__(self, init_func):
        self._init_func = init_func
        self._model = None
    
    def predict(self, *args, **kwargs):
        if self._model is None:
            self._model = self._init_func()
        return self._model.predict(*args, **kwargs)
    
    def __getattr__(self, name):
        if self._model is None:
            self._model = self._init_func()
        return getattr(self._model, name)

chinese_model = _LazyModel(_get_chinese_model)

def _get_latin_model():
    global _latin_model
    if _latin_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('latin_PP-OCRv5_mobile_rec', '{models_directory}/latin_PP-OCRv5_mobile_rec')")
        _latin_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="latin_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/latin_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _latin_model

latin_model = _LazyModel(_get_latin_model)

def _get_arabic_model():
    global _arabic_model
    if _arabic_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('arabic_PP-OCRv5_mobile_rec', '{models_directory}/arabic_PP-OCRv5_mobile_rec')")
        _arabic_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="arabic_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/arabic_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _arabic_model

arabic_model = _LazyModel(_get_arabic_model)

def _get_korean_model():
    global _korean_model
    if _korean_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('korean_PP-OCRv5_mobile_rec', '{models_directory}/korean_PP-OCRv5_mobile_rec')")
        _korean_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="korean_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/korean_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _korean_model

korean_model = _LazyModel(_get_korean_model)

def _get_enslavic_model():
    global _enslavic_model
    if _enslavic_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('eslav_PP-OCRv5_mobile_rec', '{models_directory}/eslav_PP-OCRv5_mobile_rec')")
        _enslavic_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="eslav_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/eslav_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _enslavic_model

enslavic_model = _LazyModel(_get_enslavic_model)

def _get_cyrillic_model():
    global _cyrillic_model
    if _cyrillic_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('cyrillic_PP-OCRv5_mobile_rec', '{models_directory}/cyrillic_PP-OCRv5_mobile_rec')")
        _cyrillic_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="cyrillic_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/cyrillic_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _cyrillic_model

cyrillic_model = _LazyModel(_get_cyrillic_model)

def _get_devangiri_model():
    global _devangiri_model
    if _devangiri_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('devanagari_PP-OCRv5_mobile_rec', '{models_directory}/devanagari_PP-OCRv5_mobile_rec')")
        _devangiri_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="devanagari_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/devanagari_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _devangiri_model

devangiri_model = _LazyModel(_get_devangiri_model)

def _get_th_model():
    global _th_model
    if _th_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('th_PP-OCRv5_mobile_rec', '{models_directory}/th_PP-OCRv5_mobile_rec')")
        _th_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="th_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/th_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _th_model

th_model = _LazyModel(_get_th_model)

def _get_te_model():
    global _te_model
    if _te_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('te_PP-OCRv5_mobile_rec', '{models_directory}/te_PP-OCRv5_mobile_rec')")
        _te_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="te_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/te_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _te_model

te_model = _LazyModel(_get_te_model)

def _get_ta_model():
    global _ta_model
    if _ta_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('ta_PP-OCRv5_mobile_rec', '{models_directory}/ta_PP-OCRv5_mobile_rec')")
        _ta_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="ta_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/ta_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _ta_model

ta_model = _LazyModel(_get_ta_model)

def _get_el_model():
    global _el_model
    if _el_model is None:
        print(f"Creating model: ('PP-LCNet_x1_0_textline_ori', '{models_directory}/PP-LCNet_x1_0_textline_ori')")
        print(f"Creating model: ('PP-OCRv5_server_det', '{models_directory}/PP-OCRv5_server_det')")
        print(f"Creating model: ('el_PP-OCRv5_mobile_rec', '{models_directory}/el_PP-OCRv5_mobile_rec')")
        _el_model = PaddleOCR(
            text_det_limit_side_len=960,
            text_det_limit_type='max',
            text_det_thresh=0.2,
            text_det_box_thresh=0.45,
            text_det_unclip_ratio=1.6,
            text_rec_score_thresh=0.3,
            text_recognition_batch_size=6,
            text_detection_model_name="PP-OCRv5_server_det",
            text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
            text_recognition_model_name="el_PP-OCRv5_mobile_rec",
            text_recognition_model_dir=f"{models_directory}/el_PP-OCRv5_mobile_rec",
            textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
            textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=True,
            enable_mkldnn=True,
            device='cpu'
        )
    return _el_model

el_model = _LazyModel(_get_el_model)












































































# chinese_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_recognition_model_name="PP-OCRv5_server_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_dir=f"{models_directory}/PP-OCRv5_server_rec",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# latin_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="latin_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/latin_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# arabic_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="arabic_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/arabic_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# korean_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="korean_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/korean_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# enslavic_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="eslav_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/eslav_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# cyrillic_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="cyrillic_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/cyrillic_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# devangiri_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="devanagari_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/devanagari_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# th_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="th_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/th_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# te_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="te_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/te_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# ta_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="ta_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/ta_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )

# el_model = PaddleOCR(
#             text_det_limit_side_len=960,
#             text_det_limit_type='max',
#             text_det_thresh=0.2,
#             text_det_box_thresh=0.45,
#             text_det_unclip_ratio=1.6,
#             text_rec_score_thresh=0.3,
#             text_recognition_batch_size=6,
#             text_detection_model_name="PP-OCRv5_server_det",
#             text_detection_model_dir=f"{models_directory}/PP-OCRv5_server_det",
#             text_recognition_model_name="el_PP-OCRv5_mobile_rec",
#             text_recognition_model_dir=f"{models_directory}/el_PP-OCRv5_mobile_rec",
#             textline_orientation_model_name="PP-LCNet_x1_0_textline_ori",
#             textline_orientation_model_dir=f"{models_directory}/PP-LCNet_x1_0_textline_ori",
#             use_doc_orientation_classify=False,
#             use_doc_unwarping=False,
#             use_textline_orientation=True,
#             enable_mkldnn=True,
#             device='cpu'
#         )


