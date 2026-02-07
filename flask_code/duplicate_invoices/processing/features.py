from duplicate_invoices.config import logging_config, config
import logging
from rapidfuzz.distance import Levenshtein
import pandas as pd

_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)
