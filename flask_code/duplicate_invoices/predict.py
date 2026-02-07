import numpy as np
import pandas as pd

from duplicate_invoices.processing.data_management import load_pipeline
from duplicate_invoices.config import config, logging_config
from duplicate_invoices.processing.validation import validate_inputs
from duplicate_invoices import __version__ as _version
from duplicate_invoices.pipeline import pipe
from code1.logger import  capture_log_message
import logging
import typing as t


_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)

# capture_log_message(log_message='Reading Pipeline PKL File')
# pipeline_file_name = f"{config.PIPELINE_SAVE_FILE}{_version}.pkl"
# _pipe = load_pipeline(file_name=pipeline_file_name)
INITIAL_FIT_DONE = False

def make_prediction(*, input_data: pd.DataFrame,
                    ) -> pd.DataFrame:
    """Make a prediction using a saved model pipeline.

    Args:
        input_data: Array of model prediction inputs.

    Returns:
        Predictions for each input row, as well as the model version.
    """
    
    # addded code to avoid the error of not having the model fitted
    global INITIAL_FIT_DONE
    validated_data = input_data #validate_inputs(input_data=data)
    # capture_log_message(log_message='Inside Make Prediction')
    if not INITIAL_FIT_DONE:
        dummy_X = pd.DataFrame(columns=validated_data.columns)
        pipe.fit(dummy_X)
        INITIAL_FIT_DONE = True
    output = pipe.predict(validated_data)

    results = {"output": output, "version": _version}

    _logger.info(
        f"Making predictions with model version: {_version} "
    )

    return results
