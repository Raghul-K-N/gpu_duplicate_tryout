import numpy as np
import pandas as pd

from invoice_number_similarity.processing.data_management import load_pipeline, load_features
from invoice_number_similarity.config import config, logging_config
from invoice_number_similarity import __version__ as _version
from invoice_number_similarity.processing.features import extract_features
from invoice_number_similarity.rule_based_model import rule_based_similarity

import logging
import typing as t
import random


_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)

pipeline_file_name = f"{config.PIPELINE_SAVE_FILE}.pkl"
_pipe = load_pipeline(file_name=pipeline_file_name)
features_dict = load_features(file_name=config.FEATURES_FILE)
features = features_dict['categorical'] + features_dict['numerical']


def make_prediction(*, input_data: t.Union[str, str], model:str='ML') -> dict:
    """Make a prediction using a saved model pipeline.

    Args:
        input_data: Array of model prediction inputs.
        model: The model to use: RULE_BASED or ML

    Returns:
        Predictions for each input row, as well as the model version.
    """

    if input_data[0] == input_data[1] or input_data[0] in input_data[1] or input_data[1] in input_data[0]:
        results = {"predictions": [0,1], "similarity_score": 100, "version": _version}
        return results

    if model=='RULE_BASED':
        prediction, similarity_score = rule_based_similarity(input_data[0], input_data[1])
        return {"predictions": [1-prediction,prediction], "similarity_score": similarity_score, "version": _version}

    data_features = extract_features(input_data[0], input_data[1])
    similarity_score = data_features['score']
    # # data_features['ratio_src_dest'] = data_features['length_src']/data_features['length_dest']
    # new_features = data_features.copy()
    add_features = dict()
    remove_features = []
    
    for k,v in data_features.items():
        if type(v) == str:
            add_features[k+'_'+v] = 1
        if k not in features:
            remove_features.append(k)

   # data_features = data_features | add_features
    data_features.update(add_features)
    data_features = dict([(key, val) for key, val in 
           data_features.items() if key not in remove_features])

    for col in features:
        if col not in data_features.keys():
            data_features[col] = -1

    data = np.array([data_features[f] for f in features])

    prediction = _pipe.predict_proba(data,thread_count=12)

    output = prediction # [[prediction, 1-prediction]] #np.exp(prediction)

    results = {"predictions": output, "similarity_score": similarity_score, "version": _version}

    # _logger.info(
    #     f"Making predictions with model version: {_version} "
    #     f"Inputs: {validated_data} "
    #     f"Predictions: {results}"
    # )

    return results
