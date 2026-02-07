import numpy as np
import pandas as pd
from invoice_number_similarity.config import config, logging_config
from invoice_number_similarity import __version__ as _version
import pickle
import joblib
from sklearn.pipeline import Pipeline
import typing as t
import logging
import json


_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)

def load_data(*, file_name: str) -> np.array:
    _df = pd.read_csv(f"{config.DATASET_DIR}/{file_name}", index_col=0)
    _logger.info(f"Loaded Dataset from: {config.DATASET_DIR}/{file_name}")
    return _df

def load_features(*, file_name: str) -> np.array:
    with open(f"{config.DATASET_DIR}/{file_name}", 'r') as f:
        _features = json.load(f)
    # _logger.info(f"Loaded Features from: {config.DATASET_DIR}/{file_name}")
    return _features


def load_model(*, file_name: str):
    with open(f"{config.EXTERNAL_MODEL_DIR}/{file_name}", 'rb') as f:
        model = pickle.load(f)
    clf = model['clf']
    columns = list(model['columns'])
    scaler = model['scaler']

    return clf, columns, scaler


def load_dataset(*, file_name: str) -> pd.DataFrame:
    _data = pd.read_csv(f"{config.DATASET_DIR}/{file_name}")
    return _data


def save_pipeline(*, pipeline_to_persist) -> None:
    """Persist the pipeline.
    Saves the versioned model, and overwrites any previous
    saved models. This ensures that when the package is
    published, there is only one trained model that can be
    called, and we know exactly how it was built.
    """

    # Prepare versioned save file name
    save_file_name = f"{config.PIPELINE_SAVE_FILE}{_version}.pkl"
    save_path = config.TRAINED_MODEL_DIR / save_file_name

    remove_old_pipelines(files_to_keep=[save_file_name])
    joblib.dump(pipeline_to_persist, save_path)
    _logger.info(f"Saved Pipeline: {save_file_name}")


def load_pipeline(*, file_name: str) -> Pipeline:
    """Load a persisted pipeline."""

    file_path = config.TRAINED_MODEL_DIR / file_name
    trained_model = joblib.load(filename=file_path)
    return trained_model


def remove_old_pipelines(*, files_to_keep: t.List[str]) -> None:
    """
    Remove old model pipelines.

    This is to ensure there is a simple one-to-one
    mapping between the package version and the model
    version to be imported and used by other applications.
    However, we do also include the immediate previous
    pipeline version for differential testing purposes.
    """
    do_not_delete = files_to_keep + ['__init__.py']
    for model_file in config.TRAINED_MODEL_DIR.iterdir():
        if model_file.name not in do_not_delete:
            model_file.unlink()