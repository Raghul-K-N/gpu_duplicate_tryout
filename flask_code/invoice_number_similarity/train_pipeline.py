import numpy as np
from sklearn.model_selection import train_test_split

from invoice_number_similarity import pipeline
from invoice_number_similarity.processing.data_management import load_dataset, save_pipeline, load_features
from invoice_number_similarity.config import config, logging_config
from invoice_number_similarity import __version__ as _version

import json
import logging


_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)


def run_training() -> None:
    """Train the model."""

    # read training data
    data = load_dataset(file_name=config.TRAINING_DATA_FILE)

    features_dict = load_features(file_name=config.FEATURES_FILE)
    features = features_dict['categorical'] + features_dict['numerical']

    # divide train and test
    X_train, X_test, y_train, y_test = train_test_split(
        data[features], data[config.TARGET], test_size=0.3, random_state=120, shuffle=True
    )  # we are setting the seed here

    # transform the target
    # y_train = np.log(y_train)

    pipeline.pipe.fit(X_train, y_train)

    _logger.info(f"saving model version: {_version}")
    save_pipeline(pipeline_to_persist=pipeline.pipe)


if __name__ == "__main__":
    run_training()
