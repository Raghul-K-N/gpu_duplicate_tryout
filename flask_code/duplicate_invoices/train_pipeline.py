import numpy as np
from sklearn.model_selection import train_test_split

from duplicate_invoices import pipeline
from duplicate_invoices.processing.data_management import load_dataset, save_pipeline
from duplicate_invoices.config import config, logging_config
from duplicate_invoices import __version__ as _version

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

    pipeline.pipe.fit(data, None)

    _logger.info(f"saving model version: {_version}")
    save_pipeline(pipeline_to_persist=pipeline.pipe)


if __name__ == "__main__":
    run_training()
