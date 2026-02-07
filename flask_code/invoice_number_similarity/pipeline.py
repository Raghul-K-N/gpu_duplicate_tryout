from sklearn.pipeline import Pipeline
from sklearn.dummy import DummyClassifier
from invoice_number_similarity.processing import preprocessors as pp
from invoice_number_similarity.config import config, logging_config
import logging
from catboost import CatBoostClassifier, Pool
model = CatBoostClassifier(iterations=150,
                           depth=10,
                           learning_rate=1,
                           loss_function='Logloss',
                           verbose=True)

_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)


pipe = Pipeline(
    [
        (
            "training",
            model
        )
    ]
)