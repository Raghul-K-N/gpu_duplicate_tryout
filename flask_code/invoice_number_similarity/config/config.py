import pathlib
import pandas as pd
import invoice_number_similarity
import json

pd.options.display.max_rows = 10
pd.options.display.max_columns = 10


PACKAGE_ROOT = pathlib.Path(invoice_number_similarity.__file__).resolve().parent
TRAINED_MODEL_DIR = PACKAGE_ROOT / "trained_models"
EXTERNAL_MODEL_DIR = PACKAGE_ROOT / "external_models"

DATASET_DIR = PACKAGE_ROOT / "datasets"
TESTING_DATA_FILE = "test.json"
TRAINING_DATA_FILE = "ml_data_encoded.csv"
FEATURES_FILE = "columns_to_take.json"

TARGET = "Label"

# PIPELINE_NAME = "classifier"
# PIPELINE_SAVE_FILE = f"{PIPELINE_NAME}_output_v"

PIPELINE_NAME = "model_cat_20250103_124940"
PIPELINE_SAVE_FILE = f"{PIPELINE_NAME}"

# used for differential testing
ACCEPTABLE_MODEL_DIFFERENCE = 0.05

LOGS_DIR = PACKAGE_ROOT / "logs"