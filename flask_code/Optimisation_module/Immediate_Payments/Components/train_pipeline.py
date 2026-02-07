# Optimization_module/Immediate_Payments/Componenets/train_pipeline.py
import joblib
from sklearn.pipeline import Pipeline
from .transformers import DataTransformation, FeatureEngineer, preprocessing
from .model import ModelPredictor  # <-- from above

def build_pipeline():
    pipe = Pipeline([
        ('data_transformation', DataTransformation()),
        ('feature_engineer', FeatureEngineer()),
        ("preproc", preprocessing),
        ('model_predictor', ModelPredictor())
    ])
    return pipe

def train_and_save_pipeline(original_df, y_label, save_path):
    pipe = build_pipeline()

    # Fit the scaler on raw training data
    pipe.fit(original_df, y_label)
    print(f"model save path: {save_path}")
    # Save the full pipeline (preprocessing + model)
    joblib.dump(pipe, save_path)
