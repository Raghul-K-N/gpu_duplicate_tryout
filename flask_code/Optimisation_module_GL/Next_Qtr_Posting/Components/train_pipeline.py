# Optimization_module_GL/Next_Qtr_Posting/Componenets/train_pipeline.py
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
    from sklearn.utils.validation import check_is_fitted

    # Access ColumnTransformer inside the pipeline
    preproc = pipe.named_steps['preproc']

    # Check if each transformer inside ColumnTransformer is fitted
    for name, transformer, cols in preproc.transformers_:
        if hasattr(transformer, "fit"):
            try:
                check_is_fitted(transformer)
                print(f"✅ Transformer '{name}' is fitted.")
            except:
                print(f"❌ Transformer '{name}' is NOT fitted.")
    try:
        check_is_fitted(pipe)
        print("✅ Pipeline is fitted.")
    except:
        print("❌ Pipeline is NOT fitted.")
    

    for name, step in pipe.named_steps.items():
        if hasattr(step, "fit"):  # Only check steps that have a fit method
            try:
                check_is_fitted(step)
                print(f"✅ '{name}' is fitted.")
            except:
                print(f"❌ '{name}' is NOT fitted.")
        else:
            print(f"⚠ '{name}' does not have a fit method (skipping).")
    print(f"model save path: {save_path}")
    # Save the full pipeline (preprocessing + model)
    joblib.dump(pipe, save_path)
