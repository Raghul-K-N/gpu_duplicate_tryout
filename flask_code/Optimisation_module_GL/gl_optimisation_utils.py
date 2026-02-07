import os
import glob
from code1.logger import  capture_log_message


def is_optimisation_model_available( module:str, extensions=None):
    """
    Returns True if there's at least one file in model_dir matching any of the given extensions.
    """
    script_path = os.path.abspath(__file__)
    optimisation_module_dir = os.path.dirname(script_path)
    module_directory = os.path.join(optimisation_module_dir, module)
    model_dir = os.path.join(module_directory,'Pipeline')
    model_path = None
    if extensions is None:
        extensions = ['*.bin', '*.model', '*.xgb', '*.pkl']  # add your extensions here

    for ext in extensions:
        pattern = os.path.join(model_dir, ext)
        matches = glob.glob(pattern)
        if matches:
            model_path = matches[0]
            capture_log_message(f"Found {module} optimisation model file: {matches}")
            return True, model_path
    capture_log_message(f"No model file found for module: {module}")
    return False, model_path



def predict_rules_scores(df_rules_scored, pipeline_path, rule_col):
    import joblib, pandas as pd
    pipe = joblib.load(pipeline_path)

    scores = pd.Series(0.0, index=df_rules_scored.index, name=f"OPTIMISED_{rule_col}_score")
    mask = df_rules_scored[rule_col]==1
    if not mask.any():
        capture_log_message(f"Skipping optimisation for {rule_col} since no scored data is available!")
        return scores
    
    X = df_rules_scored.loc[mask]
    preds = pipe.predict(X)

    scores.loc[mask] = preds
    return scores