# predict.py
import joblib
import pandas as pd
from code1.logger import capture_log_message
from Optimisation_module.Duplicate_Invoice.Components.transformers import DataTransformation, FeatureEngineer
from Optimisation_module.Duplicate_Invoice.Components.model import ModelPredictor
from Optimisation_module.Duplicate_Invoice.Components.aggregation import ScoreAggregator


def optimize_duplicate_scores(original_df, pipeline_path):

    capture_log_message("Starting optimization of duplicate scores")

    pipe = joblib.load(pipeline_path)
    capture_log_message(f"Duplicate Pipeline loaded successfully{pipe.steps}")

    original_df.reset_index(drop=True,inplace=True)

    # Predict scores for all pairs
    pair_scores = pipe.predict(original_df)
    capture_log_message(f"Duplicate Pipeline prediction completed, length:{len(pair_scores)}")

     # Get the original pairs (needed for aggregation)
    capture_log_message(f"Starting transformation of original DataFrame to pairs DataFrame{original_df}")
    pairs_df = pipe.named_steps['data_transformation'].transform(original_df)
    if pairs_df.empty:
        capture_log_message("Pairs DataFrame is empty, returning zeros")
        return pd.Series(0, index=original_df.index, name='OPTIMIZED_RISK')
    # capture_log_message(f"Pairs DataFrame created, pairs df:{pairs_df}")


    # Aggregate
    capture_log_message("Starting aggregation of pair scores")
    agg = ScoreAggregator(original_df.index)
    agg.set_predictions(pair_scores)
    optimisation_scores = agg.transform(pairs_df).fillna(0).round(2)
    capture_log_message(f"Aggregation of pair scores completed, length:{len(optimisation_scores)}")
    return optimisation_scores
