import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from code1.logger import capture_log_message

class ScoreAggregator(BaseEstimator, TransformerMixin):
    """Aggregates pair scores back to original documents"""
    
    def __init__(self, original_index):
        self.original_index = original_index
        self.pair_scores_ = None  # Will store predictions
        
    def fit(self, X, y=None):
        return self
        
    def transform(self, X_pairs):
        """Input: Pairs DataFrame from PairGenerator
           Output: Optimized scores aligned with original index"""
        capture_log_message(f"Score Aggregator transform method, input pairs df shape: {X_pairs.shape}")
        if self.pair_scores_ is None:
            raise ValueError("Predictions not set. Call set_predictions() first.")
        score_map = {idx: [] for idx in self.original_index}
        
        for (_, pair), score in zip(X_pairs.iterrows(), self.pair_scores_):
            score_map[pair['index_1']].append(score)
            score_map[pair['index_2']].append(score)
        capture_log_message(f"Score Aggregator, score map: {score_map}")
        #TODO threshold calculation
        avg_scores = {idx: sum(scores)/len(scores) if scores else 0 
                     for idx, scores in score_map.items()}
        avg_pair_scores = pd.Series(avg_scores, name='OPTIMIZED_RISK')
        capture_log_message(f"Score Aggregator transform method completed, output series shape: {avg_pair_scores.shape}")
        return avg_pair_scores
    
    def set_predictions(self, pair_scores):
        """Store predictions before transform"""
        self.pair_scores_ = pair_scores
        return self