# model.py
from xgboost import XGBClassifier
from sklearn.base import BaseEstimator
import numpy as np
from sklearn.utils.validation import check_is_fitted
from code1.logger import capture_log_message

class ModelPredictor(BaseEstimator):
    def __init__(self):
        self.model = XGBClassifier(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            scale_pos_weight=2.0,
            eval_metric=['logloss', 'auc']
        )

    def fit(self, X, y):
        # X: scaled feature array, y: your pair‐level labels
        self.model.fit(X, y,
                       verbose=False)
        self.feature_importances_ = self.model.feature_importances_
        capture_log_message(f"Model fit completed, model params: {self.model.get_params()}")
        return self

    def predict(self, X):
        # returns probability of positive class
        check_is_fitted(self, ["feature_importances_"])
        capture_log_message(f"Model predict method, input shape: {X.shape}")
        capture_log_message(f"Model predict method, input null check: {np.isnan(X).sum()}")
        capture_log_message(f"Model predict method, Missing values present: {np.isnan(X).any().any()}")
        pred_scores = self.model.predict(X)
        capture_log_message(f"Model predict method completed, output shape: {len(pred_scores)}")
        return pred_scores
