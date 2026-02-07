# Optimization_module/Unfavorable_Payment_Terms/Componenets/transformers.py
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, OneHotEncoder, TargetEncoder
# from category_encoders import TargetEncoder as CE_TargetEncoder
from sklearn.compose import ColumnTransformer
from code1.logger import capture_log_message
import numpy as np
from .config import num_feats, onehot_feats, freq_target_feats


class DataTransformation(BaseEstimator, TransformerMixin):
        
    def __init__(self):
        pass

    def fit(self, X, y=None):
        capture_log_message(f"Data Transformation fit method, input df shape: {X.shape}")
        self.is_fitted_ = True
        return self
        
    def transform(self, X):
        missing_values = X.isna().sum()
        missing_value_cols = missing_values[missing_values > 0].to_dict() 
        capture_log_message(f"Data Transformation started, input df shape: {X.shape}")
        capture_log_message(f"Data Transformation transform method, input null check: {missing_value_cols}")
        capture_log_message(f"Data Transformation transform method, Missing values present: {X.isna().any().any()}")
        return X


class FrequencyEncoder(BaseEstimator, TransformerMixin):
    """
    Fits on a train set, learns category → frequency (or count),
    and maps unseen to 0 at transform time.
    """
    def __init__(self):
        pass

    def fit(self, X, y=None):
        # X: DataFrame or Series with vendor codes
        s = pd.Series(X.squeeze()).astype(str)
        vc = s.value_counts()
        max_count = vc.max()
        self.freq_map_ = (vc / max_count).to_dict() 
        self.default_ = 0.0
        return self

    def transform(self, X):
        s = pd.Series(X.squeeze()).astype(str)
        # map unseen to 0.0
        enc = s.map(self.freq_map_).fillna(self.default_)
        return enc.to_numpy().reshape(-1, 1)


class FeatureEngineer(BaseEstimator, TransformerMixin):
    """Create features for each pair"""
    
    def __init__(self):
        pass

    def fit(self, X, y=None):
        capture_log_message(f"Feature Engineering fit method, input df shape: {X.shape}")
        self.is_fitted_ = True
        return self
    

    def _calculate_log_amount(self, df):
        log_amt = np.log2(df['AMOUNT'].replace(0, np.nan)).fillna(0)
        return log_amt
    

    def transform(self, X):
        # Compute raw features
        capture_log_message(f"Feature Engineering transform method, input df shape: {X.shape}")
        X_ = X.copy()
        X_['DUE_DAYS'] = X_['CREDIT_PERIOD']
        X_["LOG_AMOUNT"] = self._calculate_log_amount(X_)
        X_['INVOICE_QTR'] = X_['INVOICE_DATE'].dt.quarter
        X_['INVOICE_DAY']= X_['INVOICE_DATE'].dt.day
        X_['INVOICE_MONTH']= X_['INVOICE_DATE'].dt.month
        X_['DUE_DATE_QTR'] = X_['DUE_DATE'].dt.quarter.fillna(0).astype(int)
        X_['DUE_DATE_DAY']= X_['DUE_DATE'].dt.day.fillna(0).astype(int)
        X_['DUE_DATE_MONTH']= X_['DUE_DATE'].dt.month.fillna(0).astype(int)
        capture_log_message(f"Feature Engineering transform method completed, shape after raw features: {X_.shape}")
        return X_
    

preprocessing = ColumnTransformer([

    ("vend_freq", FrequencyEncoder(), ["VENDORCODE"]),

    ("posted_user_freq", FrequencyEncoder(), ["POSTED_BY_NAME"]),

    ("one_hot_enc", OneHotEncoder(handle_unknown="ignore", sparse_output=False), onehot_feats),

    ("tgt_enc", TargetEncoder(categories="auto",
                              target_type="binary",
                              smooth=5.0,
                              cv=10,
                              shuffle=True,
                              random_state=42), freq_target_feats),

    ("scale", StandardScaler(), num_feats),
    ],
    remainder="drop",
    verbose_feature_names_out=True)






# class SmoothedFreqEncoder(BaseEstimator, TransformerMixin):
#     def __init__(self, col, smoothing=5):
#         self.col     = col
#         self.smoothing = smoothing

#     def fit(self, X, y=None):
#         vc = X[self.col].value_counts()
#         N  = len(X)
#         # smoothed frequency: (count + α·prior) / (N + α)
#         # here prior = global mean = 1/N
#         self.freq_map_ = {
#             cat: (cnt + self.smoothing*(1/N)) / (N + self.smoothing)
#             for cat, cnt in vc.items()
#         }
#         self.default_ =(self.smoothing*(1/N)) / (N + self.smoothing)
#         return self

#     def transform(self, X):
#         s = X[self.col].map(self.freq_map_).fillna(self.default_)  # unseen→global mean
#         return s.to_frame(name=self.col + "_freq")



