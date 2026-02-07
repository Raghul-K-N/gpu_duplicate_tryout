# optimization_module/transformers.py
import pandas as pd
from itertools import combinations
from difflib import SequenceMatcher
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler
from code1.logger import capture_log_message
from .config import OPTIMIZATION_FEATURES , GROUP_COL


class DataTransformation(BaseEstimator, TransformerMixin):
    """Generate all pairwise combinations within duplicate groups"""
    
    def __init__(self):
        self.group_col = GROUP_COL

    def fit(self, X, y=None):
        self.is_fitted_ = True
        capture_log_message(f"Data Transformation fit method, input df shape: {X.shape}")
        return self
        
    def transform(self, X):
        """Returns: Single DataFrame with columns [group_id, index_1, index_2]"""
        capture_log_message(f"Data Transformation started, input df shape: {X.shape}")
        pairs = []
        for group_id, group in X.groupby(self.group_col):
            if len(group) > 1:
                pairs.extend([{
                    self.group_col: group_id,
                    'index_1': i,
                    'index_2': j
                } for i, j in combinations(group.index, 2)])
        pairs_df = pd.DataFrame(pairs)
        capture_log_message(f"Data Transformation completed, pairs df shape:{pairs_df.shape}")
        return pairs_df



class FeatureEngineer(BaseEstimator, TransformerMixin):
    """Create features for each pair"""
    
    def __init__(self, original_df):
        self.original_df = original_df
        self.FEATURE_COLS  = OPTIMIZATION_FEATURES
        self.scaler = StandardScaler()

    def fit(self, X, y=None):
        # Generate raw features to fit the scaler
        capture_log_message(f"Feature Engineering fit method, input df shape: {X.shape}")
        self.is_fitted_ = True
        raw = self.compute_raw_features(X)
        self.scaler.fit(raw[self.FEATURE_COLS])
        capture_log_message(f"Feature Engineering fit method completed, scaler fitted")
        return self
    
    def _calculate_amount_diff(self, row1, row2):
        return abs(row1['INVOICE_AMOUNT'] - row2['INVOICE_AMOUNT'])
    
    def _calculate_date_diff(self, row1, row2):
        return abs((pd.to_datetime(row1['INVOICE_DATE']) - 
                   pd.to_datetime(row2['INVOICE_DATE'])).days)
    
    def _calculate_string_similarity(self, str1, str2):
        return SequenceMatcher(None, str(str1), str(str2)).ratio()
    
    def _calculate_vendor_similarity(self, row1, row2):
        return self._calculate_string_similarity(row1['SUPPLIER_NAME'], 
                                              row2['SUPPLIER_NAME'])
    
    def _calculate_invoice_similarity(self, row1, row2):
        return self._calculate_string_similarity(row1['INVOICE_NUMBER'],
                                              row2['INVOICE_NUMBER'])
    
    def _calculate_risk_avg(self, row1, row2):
        return (row1['RISK_SCORE'] + row2['RISK_SCORE']) / 2
    
    def _calculate_amount_date_interaction(self, row1, row2):
        amount_diff = self._calculate_amount_diff(row1, row2)
        date_diff = self._calculate_date_diff(row1, row2)
        return amount_diff * date_diff
    
    def _calculate_vendor_amount_interaction(self, row1, row2):
        vendor_sim = self._calculate_vendor_similarity(row1, row2)
        amount_diff = self._calculate_amount_diff(row1, row2)
        return vendor_sim * amount_diff
    
    def _calculate_risk_amount_interaction(self, row1, row2):
        return row1['RISK_SCORE'] * self._calculate_amount_diff(row1, row2)
    
    def _calculate_risk_date_interaction(self, row1, row2):
        return row1['RISK_SCORE'] * self._calculate_date_diff(row1, row2)
    

    def transform(self, X):
        # Compute raw features
        capture_log_message(f"Feature Engineering transform method, input df shape: {X.shape}")
        raw_features = self.compute_raw_features(X)
        capture_log_message(f"Raw features computed, raw features df shape: {raw_features.shape}")
        scaled_vals = self.scaler.transform(raw_features[self.FEATURE_COLS])
        features_scaled_df = pd.DataFrame(scaled_vals, columns=self.FEATURE_COLS, index=raw_features.index)
        capture_log_message(f"Feature Engineering transform method completed, scaled features df shape: {features_scaled_df.shape}")
        return features_scaled_df
    

    def compute_raw_features(self, X):
        features = []
        for _, row in X.iterrows():
            row1 = self.original_df.loc[row['index_1']]
            row2 = self.original_df.loc[row['index_2']]
            
            features.append({
                'amount_diff': self._calculate_amount_diff(row1, row2),
                'date_diff': self._calculate_date_diff(row1, row2),
                'vendor_similarity': self._calculate_vendor_similarity(row1, row2),
                'invoice_similarity': self._calculate_invoice_similarity(row1, row2),
                'risk_avg': self._calculate_risk_avg(row1, row2),
                'amount_date_interaction': self._calculate_amount_date_interaction(row1, row2),
                'vendor_amount_interaction': self._calculate_vendor_amount_interaction(row1, row2),
                'risk_amount_interaction': self._calculate_risk_amount_interaction(row1, row2),
                'risk_date_interaction': self._calculate_risk_date_interaction(row1, row2)
            })
        return pd.DataFrame(features)