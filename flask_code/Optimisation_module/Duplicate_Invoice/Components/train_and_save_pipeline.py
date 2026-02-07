# train_model.py
# import os
# import joblib
# from sklearn.pipeline import Pipeline
# from .transformers import DataTransformation, FeatureEngineer
# from .model import ModelPredictor
# train_and_test_dummy.py
import sys
from pathlib import Path
import pandas as pd

# 1. Add project root to path (adjust the number of .parent as needed)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 2. Now import your modules
try:
    from Optimisation_module.Duplicate_Invoice.Components.train_model import train_and_save_pipeline
    from Optimisation_module.Duplicate_Invoice.Components.predict import optimize_duplicate_scores
except ImportError as e:
    print(f"Import failed: {e}")
    print(f"Current Python path: {sys.path}")
    raise

def create_test_data():
    """Generate dummy test data"""
    return pd.DataFrame({
        'group_id': [1,1,2,2,2,3],
        'INVOICE_AMOUNT': [100,105,200,210,190,300],
        'INVOICE_DATE': pd.to_datetime(
            ['2023-01-01','2023-01-05','2023-02-01','2023-02-03','2023-02-05','2023-03-01']),
        'SUPPLIER_NAME': ['A','A','B','B','B','C'],
        'INVOICE_NUMBER': ['X1','X2','Y1','Y2','Y3','Z1'],
        'RISK_SCORE': [0.1,0.2,0.3,0.4,0.5,0.6],
        'user_feedback': [1,1,0,0,0,1]
    })

def main():
    # 1) Create test data
    df = create_test_data()
    csv_path = Path("dummy_data.csv")
    df.to_csv(csv_path, index=False)
    print(f"Created test data at: {csv_path.resolve()}")

    # 2) Train pipeline
    save_dir = Path("test_pipelines")
    save_dir.mkdir(exist_ok=True)
    pipeline_path = save_dir / "opt_pipeline_dummy.pkl"
    
    try:
        train_and_save_pipeline(df, df['user_feedback'], pipeline_path)
        print(f"Pipeline saved to: {pipeline_path.resolve()}")
    except Exception as e:
        print(f"Training failed: {e}")
        return

    # 3) Test prediction
    try:
        df2 = pd.read_csv(csv_path, parse_dates=['INVOICE_DATE'])
        opt_scores = optimize_duplicate_scores(df2, pipeline_path)
        print("\nOptimized scores:")
        print(opt_scores.to_string())
    except Exception as e:
        print(f"Prediction failed: {e}")

if __name__ == "__main__":
    main()
# train_and_test_dummy.py
import pandas as pd
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent  # Adjust based on your structure
sys.path.append(str(project_root))
from Optimisation_module.Duplicate_Invoice.Components.train_model import train_and_save_pipeline
from Optimisation_module.Duplicate_Invoice.Components.predict import optimize_duplicate_scores

# 1) Make dummy CSV
df = pd.DataFrame({
    'group_id':   [1,1,2,2,2,3],
    'INVOICE_AMOUNT': [100,105,200,210,190,300],
    'INVOICE_DATE':   pd.to_datetime(
        ['2023-01-01','2023-01-05','2023-02-01','2023-02-03','2023-02-05','2023-03-01']),
    'SUPPLIER_NAME':  ['A','A','B','B','B','C'],
    'INVOICE_NUMBER':['X1','X2','Y1','Y2','Y3','Z1'],
    'RISK_SCORE':     [0.1,0.2,0.3,0.4,0.5,0.6],
    'user_feedback':  [1,1,0,0,0,1]
})
csv_path = Path("dummy_data.csv")
df.to_csv(csv_path, index=False)
print(f"Wrote {csv_path}")

# 2) Train pipeline
save_dir = Path("Pipeline")
pipeline_path = save_dir / "opt_pipeline_dummy.pkl"
train_and_save_pipeline(df, df['user_feedback'], pipeline_path)
print(f"Pipeline saved to {pipeline_path}")

# 3) Load & predict
df2 = pd.read_csv(csv_path, parse_dates=['INVOICE_DATE'])
opt_scores = optimize_duplicate_scores(df2, pipeline_path)
print("\nOptimized invoice scores:\n", opt_scores)



# def train_and_save_pipeline(df_output, save_dir):
#     """
#     df_output: invoice-level DataFrame with a 'user_feedback' column
#     save_dir:  directory path (string or Path) to write the .pkl
#     """
#     # 1) Generate pairs
#     dt = DataTransformation()
#     pairs = dt.transform(df_output)
#     if pairs.empty:
#         raise ValueError("No pairs to train on")

#     # 2) Extract pair-level labels in the same order
#     #    We assume 'user_feedback' lives on the invoice rows, and
#     #    that a pair is labeled positive if BOTH invoices have feedback=1
#     y_pairs = []
#     for _, row in pairs.iterrows():
#         fb1 = df_output.loc[row['index_1'], 'user_feedback']
#         fb2 = df_output.loc[row['index_2'], 'user_feedback']
#         # define your logic: here we say it's duplicate only if both marked 1
#         y_pairs.append(1 if (fb1 == 1 and fb2 == 1) else 0)
#     y_pairs = pd.Series(y_pairs)

#     # 3) Build pipeline
#     pipe = Pipeline([
#         ('data_transformation', dt),
#         ('feature_engineer', FeatureEngineer(df_output)),
#         # FeatureEngineer fits its own scaler internally
#         ('model_predictor', ModelPredictor())
#     ])

#     # 4) Fit end‑to‑end: pipeline.fit calls
#     #    - dt.fit_transform(df_output)   -> pairs
#     #    - fe.fit_transform(pairs)       -> scaled features
#     #    - model_predictor.fit(X_scaled, y_pairs)
#     pipe.fit(df_output, y_pairs)

#     # 5) Save the pipeline
#     save_dir = Path(save_dir)
#     save_dir.mkdir(parents=True, exist_ok=True)
#     save_path = save_dir / "opt_pipeline_v1.pkl"
#     joblib.dump(pipe, save_path)
#     print(f"Saved optimization pipeline to {save_path}")
#     return save_path

# if __name__ == "__main__":
#     # Example usage: adjust path as needed
#     df = pd.read_csv("df_output.csv", parse_dates=['INVOICE_DATE'])
#     train_and_save_pipeline(df, save_dir="models")
