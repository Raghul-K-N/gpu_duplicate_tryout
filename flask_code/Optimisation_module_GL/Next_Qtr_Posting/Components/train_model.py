# Optimization_module_GL/Next_Qtr_Posting/Componenets/train_model.py

import sys
#  ─────────────────────────────────────────────────────────────────────────────
#  STUB OUT code1.logger.capture_log_message to avoid circular import
if "code1.logger" not in sys.modules:
    dummy = type(sys)("code1.logger")
    dummy.capture_log_message = lambda *args, **kwargs: None
    sys.modules["code1.logger"] = dummy
#  ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
import joblib
from pathlib import Path

# now your real imports will see the stubbed logger
from Optimisation_module_GL.Next_Qtr_Posting.Components.train_pipeline import train_and_save_pipeline
import numpy as np
def main():
    # load your real or dummy df…
    # df = pd.DataFrame({
    #     'DUPLICATES_ID': [1,1,2,2,2,3,3,3,3],
    #     'INVOICE_AMOUNT': [100, 105, 200, 210, 190, 300,310,320,290],
    #     'INVOICE_DATE': pd.to_datetime(['2023-01-01','2023-01-05','2023-02-01','2023-02-03','2023-02-05','2023-03-01','2023-03-21','2023-03-06','2023-03-11']),
    #     'SUPPLIER_NAME': ['AB','AB','XY','XYZ','ZYX','C','CC','CD','CE'],
    #     'INVOICE_NUMBER': ['X1','X2','Y1','Y2','Y3','Z1','Z@','ZZ','Z11'],
    #     'RISK_SCORE': [0.1,0.2,0.3,0.4,0.5,0.6,0.5,0.5,0.7],
    #     'user_feedback': [1,1,0,0,0,1,1,1,1]
    # })
    df = pd.read_csv("/home/whirldata/projects/May6_latest_code_AP_IV_VM/May6_latest_code/GL_Module/csv_results/GL_Rules_result.csv",parse_dates=['POSTED_DATE','ENTERED_DATE'])
    df['POSTED_BY_NAME'] = df['POSTED_BY_NAME'].astype(str)
    df["user_feedback"] = np.random.choice([0, 1], size=len(df))
    y_label = df['user_feedback']

    output_dir = Path(__file__).parent.parent / "Pipeline"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "opt_nxt_qtr_posting_pipeline_v0.pkl"

    train_and_save_pipeline(df, y_label, output_path)
    print(f"Pipeline saved to {output_path}")

    # quick sanity
    pipe = joblib.load(output_path)
    from sklearn.utils.validation import check_is_fitted
    for name, step in pipe.named_steps.items():
        if hasattr(step, "fit"):  # Only check steps that have a fit method
            try:
                check_is_fitted(step)
                print(f"✅ '{name}' is fitted.")
            except:
                print(f"❌ '{name}' is NOT fitted.")
        else:
            print(f"⚠ '{name}' does not have a fit method (skipping).")

    print("Sample pair scores:", pipe.predict(df)[:5])

if __name__ == "__main__":
    main()

