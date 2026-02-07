# Optimization_module/Early_Posted_Invoices/Componenets/train_model.py

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
from Optimisation_module.Early_Posted_Invoices.Components.train_pipeline import train_and_save_pipeline
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
    df = pd.read_csv("/home/whirldata/projects/ap_iv_vm_combined_code_apr8/AP_Module/csv_results/Rules_Results.csv",parse_dates=['INVOICE_DATE','DUE_DATE','POSTING_DATE','ENTERED_DATE','PAYMENT_DATE'])
    df["user_feedback"] = np.random.choice([0, 1], size=len(df))
    y_label = df['user_feedback']

    output_dir = Path(__file__).parent.parent / "Pipeline"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "opt_early_psted_invs_pipeline_v1.pkl"

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


# # train_model.py
# import pandas as pd
# import joblib
# from pathlib import Path
# # from code1.logger import initialise_logger
# from Optimisation_module.Duplicate_Invoice.Components.train_pipeline import train_and_save_pipeline
# from Optimisation_module.Duplicate_Invoice.Components.transformers   import DataTransformation
# from Optimisation_module.Duplicate_Invoice.Components.config         import GROUP_COL


# # initialise_logger('ap',audit_id=1)
# # 1) Load invoice-level data (must include DUPLICATE_ID & user_feedback)
# df2 = pd.read_csv("/home/whirldata/projects/ap_iv_vm_combined_code_apr8/Optimisation_module/Duplicate_Invoice/extra/train_dupl.csv", parse_dates=['INVOICE_DATE'])

# df = pd.DataFrame({
#     'DUPLICATES_ID': [1,1,2,2,2,3,3,3,3],
#     'INVOICE_AMOUNT': [100, 105, 200, 210, 190, 300,310,320,290],
#     'INVOICE_DATE': pd.to_datetime(['2023-01-01','2023-01-05','2023-02-01','2023-02-03','2023-02-05','2023-03-01','2023-03-21','2023-03-06','2023-03-11']),
#     'SUPPLIER_NAME': ['AB','AB','XY','XYZ','ZYX','C','CC','CD','CE'],
#     'INVOICE_NUMBER': ['X1','X2','Y1','Y2','Y3','Z1','Z@','ZZ','Z11'],
#     'RISK_SCORE': [0.1,0.2,0.3,0.4,0.5,0.6,0.5,0.5,0.7],
#     'user_feedback': [1,1,0,0,0,1,1,1,1]
# })

# # 2) Generate pairs
# pairs = DataTransformation().transform(df)
# if pairs.empty:
#     raise ValueError("No pairs to train on")

# group_labels = (
#     df
#     .drop_duplicates(subset=[GROUP_COL])
#     .set_index(GROUP_COL)['user_feedback']
#     .to_dict()
# )

# y_pairs = pairs[GROUP_COL].map(group_labels)


# # 4) Train & save
# pipepath = '/home/whirldata/projects/ap_iv_vm_combined_code_apr8/Optimisation_module/Duplicate_Invoice/Pipeline'
# output_path = f"{pipepath}/opt_pipeline_v1.pkl"
# train_and_save_pipeline(df, y_pairs, output_path)
# print(f"Pipeline saved to {output_path}")
# pipe = joblib.load(output_path)
# scores =pipe.predict(df2)
# print(scores)