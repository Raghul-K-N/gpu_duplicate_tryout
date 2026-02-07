# Add to duplicate_invoices/config.py
# OPTIMIZATION_MODEL_DIR = MODELS_DIR / "optimization"
GROUP_COL = 'DUPLICATES_ID'
OPTIMIZATION_FEATURES = [
    'amount_diff', 
    'date_diff',
    'vendor_similarity',
    'invoice_similarity',
    'risk_avg',
    'amount_date_interaction',
    'vendor_amount_interaction',
    'risk_amount_interaction',
    'risk_date_interaction'
]