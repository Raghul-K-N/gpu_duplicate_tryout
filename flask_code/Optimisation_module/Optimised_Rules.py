from Optimisation_module.optimisation_utils import is_optimisation_model_available, predict_rules_scores
from code1.logger import capture_log_message
import pandas as pd
import numpy as np


RULES = [
            "LATE_PAYMENT","UNFAVORABLE_PAYMENT_TERMS","SUSPICIOUS_KEYWORD",
            "IMMEDIATE_PAYMENTS","POSTING_PERIOD","EARLY_POSTED_INVOICES",
            "NON_PO_INVOICE","INVOICES_WITHOUT_GRN"
            ]

def optimise_rule_scores(Scored_DF: pd.DataFrame, df_rules_scored: pd.DataFrame, rule_weights: dict, opt_rule_weights: dict) -> pd.DataFrame:
    """
    For each rule in RULES, if an optimization model exists, call predict_rules_scores()
    to create Scored_DF['OPTIMISED_<RULE>'].  Then returns the augmented DataFrame.
    rule_weights: dict mapping rule -> weight (from your DB)
    """
    capture_log_message("Starting optimisation of rule scores…")
    for rule in rule_weights:
        optimize_flag, model_path = is_optimisation_model_available(module=rule.title())
        rule_colm_flag = rule not in Scored_DF.columns
        opt_status_flag = f"OPTIMISED_{rule}" not in opt_rule_weights
        capture_log_message(f"Optimisation model for {rule}: model_flag: {optimize_flag}, rule_colm_flag: {rule_colm_flag}, opt_status_flag: {opt_status_flag}")
        if not optimize_flag or rule_colm_flag or opt_status_flag:
            capture_log_message(f"Skipping Optimisation Module for {rule}")
            Scored_DF[f"OPTIMISED_{rule}"] = 0.0
            continue

        capture_log_message(f"Optimising rule scores for {rule}…")
        Scored_DF[f"OPTIMISED_{rule}"] = predict_rules_scores(
            df_rules_scored=df_rules_scored,
            pipeline_path=model_path,
            rule_col=rule
        )
        capture_log_message(f"Completed optimisation for {rule}.")
    
    return Scored_DF


def optimised_rules_risk_score(df: pd.DataFrame, rule_weights: dict) -> pd.DataFrame:
    """
    Given df with columns OPTIMISED_<RULE>, compute:
      RAW   = sum( OPTIMISED_<RULE> * weight[rule] )
      SCALED = RAW / max(RAW)
    """
    capture_log_message("Calculating Optimised Rules Risk Score…")

    df['OPTIMISED_RULES_RISK_SCORE_RAW'] = 0.0

    for rule, weight in rule_weights.items():
        col = f"OPTIMISED_{rule}"
        df['OPTIMISED_RULES_RISK_SCORE_RAW'] += df[col] * weight

    # scale to [0,1]
    raw = df['OPTIMISED_RULES_RISK_SCORE_RAW']
    if raw.max() > 0:
        df['OPTIMISED_RULES_RISK_SCORE'] = (raw / raw.max()).round(2)
    else:
        df['OPTIMISED_RULES_RISK_SCORE'] = 0.0

    # adding a column with the rule names that contributed to the score
    rule_names = np.array(list(rule_weights.keys()))
    opt_cols = [f"OPTIMISED_{r}" for r in rule_weights]
    # boolean DataFrame: True where score > 0
    mask_df = df[opt_cols].values > 0

    df['OPTIMISED_CONTROL_DEVIATION'] = [", ".join(rule_names[mask]) for mask in mask_df ]

    capture_log_message("Optimised Rules Risk Score calculated.")
    return df


def optimised_doc_lvl_scores_calculation(df: pd.DataFrame, rule_weights: dict) -> pd.DataFrame:
    
    rules = list(rule_weights.keys())
    agg_dict = {f"OPTIMISED_{rule}": "max" for rule in rules}
    agg_dict.update({"OPTIMISED_RULES_RISK_SCORE_RAW":   "max",
                     "OPTIMISED_RULES_RISK_SCORE":       "max",
                     "OPTIMISED_BLENDED_RISK_SCORE":     "max", 
                     "OPTIMISED_DEVIATION":              "max" })
    rules_accountdoc = df.groupby('ACCOUNT_DOC_ID',as_index=False).agg(agg_dict)
    # control deviation calculation
    rule_names = np.array(rules)
    opt_cols = [f"OPTIMISED_{r}" for r in rule_weights]
    mask_df = rules_accountdoc[opt_cols].values > 0
    rules_accountdoc['OPTIMISED_CONTROL_DEVIATION'] = [", ".join(rule_names[mask]) for mask in mask_df ]
    return rules_accountdoc


def optimised_deviation_calculation(Scored_DF):

    capture_log_message(log_message='Optimised Blended score Calculation Started')
    Scored_DF['OPTIMISED_BLENDED_RISK_SCORE'] = Scored_DF['OPTIMISED_RULES_RISK_SCORE']   
    Scored_DF['OPTIMISED_DEVIATION'] = np.where((Scored_DF['OPTIMISED_RULES_RISK_SCORE']>0),1,0)
    capture_log_message(log_message='Optimised Blended score Calculation Completed')
    return Scored_DF



def optimised_acc_doc_lvl_rule_scores(df: pd.DataFrame, rule_weights: dict) -> pd.DataFrame:

    capture_log_message(log_message='Optimised Account Doc Level rule score calculation Started')
    rules_accountdoc = optimised_doc_lvl_scores_calculation(df, rule_weights)
    rules_accountdoc = optimised_deviation_calculation(rules_accountdoc)
    capture_log_message(log_message='Optimised Account Doc Level Function Completed')

    return rules_accountdoc