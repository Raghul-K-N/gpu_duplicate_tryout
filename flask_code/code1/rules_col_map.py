'''
Python Script for the updating the rules flags based on the mandatory and recommended columns
'''
import configparser
from datetime import datetime, timezone
import os
import pandas as pd
import numpy as np


gl_reqd_cols = {"BLANK JE" : "TRANSACTION_DESCRIPTION", "WEEKEND POSTING" : "ENTERED_DATE", "HOLIDAY POSTING": "ENTERED_DATE", "LATE NIGHT POSTING" : "ENTERED_DATE",
        "NEXT QUARTER POSTING": ["ENTERED_DATE","POSTED_DATE"], "SAME USER POSTING" :["ENTERED_BY", "POSTED_BY"],
        "SUSPICIOUS KEYWORDS" : "TRANSACTION_DESCRIPTION", "ROUNDING OFF": "AMOUNT", "CASH CONCENTRATION - CREDIT": ["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"],
        "CASH CONCENTRATION - DEBIT" : ["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"],"CASH DISBURSEMENT - CREDIT": ["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"],
        "CASH DISBURSEMENT – DEBIT": ["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"],"CASH PAYROLL - DEBIT":["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"],
        "CASH PAYROLL - CREDIT": ["ACCOUNTING_DOC","SAP_ACCOUNT","DEBIT_CREDIT_INDICATOR"]
        }
ap_reqd_cols = {"SUSPICIOUS KEYWORDS":"TRANSACTION_DESCRIPTION","LATE_PAYMENT": ["DUE_DATE","PAYMENT_DATE"],"UNFAVOURABLE PAYMENT TERMS": "CREDIT_PERIOD", "IMMEDIATE PAYMENTS":["PAYMENT_DATE", "INVOICE_DATE", "DISCOUNT_TAKEN"],
        "POSTING PERIOD" : ["INVOICE_DATE","POSTED_DATE"], "LOST DISCOUNT" : ["PAYMENT_DATE", "INVOICE_DATE", "DISCOUNT_TAKEN","DISCOUNT_PERIOD"] ,'NON_PO_INVOICES':['DOC_TYPE','SUPPLIER_ID'],'EARLY_POSTED_INVOICES':['DOC_TYPE','POSTED_DATE','INVOICE_DATE'],
        "DUPLICATE INVOICES": ["SUPPLIER_NAME", "SUPPLIER_ID","INVOICE_NUMBER", "INVOICE_AMOUNT", "INVOICE_DATE","COMPANY_NAME"],'INVOICES_WITHOUT_GRN':'GRN_NUMBER','OLD_UNPAID_INVOICES':['INVOICE_NUMBER','PAYMENT_DATE','DUE_DATE'],
        "DATE_SEQUENTIAL_MISMATCH": ["INVOICE_DATE","DUE_DATE","REQUISITION_DATE","GRN_DATE","PAYMENT_DATE","POSTED_DATE","ENTERED_DATE"], 'PAYMENT_R_BLOCK': 'PAYMENT_BLOCK_STATUS'}

# df = pd.read_csv('gl_2022_100.csv')
def rules(df,MODE_KEY):
    gl_rules,ap_rules = [],[]
    cols = df.columns
    if MODE_KEY == 'GL':
        for key,value in gl_reqd_cols.items():
            try:
                if value in cols:
                    gl_rules.append(key)
            except:
                gl_all = all(v in cols for v in value)
                if gl_all:
                    gl_rules.append(key)
        return gl_rules
    if MODE_KEY == 'AP':
        for key,value in ap_reqd_cols.items():
            try:
                if value in cols:
                    ap_rules.append(key)
            except:
                ap_all = all(v in cols for v in value)
                if ap_all:
                    ap_rules.append(key)
        
        return ap_rules


