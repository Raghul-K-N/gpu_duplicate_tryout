import pandas as pd 
import numpy as np
import os
import json
from datetime import datetime, timezone
from flask import g
from GL_Module.db_connector import MySQL_DB
from GL_Module.Data_Preparation import Preparation
#import holidays
from code1.logger import capture_log_message
from itertools import product

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)

try:
    predefined_unusual_pairs = pd.read_csv(os.path.join(base_directory,'Unusual_pairs_genmab.csv'))
    str_cols = predefined_unusual_pairs.select_dtypes(include="object").columns
    predefined_unusual_pairs[str_cols] = (predefined_unusual_pairs[str_cols].apply(lambda s: s.str.strip().str.lower()))
    predefined_unusual_dict = {(row['Credit'],row['Debit']):True  for _, row in predefined_unusual_pairs.iterrows()} 
    capture_log_message('Loaded Unusual_pairs.csv successfully')

except FileNotFoundError:
    predefined_unusual_dict = {}
    capture_log_message(
        "Unusual_pairs.csv **not found**, All Logic_1 UNUSUAL_PAIRS will be marked False"
    )

class Rules_Framework():
    
    def __init__(self,configs):

        """
        Initializing rules configurations
        """
        #outofbalance
        #Rule keys and their weights
        # self.rule_weights ={'POSTS_WEEKEND':5, 'POSTS_NIGHT':4, 'SUSPICIOUS_KEYWORDS':7, 'NON_BALANCED':9, 'NEXT_QTR_POSTING':6, 'SAME_USER_POSTING':5, 'BLANK_JE':4, 'POSTS_HOLIDAYS':4, 'ROUNDING_OFF':5,
        # 'ACCRUAL':4,'REVERSALS':6, 'CASH_NEGATIVE_BALANCE':5, "CASH_CONCENTRATION_CREDIT":4,"CASH_CONCENTRATION_DEBIT":4,"CASH_DISBURSEMENT_CREDIT":7,"CASH_DISBURSEMENT_DEBIT":7,"CASH_PAYROLL_CREDIT":4,"CASH_PAYROLL_DEBIT":4,"CASH_LOCKBOX":6,
        # 'UNUSUAL_ACCOUNTING_PATTERN':9}
        self.DB = MySQL_DB('DB.json')
        self.rule_weights = {
            'POSTS_WEEKEND':5, 'POSTS_HOLIDAYS':4, 'NEXT_QTR_POSTING':5,'SAME_USER_POSTING':6, 'SUSPICIOUS_KEYWORDS':4,'NON_BALANCED':6,'ROUNDING_OFF':4
        }

        #Rule keys and their short description
        self.rule_names = {'BLANK_JE': 'Nil Transaction description','NEXT_QTR_POSTING': 'Next Quarter Posting','POSTS_WEEKEND': 'Weekend Posting','FIREFIGHTER':"Fire fighter",'CASH_LOCKBOX_SUSPENSE': 'Cash Lockbox Suspense',
        'POSTS_NIGHT': 'Late Night Posting','POSTS_HOLIDAYS': 'Holiday Posting','SAME_USER_POSTING': 'Same User Posting','SUSPICIOUS_KEYWORDS': 'Suspicious Keywords','NON_BALANCED': 'Out of Balance',
        'ACCRUAL': 'Accruals without Reversals','REVERSALS': 'Reversals without Accruals','CASH_NEGATIVE_BALANCE': 'Cash Negative Balances',"CASH_CONCENTRATION_CREDIT": 'Cash Concentration - Credit',
        'CASH_CONCENTRATION_DEBIT': 'Cash Concentration - Debit','CASH_DISBURSEMENT_DEBIT': 'Cash Disbursement - Debit','CASH_DISBURSEMENT_CREDIT': 'Cash Disbursement - Credit','CASH_PAYROLL_DEBIT': 'Cash Payroll - Debit','CASH_PAYROLL_CREDIT': 'Cash Payroll - Credit',
        'CASH_LOCKBOX': 'CashLockBox', 'CASH_ACCRUALS': 'Cash Accruals', 'NON_BALANCED': 'Out of Balance','ROUNDING_OFF': 'Rounding off',"UNUSUAL_ACCOUNTING_PATTERN": 'unusual accounting pattern', 
        "UNUSUAL_MONETARY_FLOW" : 'unusual monetary flow', "SUSPENSE_ACCOUNT_WITH_CASH": 'suspense account with cash', "SUSPENSE_ACCOUNT_WITH_INVENTORY" : 'suspense account with inventory' ,
        "UNUSUAL_ACCOUNT_PAIRING": 'Unusual Account Pairing'}


        #Different configurations corresponding to rule
        self.frequency_threshold = 1 #Threshold for unusual account pairing
        self.business_hour_start = 7 #None #17:30
        self.business_hour_end = 18 #None #4:30
        self.week_start = 0 #None
        self.week_end = 4 #None 
        self.day_difference = 5
        self.suspicious_keywords = ['accrual', 'alter', 'adjust', 'request', 'audit', 'bonus',
                            'bury', 'cancel', 'capital', 'ceo', 'classif', 'corr', 'correct',
                            'cover', 'director', 'ebit', 'err', 'per', 'screen', 'switch',
                              'revers', 'test', 'transfer']

        #This part is used to use the rule configurations from the database
        self.configs_df = configs
        self.config_initialization()

    def build_gl_rules(self, cols):
        capture_log_message(log_message='Inside build rules function')
        capture_log_message(log_message='Available data columns:{}'.format(cols),store_in_db=False)
        
        #Rule keys and their respective functions to  be called
        existing_rule_functions = {'POSTS_WEEKEND':self.Weekend_Rule,'POSTS_HOLIDAYS':self.Holiday_Rule,'POSTS_NIGHT':self.Late_Night_Posting_Rule,
                                'NEXT_QTR_POSTING':self.Next_Qtr_posting,'BLANK_JE':self.Blank_JE_Rule,'SAME_USER_POSTING':self.Same_User_posting,
                                'SUSPICIOUS_KEYWORDS':self.Suspicious_Keywords,'NON_BALANCED':self.Out_of_Balance_Rule,'ROUNDING_OFF': self.rounding_off,
                                "CASH_CONCENTRATION_CREDIT":self.Cash_Concentration_Credit_Rule,"CASH_CONCENTRATION_DEBIT":self.Cash_Concentration_Debit_Rule,
                                "CASH_DISBURSEMENT_CREDIT":self.Cash_Disbursement_Credit_Rule,"CASH_DISBURSEMENT_DEBIT":self.Cash_Disbursement_Debit_Rule,
        "CASH_PAYROLL_CREDIT":self.CashPayroll_Credit_Rule,"CASH_PAYROLL_DEBIT":self.CashPayroll_Debit_Rule,"CASH_LOCKBOX":self.Cash_Lockbox_Rule,
        "UNUSUAL_ACCOUNTING_PATTERN": self.Unusual_Accounting_Pattern, "UNUSUAL_MONETARY_FLOW" : self.Unusual_Monetary_Flow,"CASH_ACCRUALS":self.Cash_Accrual_Rule,
        "SUSPENSE_ACCOUNT_WITH_CASH": self.Suspense_Account_With_Cash, "SUSPENSE_ACCOUNT_WITH_INVENTORY" : self.Suspense_Account_With_Inventory, 
        'ACCRUAL':self.Accruals_without_Reversals,'REVERSALS':self.Reversals_without_Accruals, "CASH_LOCKBOX_SUSPENSE": self.Cash_Lockbox_Suspense_Account_Rule, 
        'FIREFIGHTER': self.Fire_Fighter_Rule, "CASH_NEGATIVE_BALANCE": self.Cash_Negative_Balance_Rule,"UNUSUAL_ACCOUNT_PAIRING": self.Unusual_Account_Pairing}
 
        self.rule_functions = {}
        self.rule_weights = {}
        
        self.rule_functions = existing_rule_functions

        rules_matrix = pd.read_csv("GL_Module/rules_matrix_GL.csv")
        rules_matrix.columns = rules_matrix.columns.str.strip()
        for key in existing_rule_functions.keys():
            if key in rules_matrix['Rule Name'].values:
                rule_record = rules_matrix[rules_matrix['Rule Name'] == key]
                cols_rule_filt = (rule_record.iloc[0] == 1) & (rule_record.columns != 'SNo')
                cols_used_for_rule = rule_record.columns[cols_rule_filt].tolist()
                capture_log_message(log_message=f'Rule Name: {key} and Cols used for rule:{cols_used_for_rule}')
                needed_cols = set(cols_used_for_rule) - set(g.src_gl_cols)
                if len(needed_cols)==0:
                    # Retrieve weight for the rule from configurations
                    weight_key = f'WEIGHT_{key}'
                    weight_row = self.configs_df[self.configs_df['KEYNAME'] == weight_key]
                    if not weight_row.empty:
                        weight_value = float(weight_row['KEYVALUE'].values[0])
                        self.rule_weights[key] = weight_value
                        capture_log_message(log_message=f'Rule:{key}, Weight: {self.rule_weights[key]}')
                    else:
                        capture_log_message(log_message=f'No weight found for rule: {key}')
                else:
                    # Handle missing required columns
                    capture_log_message(log_message=f'Failed to add rule:{key} to self.rule_weight, needed cols:{needed_cols}')
            else:
                capture_log_message(current_logger=g.error_logger, log_message=f'Rule {key} is not found in Rule matrix csv')
        capture_log_message(log_message=f'Rule_Weights: {self.rule_weights}')



    def config_initialization(self):
        """
        Function used to initialize the above variables with current valus in the database
        """
        configs = dict(zip(self.configs_df.KEYNAME,self.configs_df.KEYVALUE))
        self.configs = configs
        # self.rule_weights = {rulename.split("WEIGHT_")[1]:float(weight) for rulename,weight in self.configs.items() if rulename.startswith("WEIGHT")}
        self.business_hour_start = json.loads(self.configs['LATE_NIGHT_POSTING'])['start']
        self.business_hour_end = json.loads(self.configs['LATE_NIGHT_POSTING'])['end']
        self.week_start = json.loads(self.configs['WEEKEND_TRANSACTION'])['start']
        self.week_end = json.loads(self.configs['WEEKEND_TRANSACTION'])['end']
        self.day_difference = int(self.configs['bufferDays'])
        self.all_holidays = [holiday['value'] for holiday in json.loads(self.configs['holidays'])]
        self.round_off = json.loads(self.configs['roundOff_list'])
        self.Cash_Concentration_account=[str(account) for account in self.configs['CASH_CONCEN_A'].split(",")]
        self.Cash_Concentration_Credit_account_debit_side=[str(account) for account in self.configs['CASH_CONCEN_B'].split(",")]
        self.Cash_Concentration_Debit_account_credit_side=[str(account) for account in self.configs['CASH_CONCEN_C'].split(",")]
        self.Cash_Disbursement_account=[str(account) for account in self.configs['CASH_DB_A'].split(",")]
        self.Cash_Disbursement_Credit_account_debit_side=[str(account) for account in self.configs['CASH_DB_B'].split(",")]
        self.Cash_Disbursement_Debit_account_credit_side=[str(account) for account in self.configs['CASH_DB_C'].split(",")]
        self.CashPayroll_account=[str(account) for account in self.configs['CASH_PR_A'].split(",")]
        self.CashPayroll_credit_account_debit_side =[str(account) for account in self.configs['CASH_PR_B'].split(",")]
        self.CashPayroll_debit_account_credit_side=[str(account) for account in self.configs['CASH_PR_C'].split(",")]
        self.Cash_Lockbox_account=[str(account) for account in self.configs['CASH_LOCKBOX_A'].split(",")]
        self.Cash_Lockbox_account_debit_side =[str(account) for account in self.configs['CASH_LOCKBOX_B'].split(",")]
        self.Cash_Lockbox_account_credit_side=[str(account) for account in self.configs['CASH_LOCKBOX_C'].split(",")] 
        self.unusual_accounting_primary = [str(account) for account in self.configs['unusual_accounting_primary'].split(",")] 
        self.unusual_accounting_secondary = [str(account) for account in self.configs['unusual_accounting_secondary'].split(",")] 
        self.unusual_accounting_pattern_primary_indicator = json.loads(self.configs['unusual_accounting_pattern_primary_indicator'])
        self.unusual_accounting_pattern_secondary_indicator = json.loads(self.configs['unusual_accounting_pattern_secondary_indicator'])
        self.unusual_monetary_primary = [str(account) for account in self.configs['unusual_monetary_primary'].split(",")] 
        self.unusual_monetary_secondary = [str(account) for account in self.configs['unusual_monetary_secondary'].split(",")]  
        self.unusual_monetary_primary_indicator = json.loads(self.configs['unusual_monetary_primary_indicator'])
        self.unusual_monetary_secondary_indicator = json.loads(self.configs['unusual_monetary_secondary_indicator'])
        self.amount_percentage  =  json.loads(self.configs['amount_percentage'])
        self.acceptable_deviation  =  json.loads(self.configs['acceptable_deviation'])
        self.suspense_account_primary = [str(account) for account in self.configs['suspense_account_primary'].split(",")] 
        self.suspense_account_secondary = [str(account) for account in self.configs['suspense_account_secondary'].split(",")] 
        self.inventory_account_primary = [str(account) for account in self.configs['inventory_account_primary'].split(",")] 
        self.inventory_account_secondary = [str(account) for account in self.configs['inventory_account_secondary'].split(",")] 
        self.cash_accounts = [str(account) for account in self.configs['cash_accounts'].split(",")] 
        self.Cash_Lockbox_Suspense_Account = [str(account) for account in self.configs['Cash_Lockbox_Suspense_Account'].split(",")] 
        self.ff_users =  [str(account) for account in self.configs['ff_users'].split(",")] 
        self.Cash_Negative_Balance_account = [str(account) for account in self.configs['Cash_Negative_Balance_account'].split(",")] 
        self.Cash_Negative_Balance_start_date = json.loads(self.configs['Cash_negative_start'])
        self.Cash_Negative_Balance_end_date = json.loads(self.configs['Cash_negative_end'])

    def Weekend_Rule(self,data):
        """
        Flag transactions on weekends 
        Passing 
        """
        
        data['POSTS_WEEKEND'] = np.where(data['ENTERED_DATE'].isnull(),0,np.where(((data['ENTERED_DATE'].dt.dayofweek<self.week_start) | (data['ENTERED_DATE'].dt.dayofweek>self.week_end)),1,0))
        capture_log_message(log_message='Weekend Rule Completed')

    def Holiday_Rule(self,data):
        """
        Flag transactions posted on a holiday
        #Confirmation on Holidays required
        """
       
        data['POSTS_HOLIDAYS'] = np.where(data["DATE_HOLIDAYS"].isin(self.all_holidays),1,0)
        capture_log_message(log_message='Holiday Rule Completed')


    def Late_Night_Posting_Rule(self,data):
        """
        Flag transactions which were posted outside working hours
        Passing
        """
        data['POSTS_NIGHT'] = np.where(data['ENTERED_DATE'].isnull(),0,np.where(((data['ENTERED_DATE'].dt.hour > self.business_hour_end) | (data['ENTERED_DATE'].dt.hour < self.business_hour_start)), 1, 0))
        capture_log_message(log_message='Late Night Posting Rule Completed')

    def Next_Qtr_posting(self, data):
        """
        Flag transactions if it was Accounted in one quarter and Posted in Another. A Buffer period of 'n' days to be given
        Passing
        """
        cols = ['DIFF_POSTING_ACCOUNTING','SAME_QUARTER']
        
        data['DIFF_POSTING_ACCOUNTING'] = np.where(data['DIFF_POSTING_ACCOUNTING'].isnull(),0,data['DIFF_POSTING_ACCOUNTING'].astype(int))
        data['NEXT_QTR_POSTING'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DIFF_POSTING_ACCOUNTING'] >= self.day_difference) & (data['SAME_QUARTER'] == 0), 1, 0))
        capture_log_message(log_message='Next Quarter Posting Rule Completed')
    
    def Blank_JE_Rule(self,data):
        """
        Flag transactions with JE Description column blank 
        Passing
        """
        #replace blank values in transacation_desc column with NaN
        data['BLANK_JE'] = np.where(data['TRANSACTION_DESC'].isna(), 1, 0)
        capture_log_message(log_message='Blank JE Rule Completed')
    
    def Same_User_posting(self, data):
        """
        Flags if the transactions is Posted and Entered by the same person
        Passing
        """
        cols = ['POSTED_BY_USERID','ENTERED_BY_USERID']
        
        data['SAME_USER_POSTING'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['POSTED_BY_USERID'] == data['ENTERED_BY_USERID']), 1, 0))
        capture_log_message(log_message='Same User Posting Rule Completed')
    
    # def Suspicious_Keywords(self,data):
    #     """
    #     Flag is any of suspicious keywords occur in the transaction description or accountdescription
    #     Passing
    #     """
    #     cols = ['TRANSACTION_DESC']
    #     self.suspicious_keywords = json.loads(self.configs['suspicious_words'])

    #     data['SUSPICIOUS_KEYWORDS'] = 0
    #     for word in self.suspicious_keywords:
    #         data['SUSPICIOUS_KEYWORDS'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data.SUSPICIOUS_KEYWORDS) | (data.TRANSACTION_DESC.astype(str).str.lower().str.contains(word)),1,0))

    def Suspicious_Keywords(self,data):
        """
        Flag is any of suspicious keywords occur in the transaction description
        Is there in Config settingss
        """
        cols=['TRANSACTION_DESC']
        self.suspicious_words = json.loads(self.configs['suspicious_words'])

        data['SUSPICIOUS_KEYWORDS'] = 0
        data['SUS_FLAG'] = 0
        for word in self.suspicious_words:
            data['SUS_FLAG'] = np.where(data[cols].isnull().any(axis=1),0,np.where(( data.SUSPICIOUS_KEYWORDS) | (data.TRANSACTION_DESC.astype(str).str.lower().str.contains(word)),1,0))
            data['SUSPICIOUS_KEYWORDS'] = data['SUSPICIOUS_KEYWORDS'] + data['SUS_FLAG']
        data.loc[data['SUSPICIOUS_KEYWORDS'] > 1, ['SUSPICIOUS_KEYWORDS']] = 1
        data.drop(['SUS_FLAG'], axis=1, inplace=True)
        capture_log_message(log_message='Suspicious Keywords Rule Completed')

    def Out_of_Balance_Rule(self,data):
        """
        Accounting Docs where Debit Amount != Credit Amount
        Passing
        """
        data['NON_BALANCED'] =  np.where(np.isclose(data.groupby('ACCOUNTDOCID')['DEBIT_AMOUNT'].transform('sum'),
                                                    data.groupby('ACCOUNTDOCID')['CREDIT_AMOUNT'].transform('sum')),0,1)  
        capture_log_message(log_message='Out of Balance Rule Completed')
                                           
    def Cash_Accrual_Rule(self,data):

        """
        Flags transactions if its a Cash Account and if it is an Accrual Entry Passing
        
        """
        cols=['ACCOUNT_CODE','DOC_TYPE']
        data["CASH_ACCRUALS"] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['ACCOUNT_CODE'].isin(self.cash_accounts)) & (data['DOC_TYPE'] == 'YA'), 1, 0))
        capture_log_message(log_message='Cash Accrual Rule Completed')

    def Accruals_without_Reversals(self, data):

        """
        Flags transactions if an Accrual Entry is made without Reversal Passing

        """
        cols=['DOC_TYPE','IS_REVERSED']

        data['ACCRUAL'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DOC_TYPE'] == 'YA') & (data['IS_REVERSED'] != 1), 1, 0))
        capture_log_message(log_message='Accruals without Reversals Rule Completed')

    def Reversals_without_Accruals(self, data):

        """
        Flags transactions if Reversal is made without Accrual Passing
        """

        cols=['DOC_TYPE','IS_REVERSED']
        data['REVERSALS'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DOC_TYPE'] == 'YR') & (data['IS_REVERSED'] != 1), 1, 0))
        capture_log_message(log_message='Reversals without Accruals Rule Completed')

    def Cash_Lockbox_Suspense_Account_Rule(self, data):

        """
        Flags JE with Cash Lockbox Suspense.
        """

        data['CASH_LOCKBOX_SUSPENSE'] = np.where(data['ACCOUNT_CODE'] .isin(self.Cash_Lockbox_Suspense_Account),1,0)
        capture_log_message(log_message='Cash Lockbox Suspense Account Rule Completed')

    def Fire_Fighter_Rule(self, data):

        """
        List of users called FF users if they have posted then firefighter rule is breached
        """
        data['FIREFIGHTER'] = np.where((data['ENTERED_BY_USERID'].isin(self.ff_users)),1,0)
        capture_log_message(log_message='Fire Fighter Rule Completed')

    def Cash_Negative_Balance_Rule(self, data):

        """
        JE in Cash Negative Balances account is not entered in the beginning of the month.
        """

        data["CASH_NEGATIVE_BALANCE"] = np.where((data['ACCOUNT_CODE'] .isin(self.Cash_Negative_Balance_account)) & (data['ENTERED_DATE'].dt.day.astype(int).between(self.Cash_Negative_Balance_start_date, self.Cash_Negative_Balance_end_date)),1,0)
        capture_log_message(log_message='Cash Negative Balance Rule Completed')

    def Cash_Concentration_Credit_Rule(self, data):
            """
            JE with Cash Concentration account in credit side   are considered.
            """
            flag_list=[]
            for i in set(data[((data['ACCOUNT_CODE'].isin( self.Cash_Concentration_account))) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
                if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] \
                if row not in self.Cash_Concentration_Credit_account_debit_side]:
                    flag_list.append(i)
                else:
                    continue
            data["CASH_CONCENTRATION_CREDIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
            capture_log_message(log_message='Cash Concentration Credit Rule Completed')

    def Cash_Concentration_Debit_Rule(self, data):
        """
        JE with Cash Concentration account in debit side are considered.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'].isin(self.Cash_Concentration_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNT_CODE'] if row not in self.Cash_Concentration_Debit_account_credit_side]:
                flag_list.append(i)
            else:
                continue
        data["CASH_CONCENTRATION_DEBIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Concentration Debit Rule Completed')
        
    def Cash_Disbursement_Credit_Rule(self, data):
        """
        JE with Cash Disbursement account in  credit side are considered.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'] .isin( self.Cash_Disbursement_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] if row not in self.Cash_Disbursement_Credit_account_debit_side]:
                flag_list.append(i)
            else:
                continue
        data["CASH_DISBURSEMENT_CREDIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Disbursement Credit Rule Completed')

    def Cash_Disbursement_Debit_Rule(self, data):
        """
        JE with Cash Disbursement account in debit side are considered.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'].isin(self.Cash_Disbursement_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNT_CODE'] if row not in self.Cash_Disbursement_Debit_account_credit_side]:
                flag_list.append(i)
            else:
                continue
        data["CASH_DISBURSEMENT_DEBIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Disbursement Debit Rule Completed')
       
    def CashPayroll_Credit_Rule(self, data):
        """
        JE with Cash Payroll account in credit side  are considered.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'].isin(self.CashPayroll_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] if row not in self.CashPayroll_credit_account_debit_side]:
                flag_list.append(i)
            else:
                continue
        data["CASH_PAYROLL_CREDIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Payroll Credit Rule Completed')
        

    def CashPayroll_Debit_Rule(self, data):
        """
        JE with Cash Payroll account in debit side are considered.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'].isin(self.CashPayroll_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNT_CODE'] if row not in self.CashPayroll_debit_account_credit_side]:
                flag_list.append(i)
            else:
                continue
        data["CASH_PAYROLL_DEBIT"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Payroll Debit Rule Completed')
        
        
    def Cash_Lockbox_Rule(self, data):
        """
        JE with Cash Lockbox account as Credit are considered if their corresponding debit account does fall under Cash Concentration.
        """
        flag_list=[]
        for i in set(data[(data['ACCOUNT_CODE'].isin(self.Cash_Lockbox_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] if row not in self.Cash_Lockbox_account_debit_side]:
                flag_list.append(i)
            else:
                continue

        for i in set(data[(data['ACCOUNT_CODE'] .isin(self.Cash_Lockbox_account)) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
            if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNT_DESCRIPTION'] if row in self.Cash_Lockbox_account_credit_side]:
                flag_list.append(i)
            else:
                continue

        data["CASH_LOCKBOX"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
        capture_log_message(log_message='Cash Lockbox Rule Completed')
    

    def find_credit_debit_combinations(self, df):
        """This function creates all possible Credit Debit Combinations for the given data"""
        credit_df = df[df['CREDIT_AMOUNT'] > 0].groupby('ACCOUNT_CODE',as_index=False)['CREDIT_AMOUNT'].sum().copy()
        debit_df = df[df['DEBIT_AMOUNT'] > 0].groupby('ACCOUNT_CODE',as_index=False)['DEBIT_AMOUNT'].sum().copy()

        credit_df['CREDIT_AMOUNT'] = credit_df['CREDIT_AMOUNT'].round(2)
        debit_df['DEBIT_AMOUNT'] = debit_df['DEBIT_AMOUNT'].round(2)

        # Merge credit and debit dataframes based on equal amounts
        merged_df = credit_df.merge(debit_df, left_on='CREDIT_AMOUNT', right_on='DEBIT_AMOUNT', suffixes=('_CREDIT', '_DEBIT'))

        # Create pairs of matching GL Accounts
        account_number_pairs = list(zip(merged_df['ACCOUNT_CODE_CREDIT'], merged_df['ACCOUNT_CODE_DEBIT']))
        # Handle remaining unmatched amounts
        remaining_df = df[~df['ACCOUNT_CODE'].isin([id for pair in account_number_pairs for id in pair])]
        if not remaining_df.empty:
            total_credit = round(remaining_df['CREDIT_AMOUNT'].sum(),2)
            total_debit = round(remaining_df['DEBIT_AMOUNT'].sum(),2)
            if total_credit == total_debit:
                credit_accounts = remaining_df[remaining_df['CREDIT_AMOUNT'] > 0]['ACCOUNT_CODE'].unique()
                debit_accounts = remaining_df[remaining_df['DEBIT_AMOUNT'] > 0]['ACCOUNT_CODE'].unique()
                account_number_pairs.extend(list(product(credit_accounts, debit_accounts)))

    
        return account_number_pairs

    #LOGIC:1
    def Unusual_Account_Pairing_Predefined_List(self, data):
        """
        LOGIC 1: Return a pd.Series of 0/1 indicating rows where an unusual 
        credit-debit pairing occurs (per predefined_unusual_dict), ignoring reversals.
        """

        df = data[['ACCOUNTDOC_CODE','ACCOUNT_CODE','DEBIT_AMOUNT','CREDIT_AMOUNT',
               'DEBIT_CREDIT_INDICATOR','IS_REVERSED']].copy()
        
        unusual_series = pd.Series(0, index=data.index)
        
        # Filter out reversal Transactions
        capture_log_message(f"Shape of Data before dropping reversals: {df.shape}")
        non_reversal_df = df[df['IS_REVERSED']==0]
        capture_log_message(f"Shape of Data after dropping reversals: {non_reversal_df.shape}")
        
        # 1. Compute combinations per journal as a Series of lists of tuples
        comb_series = (
            non_reversal_df
            .groupby('ACCOUNTDOC_CODE', sort=False)
            .apply(lambda df: self.find_credit_debit_combinations(df),include_groups=False)
        )

        # 2. Turn that Series into a DataFrame so the journal code becomes a column
        exploded = comb_series.explode()

        # 3. “Explode” the list in each row so each tuple becomes its own row
        exploded = exploded.reset_index(name='pairs')

        # 4. Drop any rows where there was no combination
        exploded = exploded.dropna(subset=['pairs'])

        # 5. Split the tuple in “pairs” into two separate columns
        pairs_df = pd.DataFrame(
            exploded['pairs'].tolist(),
            columns=['CREDIT_ACCOUNT_CODE', 'DEBIT_ACCOUNT_CODE'],
            index=exploded.index
        )

        # 6. Join back the journal codes
        credit_debit_combinations_df = (
            pairs_df.join(exploded[['ACCOUNTDOC_CODE']])
            [['ACCOUNTDOC_CODE','CREDIT_ACCOUNT_CODE','DEBIT_ACCOUNT_CODE']]
            .reset_index(drop=True) )
        
        capture_log_message(f"No of unique journals after calculating cr/db combinations:{credit_debit_combinations_df['ACCOUNTDOC_CODE'].nunique()}")

        coa_data = self.DB.read_table('mschartofaccounts')
        coa_data = coa_data.apply(lambda col: col.map(lambda x: x.strip().lower() if isinstance(x, str) else x))
        capture_log_message(f"COA data:{coa_data.shape}")
        
        acc_number_to_type_mappings = dict(zip(coa_data['ACCOUNT_CODE'],coa_data['ACCOUNT_SUBCATEGORY']))
        
        credit_debit_combinations_df['CREDIT_ACC_TYPE'] = credit_debit_combinations_df['CREDIT_ACCOUNT_CODE'].map(acc_number_to_type_mappings)
        credit_debit_combinations_df['DEBIT_ACC_TYPE'] = credit_debit_combinations_df['DEBIT_ACCOUNT_CODE'].map(acc_number_to_type_mappings)
        
        capture_log_message(f"No of rows in credit_debit_combinations_Df: {credit_debit_combinations_df.shape}")
        credit_debit_combinations_df['UNUSUAL_FLAG'] = credit_debit_combinations_df.apply( lambda x:\
                                                        predefined_unusual_dict.get( (x['CREDIT_ACC_TYPE'],x['DEBIT_ACC_TYPE']),False ),axis=1)
        
        capture_log_message(f"No. of rows that were flagged true: {credit_debit_combinations_df['UNUSUAL_FLAG'].value_counts()}")
        unusual_entries_df = credit_debit_combinations_df[credit_debit_combinations_df['UNUSUAL_FLAG']==True].copy()
        capture_log_message(f"No of accounting docs flagged under unusual accounting module: {unusual_entries_df['ACCOUNTDOC_CODE'].nunique()}")
        
        # OPTIMIZED Flagging
        if not unusual_entries_df.empty:
            # Build maps: account → list of docs where it's flagged as credit/debit
            credit_account_to_docs_map = (
                unusual_entries_df
                .groupby('CREDIT_ACCOUNT_CODE')['ACCOUNTDOC_CODE']
                .apply(list)
                .to_dict()
            )
            debit_account_to_docs_map = (
                unusual_entries_df
                .groupby('DEBIT_ACCOUNT_CODE')['ACCOUNTDOC_CODE']
                .apply(list)
                .to_dict()
            )

            # Flatten into sets of (account, doc) pairs for O(1) lookups
            flagged_pairs_credit = {
                (acc, doc)
                for acc, docs in credit_account_to_docs_map.items()
                for doc in docs
            }
            flagged_pairs_debit = {
                (acc, doc)
                for acc, docs in debit_account_to_docs_map.items()
                for doc in docs
            }

            # Create tuple column for per-row (account, doc)
            acc_and_accdoc_series = pd.Series(zip(data['ACCOUNT_CODE'], data['ACCOUNTDOC_CODE']), index=data.index)

            #Boolean masks for debit/credit anomalies
            unusual_debit_flag = acc_and_accdoc_series.isin(flagged_pairs_debit) & (data['DEBIT_AMOUNT'] > 0)
            unusual_credit_flag = acc_and_accdoc_series.isin(flagged_pairs_credit) & (data['CREDIT_AMOUNT'] > 0)

            # Combine into final flag series
            unusual_series = (unusual_debit_flag | unusual_credit_flag).astype(int)

        flagged_count = unusual_series.sum()
        capture_log_message(f"Total rows flagged as UNUSUAL_ACCOUNT_PAIRING: {flagged_count}")
        capture_log_message('Unusual Account Pairing Rule (Logic 1) Completed')
        return unusual_series

    #LOGIC:2
    def Unusual_Account_Pairing_Frequency_Based(self, data):
        """
        Find out the transactions in the primary account which is not debited or credited to secondary account.
        Uses frequency-based approach to identify unusual account pairings.
        Returns a pandas Series with flags instead of modifying data directly.
        """
        capture_log_message('Unusual Account Pairing Rule Started (Logic 2 - Frequency Based)')

        df = data[['ACCOUNTDOC_CODE','ACCOUNT_CODE',
                    'DEBIT_AMOUNT','CREDIT_AMOUNT','IS_REVERSED']].copy()
        
        unusual_series = pd.Series(0, index=data.index)
        # Filter out reversals
        non_reversal_df = df[df['IS_REVERSED'] == 0]
        capture_log_message(f"Shape of Data after dropping reversals: {non_reversal_df.shape}")
        
        # Use amount-based conditions for better accuracy
        debit_data = non_reversal_df[non_reversal_df['DEBIT_AMOUNT'] > 0]
        credit_data = non_reversal_df[non_reversal_df['CREDIT_AMOUNT'] > 0]

        # Create pairs data efficiently
        debit_subset = debit_data[['ACCOUNTDOC_CODE', 'ACCOUNT_CODE']].rename(columns={'ACCOUNT_CODE': 'ACCOUNT_CODE_DEBIT'})
        credit_subset = credit_data[['ACCOUNTDOC_CODE', 'ACCOUNT_CODE']].rename(columns={'ACCOUNT_CODE': 'ACCOUNT_CODE_CREDIT'})
        
        pairs_data = pd.merge(debit_subset, credit_subset, on="ACCOUNTDOC_CODE")
        pairs_data = pairs_data[["ACCOUNTDOC_CODE", "ACCOUNT_CODE_DEBIT", "ACCOUNT_CODE_CREDIT"]].drop_duplicates()
        
        capture_log_message(f"Pairs data shape: {pairs_data.shape}")

        # Count frequencies of each account pair
        pair_frequencies = (
            pairs_data
            .groupby(['ACCOUNT_CODE_DEBIT', 'ACCOUNT_CODE_CREDIT'])['ACCOUNTDOC_CODE']
            .agg( ACCOUNTDOC_CODE_count='size')
            .reset_index()
        )
        capture_log_message(f'No of unique pairs: {pair_frequencies.shape}')

        # Identify unusual pairs
        unusual_pairs = pair_frequencies[
            (pair_frequencies["ACCOUNTDOC_CODE_count"] <= self.frequency_threshold) &
            (pair_frequencies["ACCOUNT_CODE_DEBIT"] != pair_frequencies["ACCOUNT_CODE_CREDIT"])
        ]
        capture_log_message(f'Below threshold pairs: {unusual_pairs.shape}')

        # OPTIMIZED FLAGGING - Vectorized approach
        if len(unusual_pairs) > 0:
            # Get all document-account combinations for unusual pairs
            unusual_docs_detailed = (
                pairs_data.merge(
                    unusual_pairs[['ACCOUNT_CODE_DEBIT', 'ACCOUNT_CODE_CREDIT']], 
                    on=['ACCOUNT_CODE_DEBIT', 'ACCOUNT_CODE_CREDIT']
                )
            )
            unique_unusual_docs = unusual_docs_detailed["ACCOUNTDOC_CODE"].nunique()
            capture_log_message(f"Unique ACCOUNTDOC_CODEs involved in unusual pairs: {unique_unusual_docs}")

            # Build account number→acc_docs maps for debit & credit
            debit_account_to_docs_map = (
                unusual_docs_detailed
                .groupby('ACCOUNT_CODE_DEBIT')['ACCOUNTDOC_CODE']
                .apply(list)
                .to_dict()
            )
            credit_account_to_docs_map = (
                unusual_docs_detailed
                .groupby('ACCOUNT_CODE_CREDIT')['ACCOUNTDOC_CODE']
                .apply(list)
                .to_dict()
            )
            
            # Flatten to sets of (account,acc_doc) for O(1) lookup
            flagged_pairs_debit = {
                (acc,doc) for acc,docs in debit_account_to_docs_map.items() for doc in docs
            }
            flagged_pairs_credit = {
                (acc,doc) for acc,docs in credit_account_to_docs_map.items() for doc in docs
            }
            # Create tuple column for per-row (account, doc)
            acc_and_accdoc_series = pd.Series(zip(data['ACCOUNT_CODE'], data['ACCOUNTDOC_CODE']), index=data.index)

            #Boolean masks for debit/credit anomalies
            unusual_debit_flag = acc_and_accdoc_series.isin(flagged_pairs_debit) & (data['DEBIT_AMOUNT'] > 0)
            unusual_credit_flag = acc_and_accdoc_series.isin(flagged_pairs_credit) & (data['CREDIT_AMOUNT'] > 0)

            # Combine into final flag series
            unusual_series = (unusual_debit_flag | unusual_credit_flag).astype(int)
            flagged_transactions = unusual_series.sum()
            flagged_docs = data[unusual_series == 1]['ACCOUNTDOC_CODE'].nunique()
            
        else:
            flagged_transactions = 0
            flagged_docs = 0
        
        capture_log_message(f'Total transactions flagged as unusual (Logic 2): {flagged_transactions}')
        capture_log_message(f'Unique accounting docs with unusual pairings: {flagged_docs}')
        capture_log_message(f'Total unusual account pairs identified: {len(unusual_pairs)}')
        capture_log_message('Unusual Account Pairing Rule Completed (Logic 2 - Frequency Based)')
        return unusual_series
    

    def Unusual_Account_Pairing(self, data):
        """
        Main function that combines both logics for unusual account pairing.
        Calls both logic functions, gets their flag series, and combines them using AND operation.
        """
        capture_log_message('Unusual Account Pairing Main Function Started')
        
        # Get flag series from both logics
        logic1_flags = self.Unusual_Account_Pairing_Predefined_List(data)
        logic2_flags = self.Unusual_Account_Pairing_Frequency_Based(data)
        
        # Combine using AND operation (both logics must flag for final flag)
        # Change to OR operation if you want either logic to flag: logic1_flags | logic2_flags
        combined_flags = logic1_flags | logic2_flags
        
        # Set the final flag in the data
        data['UNUSUAL_ACCOUNT_PAIRING'] = combined_flags.astype(int)
        
        # Log combined results
        total_flagged = data['UNUSUAL_ACCOUNT_PAIRING'].sum()
        flagged_docs = data[data['UNUSUAL_ACCOUNT_PAIRING'] == 1]['ACCOUNTDOC_CODE'].nunique()
        
        capture_log_message(f'Logic 1 flagged transactions: {logic1_flags.sum()}')
        capture_log_message(f'Logic 2 flagged transactions: {logic2_flags.sum()}')
        capture_log_message(f'Combined (OR) flagged transactions: {total_flagged}')
        capture_log_message(f'Unique accounting docs flagged: {flagged_docs}')
        capture_log_message('Unusual Account Pairing Main Function Completed')
    

    def mark_unusual_activity_using_MAD(self, df ,column_name ,threshold=3.5, amount_threshold = 1000000):
        values = df[column_name].values
        min_val = np.min(values)
        max_val = np.max(values)
        median = np.median(values)
        if median!=0:
            spread_ratio = (max_val - min_val) / median
        else:
            spread_ratio = 0
        # print('median',median)
        # print('spread ratio',spread_ratio)
        if spread_ratio < 0.2:
            df['mad_label'] = False
            return df
        mad = np.median(np.abs(values - median))
        # print('mad',mad)
        if mad ==0:
            is_outlier = (values != median) & ((np.abs(values - median) / median) >= 0.5)
            df['mad_label'] = is_outlier
            return df
        
        # The constant 0.6745 makes MAD comparable to std dev for normal data.
        modified_z = 0.6745 * (values - median) / mad
        # print('modified z score',modified_z)
        is_outlier = np.abs(modified_z) > threshold

        if amount_threshold is not None:
            is_outlier = is_outlier & (np.abs(values)> amount_threshold)
        df['mad_label'] = is_outlier
        df['modified_z'] = modified_z
        return df
    

    def unsual_account_pattern_based_on_account_numbers(self, df):

        # Seperate credit rows and debit rows
        credit_df = df[df['CREDIT_AMOUNT']>0]
        debit_df = df[df['DEBIT_AMOUNT']>0]
        
        # Get list of all credit and debit account numbers
        list_of_credit_accounts = credit_df['ACCOUNT_CODE'].unique()
        list_of_debit_accounts = debit_df['ACCOUNT_CODE'].unique()

        unusual_docs_for_credit_numbers = {}
        unusual_docs_for_debit_numbers = {}
        
        # Store all unique account numbers 
        list_of_unique_accounts = set(list(list_of_credit_accounts)+list(list_of_debit_accounts))
        for account_num in list_of_unique_accounts:
            credit_rows = credit_df[credit_df['ACCOUNT_CODE']==account_num]
            debit_rows = debit_df[debit_df['ACCOUNT_CODE']==account_num]
            if credit_rows.shape[0]!=0:
                doc_wise_df = credit_rows.groupby('ACCOUNTDOC_CODE').agg({'CREDIT_AMOUNT':'sum'})
                res = self.mark_unusual_activity_using_MAD(doc_wise_df,'CREDIT_AMOUNT',4.5)
                if res['mad_label'].any():
                    unusual_amounts = res[res['mad_label']==True]
                    unusual_docs_for_credit_numbers[account_num]=unusual_amounts.index.values

            if debit_rows.shape[0]!=0:
                doc_wise_df = debit_rows.groupby('ACCOUNTDOC_CODE').agg({'DEBIT_AMOUNT':'sum'})
                res = self.mark_unusual_activity_using_MAD(doc_wise_df,'DEBIT_AMOUNT',4.5)
                if res['mad_label'].any():
                    unusual_amounts = res[res['mad_label']==True]
                    unusual_docs_for_debit_numbers[account_num]=unusual_amounts.index.values

        return unusual_docs_for_credit_numbers, unusual_docs_for_debit_numbers
                    
    
    def Unusual_Accounting_Pattern(self, data):
        """
        Find out the transactions in the primary account which is not debited or credited to secondary account.
        """
        res_credit, res_debit = self.unsual_account_pattern_based_on_account_numbers(data.copy())
        
        # Flatten to sets of (account,acc_doc) for O(1) lookup
        flagged_pairs_debit = {(acc, doc) for acc, docs in res_debit.items() for doc in docs}
        flagged_pairs_credit = {(acc, doc) for acc, docs in res_credit.items() for doc in docs}
        
        # Create tuple column for pairwise matching
        data['ACC_DOC_PAIR'] = list(zip(data['ACCOUNT_CODE'], data['ACCOUNTDOC_CODE']))

        #Vectorized flaging
        unusual_debit_flag = data['ACC_DOC_PAIR'].isin(flagged_pairs_debit) & (data['DEBIT_AMOUNT'] > 0)
        unusual_credit_flag = data['ACC_DOC_PAIR'].isin(flagged_pairs_credit) & (data['CREDIT_AMOUNT'] > 0)
        
        # Combine into final flag series
        data['UNUSUAL_ACCOUNTING_PATTERN'] = (unusual_debit_flag | unusual_credit_flag).astype(int)

        # Clean up temporary column
        data.drop(columns=['ACC_DOC_PAIR'], inplace=True)
        capture_log_message(log_message='Unusual Accounting Pattern Rule Completed')

    # def Unusual_Accounting_Pattern(self, data):
    #         """
    #         Find out the transactions in the primary account which is not debited or credited to secondary account.
    #         """
    #         flag_list=[]

    #         for i in set(data[((data['ACCOUNT_CODE'].isin( self.unusual_accounting_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_accounting_pattern_primary_indicator[0])]['ACCOUNTDOCID']):
    #             if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_accounting_pattern_secondary_indicator[0])]['ACCOUNT_CODE'] \
    #             if row in self.unusual_accounting_secondary]:
    #                 flag_list.append(i)
    #             else:
    #                 continue
    #         for i in set(data[((data['ACCOUNT_CODE'].isin( self.unusual_accounting_secondary))) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_accounting_pattern_secondary_indicator[0])]['ACCOUNTDOCID']):
    #             if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_accounting_pattern_primary_indicator[0])]['ACCOUNT_CODE'] \
    #             if row in self.unusual_accounting_primary]:
    #                 flag_list.append(i)
    #             else:
    #                 continue
    #         data["UNUSUAL_ACCOUNTING_PATTERN"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
    #         capture_log_message(log_message='Unusual Accounting Pattern Rule Completed')
   
    def Unusual_Monetary_Flow(self, data):
            """
            Find out the transactions in the primary account which is not debited or credited to secondary account.
            """
            flag_list = []
            for i in set(data[((data['ACCOUNT_CODE'].isin(self.unusual_monetary_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_monetary_primary_indicator[0])]['ACCOUNTDOCID']):
                if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == self.unusual_monetary_secondary_indicator[0])]['ACCOUNT_CODE'] if row in self.unusual_monetary_secondary]:
                    trans_id = []
                    trans_id = (data[(data['ACCOUNTDOCID'] == i)]["TRANSACTIONID"].values)

                    try:
                        for tran in trans_id:
                            # print(tran)
                            for row in data[(data['TRANSACTIONID'] == tran)]['ACCOUNT_CODE']:
                                if row in self.unusual_monetary_primary:
                                    primary_amount = data[(data['TRANSACTIONID'] == tran)]["AMOUNT"].values

                            for row in data[(data['TRANSACTIONID'] == tran)]['ACCOUNT_CODE']:
                                if row in self.unusual_monetary_secondary:
                                    secondary_amount = data[(data['TRANSACTIONID'] == tran)]["AMOUNT"].values
                            try:
                                amount_diff = (secondary_amount/primary_amount)*100
                                expected_diff = self.amount_percentage
                                amount_tol = isclose(amount_diff,expected_diff, rel_tol=self.acceptable_deviation)
                                if amount_tol:
                                    flag_list.append(i)
                            except:
                                continue
                    except:
                        continue

            # for i in set(data[((data['ACCOUNT_CODE'].isin(self.unusual_monetary_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
            #     if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] if row in self.unusual_monetary_secondary]:
            #         trans_id = []
            #         trans_id = (data[(data['ACCOUNTDOCID'] == i)]["TRANSACTIONID"].values)

            #         try:
            #             for tran in trans_id:
            #                 # print(tran)
            #                 for row in data[(data['TRANSACTIONID'] == tran)]['ACCOUNT_CODE']:
            #                     if row in self.unusual_monetary_primary:
            #                         primary_amount = data[(data['TRANSACTIONID'] == tran)]["AMOUNT"].values

            #                 for row in data[(data['TRANSACTIONID'] == tran)]['ACCOUNT_CODE']:
            #                     if row in self.unusual_monetary_secondary:
            #                         secondary_amount = data[(data['TRANSACTIONID'] == tran)]["AMOUNT"].values
            #                 try:
            #                     amount_diff = (secondary_amount/primary_amount)*100
            #                     expected_diff = self.amount_percentage
            #                     amount_tol = isclose(amount_diff,expected_diff, rel_tol=self.acceptable_deviation)
            #                     if amount_tol:
            #                         flag_list.append(i)
            #                 except:
            #                     continue
            #         except:
            #             continue
            data["UNUSUAL_MONETARY_FLOW"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
            capture_log_message(log_message='Unusual Monetary Flow Rule Completed')
            
    def Suspense_Account_With_Cash(self, data):
            """
            Identify the cash entries (debit or credit ) transacted with SUSPENSE account.
            """
            flag_list=[]
            # print(self.suspense_account_primary)
            # print(self.suspense_account_secondary)
            # print(data['ACCOUNT_CODE'].dtype)
            flag_list=[]
            for i in set(data[((data['ACCOUNT_CODE'].isin(self.suspense_account_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
                # print(i)
                # print(data[data['ACCOUNTDOCID'] == i]['DEBIT_CREDIT_INDICATOR'])
                if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] if row in self.suspense_account_secondary]:
                
                    # print(i)
                    flag_list.append(i)
                else:
                    continue
            for i in set(data[((data['ACCOUNT_CODE'].isin(self.suspense_account_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNTDOCID']):
                if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNT_CODE'] \
                if row in self.suspense_account_secondary]:
                    # print(i)
                    flag_list.append(i)
                else:
                    continue
            data["SUSPENSE_ACCOUNT_WITH_CASH"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
            capture_log_message(log_message='Suspense Account With Cash Rule Completed')
            
    def Suspense_Account_With_Inventory(self, data):
            """
            Identify the cash entries (debit or credit ) transacted with SUSPENSE account.
            """
            flag_list=[]
            for i in set(data[((data['ACCOUNT_CODE'].isin( self.inventory_account_primary))) & (data['DEBIT_CREDIT_INDICATOR'] == "C")]['ACCOUNTDOCID']):
                if [row for row in data[(data['ACCOUNTDOCID'] == i) & (data['DEBIT_CREDIT_INDICATOR'] == "D")]['ACCOUNT_CODE'] \
                if row in self.inventory_account_secondary]:
                    flag_list.append(i)
                else:
                    continue  
            data["SUSPENSE_ACCOUNT_WITH_INVENTORY"] = np.where(data['ACCOUNTDOCID'].isin(flag_list),1,0)
            capture_log_message(log_message='Suspense Account With Inventory Rule Completed')
  
    def rounding_off(self,data):
        """
        Last 3 digits: This entry’s monetary value does not end in “0.00” or “9.99”

        """
        data["ROUNDING_OFF"] = np.where(data['AMOUNT'].isnull(),0,np.where(data["ROUND_OFF"].isin(self.round_off),1,0)) 
        capture_log_message(log_message='Rounding Off Rule Completed')   
    

    def Rule_Scores_Calculation(self,data):
        """
        Function to calculate the Rule Score

        """
        #Finding the weighted Rules score for each transactions
        data['RULES_RISK_SCORE_RAW'] = 0
        for rulename,weight in self.rule_weights.items():
            data['RULES_RISK_SCORE_RAW']+= data[rulename]*weight

        #indexed score
        data['RULES_RISK_SCORE'] = data['RULES_RISK_SCORE_RAW']/data['RULES_RISK_SCORE_RAW'].max()

        #description of breached rules per transaction
        data['CONTROL_DEVIATION'] =""
        # print(self.rule_weights.keys())
        for col in self.rule_weights.keys():
            data['CONTROL_DEVIATION']+= np.where(data[col]>0,self.rule_names[col]+",","")
            
        capture_log_message(log_message='Rule Score Calculation Completed')
        
        return data

    def Rule_Score_Calculation_AccountDOC(self,data):
        """
        Calculating Rules score at AccountingDoc level
        """
        capture_log_message(log_message='Account Doc Level Rules Score Calculation Started')

        rules = list(self.rule_weights.keys())
        agg_dict = {rule: "max" for rule in rules}
        agg_dict.update({"RULES_RISK_SCORE_RAW":   "max",
                         "RULES_RISK_SCORE":       "max", 
                         "BLENDED_RISK_SCORE_RAW": "max", 
                         "BLENDED_RISK_SCORE":     "max", 
                         "DEVIATION":              "max" })
        rules_accountdoc = data.groupby('ACCOUNTDOCID',as_index=False).agg(agg_dict)
        
        # control deviation calculation
        comm_rule_names = np.array([self.rule_names[key] for key in rules])
        mask_df = rules_accountdoc[rules].values > 0
        rules_accountdoc['CONTROL_DEVIATION'] = [", ".join(comm_rule_names[mask]) for mask in mask_df ]
        capture_log_message(log_message='Account Doc Level Rules Score Calculation Completed')
        return rules_accountdoc

    def Run_Rules(self,data):
        """
        Function to facilitate whole Rule scoring
        ----------------------------------------
        Input : Data to be scored
        Output : Dataframe with Transactionid,flags of each rules, and calculated scores
        """

        #data preparation
        Prep = Preparation()
        data = Prep.Data_Prep_for_Rules(data)
        self.build_gl_rules(data.columns)
        # print(data["ENTERED_DATE"].dtype)
        #Running the enabled rules based on the rule keys
        capture_log_message(log_message='Rules Calculation Started')
        for rule in self.rule_weights.keys():
            capture_log_message(log_message='Rule Calculation Started for {rule}'.format(rule=rule))
            self.rule_functions[rule](data)
        #suppressing scores of  transactions based on request
        # self.suppression_rule(data)
        capture_log_message(log_message='Rules Calculation Completed')
        capture_log_message(log_message='Rule Score Calculation Started')

        data = self.Rule_Scores_Calculation(data)
        df_rules_scored = data.copy()

        # df_rules_scored.to_csv('/home/whirldata/projects/May6_latest_code_AP_IV_VM/May6_latest_code/GL_Module/csv_results/GL_Rules_result.csv',index=False)
        capture_log_message(log_message='Rule Score Calculation Completed')

        id_column = ['TRANSACTIONID','ACCOUNTDOCID']
        rule_cols = [rulename for rulename,weight in self.rule_weights.items()]
        scores_column = ['CONTROL_DEVIATION','RULES_RISK_SCORE_RAW','RULES_RISK_SCORE']
        
        data = data[id_column+rule_cols+scores_column]
        # print(data[data["ROUNDING_OFF"]==1]["ACCOUNTDOCID"])
        return data, df_rules_scored

def main():

    DB = MySQL_DB('DB.json')
    connection = DB.connect_to_database()
    start_db_read = datetime.now(timezone.utc)
    df= pd.read_sql("""select tran.TRANSACTIONID,tran.TRANSACTION_CODE,tran.TRANSACTION_DESC,tran.ACCOUNTID,tran.ACCOUNTDOCID,tran.ENTERED_BY as ENTERED_BY_USERID,tran.ENTERED_DATE,
    tran.POSTED_DATE,tran.DOCTYPEID,tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,
    coa.ACCOUNT_CODE,coa.ACCOUNT_DESCRIPTION,
    doc.DOCUMENT_TYPE_CODE as DOC_TYPE,doc.DOCUMENT_TYPE_DESCRIPTION, 
    msfu.FMSUSERID as POSTED_BY_USERID
    from rpttransaction tran
    left join msfmsuser msfutr on tran.ENTERED_BY = msfutr.FMSUSERID
    left join mschartofaccounts coa on tran.ACCOUNTID=coa.ACCOUNTID
    left join msdocumenttype doc on tran.DOCTYPEID=doc.DOCUMENTID
    left join rptaccountdocument accdoc on tran.ACCOUNTDOCID=accdoc.ACCOUNTDOCID
    left join msfmsuser msfu on accdoc.POSTED_BY = msfu.FMSUSERID;
    """,con=connection)

    configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='framework' and STATUS=1",con=connection)
    # print(configurations)
    finish_db_read = datetime.now(timezone.utc)
    capture_log_message(log_message='Time Taken for Reading {shape} Dimensioned Dataframe {time}'.format(shape=df.shape,time=finish_db_read-start_db_read))
    connection.close()
    configs = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
    Rules = Rules_Framework(configs)
    Rules_scored = Rules.Run_Rules(df)
    #save Rules scored data as csv in csv_files folder
    # Rules_scored.to_csv(os.path.join('csv_files',"Rules_scored.csv"),index=False)

    for i in Rules_scored.columns:
        capture_log_message(log_message=Rules_scored[i].value_counts())
   

if __name__ == "__main__":
    main()
