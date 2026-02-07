import pandas as pd 
import numpy as np
import json
import itertools
import time
# from fuzzywuzzy import fuzz
# from rapidfuzz import process, fuzz
from flask import g
from AP_Module.db_connector import MySQL_DB
from collections import OrderedDict
from datetime import datetime, timezone
from AP_Module.Data_Preparation import Preparation
from AP_Module.Approval_Matrix.main import process
from code1.logger import capture_log_message
from code1.src_load import fetch_iv_module_data, df_to_dict, build_payment_terms_dict


class Rules_Framework():
    
    def __init__(self,configurations,vend,columns,ap_vendorlist):
        
        """
        Initializing Variables for class
        """
        self.rule_weights ={'LATE_PAYMENT':6,'EARLY_POSTED_INVOICES':4}#'POSTING_PERIOD':6,
        # self.rule_weights = {'POSTING_PERIOD':6}
        #'DUPLICATE_INVOICE_POSTING':8, 'INVOICE_VALUE_OF_SUPPLIER':6,'UNUSUAL_VENDOR':6,'MANUAL_DEBIT_OR_CREDIT_NOTES':6,'ROUNDING_OFF':3,'TRANSACTION_TEXT_EMPTY':3,'DUPLICATE_INVOICE_POSTING':8,'DUPLICATE_INVOICE_POSTING':5,
        
        # self.rule_functions = {'LATE_PAYMENT':self.Late_Payment,'UNFAVORABLE_PAYMENT_TERMS':self.Unfavorable_Payment_Terms,'SUSPICIOUS_KEYWORD':self.Suspicious_Keywords,'VENDOR_MASTER_CHANGES' : self.vendor_master_changes,
        #                        'IMMEDIATE_PAYMENTS':self.Immediate_Payments,'POSTING_PERIOD':self.Posting_Period,'INVOICE_CREATED_AFTER_ACCOUNTING_DATE':self.late_invoice_creation,
        #                        'LOST_DISCOUNT':self.Lost_Discount,'HIGH_VALUE_DEBIT_CREDIT_NOTES': self.high_value_debit_credit_notes,'NON_PO_INVOICE': self.non_po_invoices,
        #                        'WORK_FLOW_DEVIATION':self.Work_Flow_Deviation,'INVOICES_WITHOUT_GRN':self.invoices_without_GRN, 'DIFFERENCE_IN_INVOICE_QUANTITY':self.invoice_po_quantity_mismatch,
        #                        'DIFFERENCE_IN_INVOICE_PRICE':self.invoice_po_price_mismatch,'ADVANCE_PAYMENT_AGAINST_INVOICES':self.advance_payment_invoices,'CASH_REIMBURSEMENTS':self.cash_reimbursements,
        #                        'CASH_EXPENSES':self.cash_expenses,'EARLY_POSTED_INVOICES':self.early_posted_invoices,'OLD_UNPAID_INVOICE' : self.Old_Unpaid_Invoice,'DATE_SEQUENTIAL_MISMATCH': self.Date_Sequential_Mismatch,
        #                        'APPROVAL_MATRIX':self.Approval_Matrix_Rule,'PAYMENT_BLOCK_R':self.payment_r_block
        #                       }

                              
                            # 'INVOICE_VALUE_OF_SUPPLIER':self.invoice_exceeds_range,'UNUSUAL_VENDOR':self.Unusual_Vendor,'MANUAL_DEBIT_OR_CREDIT_NOTES':self.manual_debit_credit_notes,
                            # 'ROUNDING_OFF':self.rounding_off,'TRANSACTION_TEXT_EMPTY':self.blank_je, 'DUPLICATE_INVOICE_POSTING':self.Duplicate_Invoices} #'DUPLICATE_INVOICE_POSTING':self.Duplicate_Invoices,
        self.configs = configurations
        self.rule_functions = {'LATE_PAYMENT':self.Late_Payment,'EARLY_POSTED_INVOICES':self.early_posted_invoices}
        # self.rule_functions = {'POSTING_PERIOD':self.Posting_Period}
        # self.build_rules(columns)
        self.suspicious_words = ["Cancel", "Closed","Partial","split","wrong posting","duplicate","move","rejection","reversal","accrual","alter","adj","adjust","request","audit","bonus","bury","cancel","capital","ceo","classif","corr","correct","cover","director","ebit","err","per","screen","switch","revers","test","transfer","EPAY"]
        self.shorter_credit  = None
        self.immediate_payments  = None 
        self.old_unpaid_invoice = None  
        self.Date_Sequence = ['REQUISITION_DATE','TRANSPORTATION_DATE','DUE_DATE']
        self.early_posted_invoices_doctype = ["IN", "CM", "DM"]
        # self.configs = configurations
        self.config_initialization()
        self.approved_vendors = vend
        self.vendor_list = ['ABC']
        self.rev_pay_types = ['W2','Y1','Y2','Y3','Y4','Y5','KG','AB','ZI','ZW','ZC','ZN','ZA','ZK','ZP','ZE']
        self.strip_list = []
        
        vendor_data = df_to_dict(ap_vendorlist[['VENDORID','payment_terms']].copy(), id_col='VENDORID', value_col='payment_terms')
        iv_data = df_to_dict(fetch_iv_module_data(fields=['payment_terms'],vendor_ids=ap_vendorlist['VENDORID'].unique().tolist()), id_col='VENDORID', value_col='payment_terms')
        self.vendor_dict = build_payment_terms_dict(data_dict=vendor_data)
        # self.vendor_dict = {k: [str(term).strip().lower() for val in v if (term:= payment_terms_extraction(val)[2]) is not None] for k, v in vendor_data.items()}
        self.iv_dict = build_payment_terms_dict(data_dict=iv_data)
        # self.iv_dict = {k: [str(term).strip().lower() for val in v if (term := payment_terms_extraction(val)[2]) is not None] for k, v in iv_data.items()}


    def build_rules(self, cols):
        capture_log_message(log_message='Inside build rules function')
        capture_log_message(log_message='Available data columns:{}'.format(cols),store_in_db=False)
        
        # 'WORK_FLOW_DEVIATION':self.Work_Flow_Deviation,'ADVANCE_PAYMENT_AGAINST_INVOICES':self.advance_payment_invoices,
        # 'CASH_EXPENSES':self.cash_expenses,'CASH_REIMBURSEMENTS':self.cash_reimbursements,
        existing_rule_functions = {'LATE_PAYMENT':self.Late_Payment,'UNFAVORABLE_PAYMENT_TERMS':self.Unfavorable_Payment_Terms,'SUSPICIOUS_KEYWORD':self.Suspicious_Keywords,'VENDOR_MASTER_CHANGES' : self.vendor_master_changes,
                               'IMMEDIATE_PAYMENTS':self.Immediate_Payments,'POSTING_PERIOD':self.Posting_Period,'INVOICE_CREATED_AFTER_ACCOUNTING_DATE':self.late_invoice_creation,
                               'LOST_DISCOUNT':self.Lost_Discount,'HIGH_VALUE_DEBIT_CREDIT_NOTES': self.high_value_debit_credit_notes,'NON_PO_INVOICE': self.non_po_invoices,
                               'INVOICES_WITHOUT_GRN':self.invoices_without_GRN, 'DIFFERENCE_IN_INVOICE_QUANTITY':self.invoice_po_quantity_mismatch,
                               'DIFFERENCE_IN_INVOICE_PRICE':self.invoice_po_price_mismatch,
                               'EARLY_POSTED_INVOICES':self.early_posted_invoices,'OLD_UNPAID_INVOICE' : self.Old_Unpaid_Invoice,'DATE_SEQUENTIAL_MISMATCH': self.Date_Sequential_Mismatch,
                               'APPROVAL_MATRIX':self.Approval_Matrix_Rule,'PAYMENT_BLOCK_R':self.payment_r_block,
                               'DUPLICATE_INVOICE_POSTING':self.Duplicate_Invoices,'EARLY_PAYMENT':self.Early_Payment, '3_WAY_MATCHING':self.three_way_matching
                              }
        self.rule_functions = {}
        self.rule_weights = {}
        
        self.rule_functions = existing_rule_functions

        rules_matrix = pd.read_csv("AP_Module/rules_matrix_AP.csv")
        rules_matrix.columns = rules_matrix.columns.str.strip()
        for key in existing_rule_functions.keys():
            if key in rules_matrix['Rule Name'].values:
                rule_record = rules_matrix[rules_matrix['Rule Name'] == key]
                cols_rule_filt = (rule_record.iloc[0] == 1) & (rule_record.columns != 'SNo')
                cols_used_for_rule = rule_record.columns[cols_rule_filt].tolist()
                capture_log_message(log_message=f'Rule Name: {key} and Cols used for rule:{cols_used_for_rule}')
                needed_cols = set(cols_used_for_rule) - set(g.src_ap_cols)
                if len(needed_cols)==0:
                    # Retrieve weight for the rule from configurations
                    weight_key = f'WEIGHT_{key}'
                    weight_row = self.configs[self.configs['KEYNAME'] == weight_key]
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

        # for key in existing_rule_functions.keys():
        #     rule_record = rules_matrix[rules_matrix['Rule Name']==key]
        #     tgt = 1
        #     mask = rule_record == tgt
        #     existing_cols = list(rule_record.columns[mask.any()])
        #     existing_cols = [existing_col.strip() for existing_col in existing_cols]
        #     # Remove SNo column for list
        #     existing_cols = [each for each in existing_cols if each!='SNo']
        #     if set(existing_cols) <= set(cols):
        #         self.rule_functions[key] = existing_rule_functions[key]
        #         self.rule_weights[key] = 5
        #     else:
        #         capture_log_message(log_message='Failed to add rule:{} to self.rule function, needed cols:{}'.format(key,set(existing_cols)))

    def config_initialization(self):
        """
        Function to read from Configuraitons
        """
        
        def safe_json_load(value, default):
            """Helper function to safely parse JSON values that might already be Python objects"""
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return default
            return value if value is not None else default
        
        configs = dict(zip(self.configs.KEYNAME,self.configs.KEYVALUE))
        capture_log_message(f'DB config settings:{configs}',store_in_db=False)
        # self.rule_weights = {rulename.split("WEIGHT_")[1]:float(weight) for rulename,weight in configs.items()if rulename.startswith("WEIGHT")} 
        self.suspicious_words = safe_json_load(configs.get('suspicious_words',[]),[])  
        self.shorter_credit = safe_json_load(configs.get('shorter_credit',[]),[]) 
        self.immediate_payment_percent = safe_json_load(configs.get('immediate_payments',0),0)/100
        self.unpaid_threshold_days = safe_json_load(configs.get('old_unpaid_invoice',0),0)
        self.Date_Sequence = safe_json_load(configs.get('date_Sequence',[]),[])
        self.high_value_dm_cm = safe_json_load(configs.get('high_value_dm_cm',[]),[])
        self.non_po_invoices_doctype = safe_json_load(configs.get('non_po_invoices_doctype',[]),[])
        self.po_invoices_doctype = safe_json_load(configs.get('po_invoices_doctype',[]),[])
        # self.non_po_invoices_acc_num = safe_json_load(configs.get('non_po_invoices_acc_num',[]),[])
        self.non_po_invoices_amount = safe_json_load(configs.get('non_po_invoices_amount',[]),[])
        # self.advance_payment_invoices_doctype = safe_json_load(configs.get('advance_payment_invoices_doctype',[]),[])
        # self.advance_payment_invoices_amount = safe_json_load(configs.get('advance_payment_invoices_amount',[]),[])
        # self.cash_expenses_doctype = safe_json_load(configs.get('cash_expenses_doctype',[]),[])
        # self.cash_expenses_acc_num = safe_json_load(configs.get('cash_expenses_acc_num',[]),[])
        # self.cash_expenses_amount = safe_json_load(configs.get('cash_expenses_amount',[]),[])
        # self.cash_reimbursement_doctype = safe_json_load(configs.get('cash_reimbursement_doctype',[]),[])
        # self.cash_reimbursement_acc_num = safe_json_load(configs.get('cash_reimbursement_acc_num',[]),[])
        # self.cash_reimbursement_amount = safe_json_load(configs.get('cash_reimbursement_amount',[]),[])
        self.early_posted_invoices_doctype = safe_json_load(configs.get('early_posted_invoices_doctype',[]),[])        
        self.invoices_without_grn_account_num = safe_json_load(configs.get('invoices_without_grn_account_num',[]),[])
        # self.work_flow_deviation_account_num = safe_json_load(configs.get('work_flow_deviation_account_num',[]),[])
         #self.manual_entry_users = safe_json_load(configs.get('manual_entry_users',[]),[])
        self.vendor_list = safe_json_load(configs.get('vendor_list',[]),[])
        self.round_off = safe_json_load(configs.get('round_off',[]),[])


            
    # def Excess_Payment(self,data):
    #     """
    #     Flag if Payment amount exceeds invoice amount
    #     Done
    #     """
    #     cols=['PAYMENT_AMOUNT','INVOICE_AMOUNT','DISCOUNT_PERCENT']
        
    #     data['EXCESS_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['PAYMENT_AMOUNT']>data['INVOICE_AMOUNT']),1,0)) #& (data['DISCOUNT_PERCENT']<=0)

    def Unfavorable_Payment_Terms(self,data):
        """
        Flag if Cash Payments are done within short span
        Configurable number of days - Is there in Config Settings
        & Amount to be added in the Configuration
        """
        capture_log_message(log_message='Unfavorable Payment Terms Rule Started')
        # capture_log_message(log_message=f"vendor_dict: {self.vendor_dict} and iv_dict: {self.iv_dict}")
        cols=['DUE_DAYS']
        
        data['UNFAVORABLE_PAYMENT_TERMS'] = np.where(data[cols].isnull().any(axis=1),0,np.where(data['DUE_DAYS']<self.shorter_credit,1,0)) #data['PAYMENT_TERMS']=="_CASH") |
        for idx, row in data.iterrows():
            sap_credit_period = str(row['DUE_DAYS']).strip().lower() if row['DUE_DAYS'] else None
            iv_credit_period = self.iv_dict.get(str(row['VENDORID']), [])
            vend_credit_period = self.vendor_dict.get(str(row['VENDORID']), [])
            if sap_credit_period and iv_credit_period and vend_credit_period:
                data.at[idx, 'UNFAVORABLE_PAYMENT_TERMS'] = 0 if (sap_credit_period in iv_credit_period and sap_credit_period in vend_credit_period) else 1
            elif sap_credit_period and iv_credit_period:
                data.at[idx, 'UNFAVORABLE_PAYMENT_TERMS'] = 0 if (sap_credit_period in iv_credit_period) else 1
            elif sap_credit_period and vend_credit_period:
                data.at[idx, 'UNFAVORABLE_PAYMENT_TERMS'] = 0 if (sap_credit_period in vend_credit_period) else 1
            


        capture_log_message(log_message='Unfavorable Payment Terms Rule Checked')


    def payment_r_block(self,data):
        """
        Identify the invoices from AP with payment blosk status as R
        """
        cols = ['ENTRY_TYPE','PAYMENT_BLOCK_STATUS']
        data["PAYMENT_BLOCK_R"] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['ENTRY_TYPE']=='INV') & (data["PAYMENT_BLOCK_STATUS"]=='R'),1,0))
        capture_log_message(log_message='Payment Block R Rule Checked')
        
    def Approval_Matrix_Rule(self,data):
        """
        Flag the account documents which satisfies approval matrix
        """
        capture_log_message(log_message='Approval Matrix Rule Started')
        audit_id = data['audit_id'][0]
        result_data = process(audit_id)
        data['APPROVAL_MATRIX'] = data.join(result_data.set_index('ACCOUNT_DOC_ID'),on='ACCOUNT_DOC_ID')['APPROVAL_MATRIX']
        capture_log_message(log_message='Approval Matrix Rule Checked')

    def vendor_master_changes(self,data):
        cols = ['VENDORCODE']
        vendor_changes = self.approved_vendors.loc[self.approved_vendors['IS_SENSITIVE_CHANGE']==1]['VENDORCODE'].to_list()
        data['VENDOR_MASTER_CHANGES'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['VENDORCODE'].isin(vendor_changes)),1,0))
        #data['VENDOR_MASTER_CHANGES']=np.where(data['VENDORCODE'].isin(vendor_changes),1,0)
        capture_log_message(log_message='Vendor Master Changes Rule Checked')

    def Immediate_Payments(self, data):
        """
        Flag transactions which was paid immediately when there was no discount
        The percentage configurable to be taken from configuraiton
        """
        cols = ['INVOICE_DATE', 'PAYMENT_DATE', 'PAYMENT_INVOICE_DIFFERENCE', 'DUE_DAYS']
        discount_cols = ['MAX_DISCOUNT_PERIOD','MAX_DISCOUNT_PERCENT']
        
        # Step 1: 
        has_required_data = data[cols].notna().all(axis=1)
        invoice_on_or_before_payment = pd.to_datetime(data['INVOICE_DATE']) <= pd.to_datetime(data['PAYMENT_DATE'])
        within_immediate_range = data['PAYMENT_INVOICE_DIFFERENCE'] < data['DUE_DAYS'] * self.immediate_payment_percent
        is_payment_made_immediately = has_required_data & invoice_on_or_before_payment & within_immediate_range

        # Step 2:
        discount_defined = data[discount_cols].notna().all(axis=1)
        within_discount = data['PAYMENT_INVOICE_DIFFERENCE'] < data['MAX_DISCOUNT_PERIOD']
        is_discount_availed = np.where(discount_defined,within_discount,False).astype(bool)

        data['IMMEDIATE_PAYMENTS'] = (is_payment_made_immediately & ~is_discount_availed).astype(int)
        
        capture_log_message(log_message='Immediate Payments Rule Checked')
        
    # def Work_Flow_Deviation(self, data):
    #     """
    #     Flag transactions with accounts which were only used less than 5 times in a month
    #     Kind of working
    #     """
    
    #     data['WORK_FLOW_DEVIATION'] = np.where((data['ACCOUNT_CODE'].isin(self.work_flow_deviation_account_num)) & (((~data['INVOICE_DATE'].isna()) & ((data['PURCHASE_ORDER_DATE'].isna()) | (data['REQUISITION_DATE'].isna()) | (data['GRN_DATE'].isna()))) | (data['PAYMENT_DATE'].isna())),1,0) # | (data['PAYMENT_AMOUNT'].isna())
    
    def invoices_without_GRN(self,data):
        """
        Identify the AP invoices for which goods are not received. Users can configure the purchase accounts from AP.
        """

        # data["INVOICES_WITHOUT_GRN"] = np.where(data["INVOICE_DATE"].isna(),0,np.where((data['ACCOUNT_CODE'].isin(self.invoices_without_grn_account_num)) &  (data['GRN_NUMBER'].isna()),1,0))
        data["INVOICES_WITHOUT_GRN"] = np.where(data['GRN_NUMBER'].isna(),1,0)
        capture_log_message(log_message='Invoices without GRN Rule Checked')
        

    def Date_Sequential_Mismatch(self,data):

        rev_columns = self.Date_Sequence[::-1]
        data[self.Date_Sequence]=data[self.Date_Sequence].astype(np.datetime64).fillna(pd.NaT)
        k=1
        column_check_list =[]
        for i in range(0,len(rev_columns)):
            for j in range(k,len(rev_columns)):
                colname = rev_columns[i]+'_'+rev_columns[j]
                data[colname] = ((data[rev_columns[i]]-data[rev_columns[j]])/np.timedelta64(1, 'D')).fillna(10000).astype('int')
                column_check_list.append(colname)
            k=k+1
        
        data['DATE_SEQUENTIAL_MISMATCH'] = np.where((data[column_check_list]<0).any(1),1,0)
        capture_log_message(log_message='Date Sequential Mismatch Rule Checked')
        
    def Suspicious_Keywords(self,data):
        """
        Flag is any of suspicious keywords occur in the transaction description
        Is there in Config settingss
        """
        cols=['TRANSACTION_DESCRIPTION']
        data['SUSPICIOUS_KEYWORD'] = 0
        data['SUS_FLAG'] = 0
        for word in self.suspicious_words:
            data['SUS_FLAG'] = np.where(data[cols].isnull().any(axis=1),0,np.where(data['TRANSACTION_DESCRIPTION'].astype(str).str.lower().str.contains(word),1,0))
            data['SUSPICIOUS_KEYWORD'] = data['SUSPICIOUS_KEYWORD'] + data['SUS_FLAG']
        data.loc[data['SUSPICIOUS_KEYWORD'] > 1, ['SUSPICIOUS_KEYWORD']] = 1
        data.drop(['SUS_FLAG'], axis=1, inplace=True)
        capture_log_message(log_message='Suspicious Keywords Rule Checked')
        
    def Posting_Period(self,data):
        """
        Check whether the Posting is done in the same quarter when it generated the invoice 
        Passing
        """
        cols=['INVOICE_QUARTER','SYSTEM_POSTING_QUARTER','DISCOUNT_CHECK']
        
        data['POSTING_PERIOD'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['INVOICE_QUARTER']!=data['SYSTEM_POSTING_QUARTER']),1,0))
        capture_log_message(log_message='Posting Period Rule Checked')
        
    def Late_Payment(self,data):
        """
        Flag payments done after Due Date
        """
        cols=['DUE_PAYMENT_DIFFERENCE']
        data['LATE_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DUE_PAYMENT_DIFFERENCE']<0),1,0))
        # data['LATE_PAYMENT'] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['DUE_PAYMENT_DIFFERENCE']<0) & (data['DISCOUNT_CHECK']==1),1,0))
        capture_log_message(log_message='Late Payment Rule Checked')
        
        
    def Early_Payment(self,data):
        """
        Validate the below conditions
               
        If Due date - Payment Date is less than 0, i.e Payment is done after due date, then its not applicable for early payment check
        
        Else ( i.e, Payment is done before due date )
            1. If the Payment Date - Invoice Date is less than or equal to Discount Period, then Payment is made early to avail discount, hence not anomaly
            2. If the Payment Date - Invoice Date is greater than Discount Period, then Payment is made early but not to avail discount, hence anomaly
            
        """
        #  Build masks
        is_payment_made_before_due_date    = data['DUE_PAYMENT_DIFFERENCE'] > 0
        is_payment_made_within_discount_period   = data['PAYMENT_INVOICE_DIFFERENCE'].le(data['MAX_DISCOUNT_PERIOD'])

        #  Final flag
        data['EARLY_PAYMENT'] = (
            is_payment_made_before_due_date & ~(is_payment_made_within_discount_period)
        ).astype(int)

        capture_log_message(log_message='Early Payment Rule Checked')
        

    def three_way_matching(self,data):
        """Flag po transaction which has null values for inv number, po number and grn number"""
        
        numbers_check = (data['INVOICE_NUMBER'].isna() | data['PURCHASE_ORDER_NUMBER'].isna() | data['GRN_NUMBER'].isna())
        
        dates_check = (data['PURCHASE_ORDER_DATE'].isna() | data['GRN_DATE'].isna() | data['INVOICE_DATE'].isna() |
                        (data['PURCHASE_ORDER_DATE'] >= data['GRN_DATE']) | (data['GRN_DATE'] >= data['INVOICE_DATE']) )

        # Apply the condition only to PO transactions and set the flag
        data["3_WAY_MATCHING"] = np.where( data['DOC_TYPE'].isin(self.po_invoices_doctype),
                                    np.where(numbers_check | dates_check, 1, 0),  0)
        
        capture_log_message(log_message='3 Way Matching Rule Checked')
        
    
    def Lost_Discount(self,data):
        """
        Flag transactions where discount was available but not availed.

        A transaction is flagged as 'LOST_DISCOUNT' if:
        - Required fields are present
        - Payment was made within the discount period(s)
        - Discount was NOT taken
        """

        # 1. Build masks
        has_required_data = data[['PAYMENT_INVOICE_DIFFERENCE', 'DISCOUNT_TAKEN', 'MAX_DISCOUNT_PERIOD']].notna().all(axis=1)
        within_discount_period = data['PAYMENT_INVOICE_DIFFERENCE'] < data['MAX_DISCOUNT_PERIOD']
        discount_not_taken = data['DISCOUNT_TAKEN'].astype(float).eq(0)

        # 2. Combine all conditions
        mask = has_required_data & within_discount_period & discount_not_taken

        # 3. Assign result
        data['LOST_DISCOUNT'] = mask.astype(int)
        capture_log_message(log_message='Lost Discount Rule Checked')

    def Unusual_Vendor(self,data):
        """
        Flag transactions where an unknown vendor is there 
        Vendor list required from UI
        """
        
        data['UNUSUAL_VENDOR'] = np.where(data['SUPPLIER_ID'].isnull(),0,np.where(data['SUPPLIER_ID'].isin(self.vendor_list),1,0))
        capture_log_message(log_message='Unusual Vendor Rule Checked')
    
    def blank_je(self,data):
        """
        Flag transactions where transaction text is empty
        """
        cols=['TRANSACTION_DESCRIPTION']
        data['TRANSACTION_TEXT_EMPTY'] = np.where(data[cols].isnull(),1,0)
        capture_log_message(log_message='Blank JE Rule Checked')
    

    def rounding_off(self,data):
        """
        Last 3 digits: This entry’s monetary value does not end in “0.00” or “9.99”

        """
        data["ROUNDING_OFF"] = np.where(data['AMOUNT'].isnull(),0,np.where(data["ROUND_OFF"].isin(self.round_off),1,0))
        capture_log_message(log_message='Rounding Off Rule Checked')
    
    def late_invoice_creation(self,data):
        """
        The Invoice date from the supplier is after the accounting date

        """
        data["INVOICE_CREATED_AFTER_ACCOUNTING_DATE"] = np.where(data['INVOICE_DATE'].isnull(),0,np.where((data['INVOICE_DATE']>data['POSTING_DATE']),1,0))
        capture_log_message(log_message='Late Invoice Creation Rule Checked')
    
    def invoice_exceeds_range(self,data):
        """
        Invoice amount of supplier exceeds normal range of the supplier

        """
        data["INVOICE_VALUE_OF_SUPPLIER"] = np.where(data['INVOICE_AMOUNT'].isnull(),0,np.where((data['INVOICE_AMOUNT']>(data["INVOICE_SUPPLIER_RANGE_MEAN"]+3*(data["INVOICE_SUPPLIER_RANGE_STD"]))),1,0))
        capture_log_message(log_message='Invoice Exceeds Range Rule Checked')
    # def manual_debit_credit_notes(self,data):
    #     """
    #     Manually posted entry for debit or credit notes then we can flag the transaction
    #     """
    #     data["MANUAL_DEBIT_OR_CREDIT_NOTES"] = np.where(data['ENTRY_TYPE'].isnull(),0,np.where((data['ENTRY_TYPE']=="DM") | (data['ENTRY_TYPE']=="CM") &  (data['POSTED_BY'].isin(self.manual_entry_users)),1,0))

    def high_value_debit_credit_notes(self,data):
        """
        Identify the high-value Debit or Credit Notes ( Amount can be configured by the user )

        """
        data["HIGH_VALUE_DEBIT_CREDIT_NOTES"] = np.where(data['DOC_TYPE'].isnull(),0,np.where(((data['ENTRY_TYPE']=="DM") | (data['ENTRY_TYPE']=="CM")) &  (data['AMOUNT']>int(self.high_value_dm_cm)),1,0))
        capture_log_message(log_message='High Value Debit Credit Notes Rule Checked')
        
    def non_po_invoices(self,data):
        """
        Identify the non PO invoices from AP ( Configurable document type/Account name)
        """ 
        # cols = ['DOC_TYPE','ACCOUNT_CODE','INVOICE_AMOUNT']
        # data["NON_PO_INVOICES"] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['DOC_TYPE'].isin(self.non_po_invoices_doctype)) | (data['ACCOUNT_CODE'].isin(self.non_po_invoices_acc_num)) | (data['INVOICE_AMOUNT']>self.non_po_invoices_amount))  &  (data['PURCHASE_ORDER_NUMBER'].isna()),1,0))
        # data["NON_PO_INVOICE"] = np.where(data[cols].isnull().any(axis=1),0,np.where(( (data['DOC_TYPE'].isin(self.non_po_invoices_doctype)) & (data['INVOICE_AMOUNT']>self.non_po_invoices_amount))  &  (data['PURCHASE_ORDER_NUMBER'].isna()),1,0))
        data["NON_PO_INVOICE"] = np.where(data['PURCHASE_ORDER_NUMBER'].isna(),1,0)
        capture_log_message(log_message='Non PO Invoices Rule Checked')

    # def advance_payment_invoices(self,data):
    #     """
    #     Identify the advance payments made against the AP booked invoices without a purchase order.   
    #     """ 
    #     cols = ['DOC_TYPE','ACCOUNT_CODE','INVOICE_AMOUNT']
    #     # data["ADVANCE_PAYMENT_AGAINST_INVOICES"] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['DOC_TYPE'].isin(self.advance_payment_invoices_doctype)) | (data['INVOICE_AMOUNT']>self.advance_payment_invoices_amount)) & ((data['PAYMENT_DATE']<data['INVOICE_DATE']) & (data['PURCHASE_ORDER_NUMBER'].isna())),1,0))
    #     data["ADVANCE_PAYMENT_AGAINST_INVOICES"] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['DOC_TYPE'].isin(self.advance_payment_invoices_doctype)) & (data['INVOICE_AMOUNT']>self.advance_payment_invoices_amount) & (data['PAYMENT_DATE']<data['INVOICE_DATE']) & (data['PURCHASE_ORDER_NUMBER'].isna())),1,0))

    def early_posted_invoices(self,data):
        """
        Identify the early posted invoice booking journal entries from the AP data.  
        """ 
        cols = ['DOC_TYPE','POSTING_DATE','INVOICE_DATE']
        data["EARLY_POSTED_INVOICES"] = np.where(data[cols].isnull().any(axis=1),0,np.where((data['POSTING_DATE'] < data["INVOICE_DATE"]),1,0))
        capture_log_message(log_message='Early Posted Invoices Rule Checked')
        
        
    # def cash_expenses(self,data):
    #     """
    #     Identify the business expenses( other than employee reimbursements) which are paid in cash
    #     """
    #     cols=['PAYMENT_METHOD']
    #     # data["CASH_EXPENSES"] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['DOC_TYPE'].isin(self.cash_expenses_doctype)) | (data['ACCOUNT_CODE'].astype('int').isin(self.cash_expenses_acc_num) ) | (data['INVOICE_AMOUNT']>self.cash_expenses_amount)) &  (data['PAYMENT_METHOD'] == 'Cash'),1,0))
    #     data["CASH_EXPENSES"] = np.where(data[cols].isnull().any(axis=1),0,np.where( (data['DOC_TYPE'].isin(self.cash_expenses_doctype) ) & (data['INVOICE_AMOUNT']>self.cash_expenses_amount) &  (data['PAYMENT_METHOD'] == 'Cash'),1,0))

    # def cash_reimbursements(self,data):
    #     """
    #     Identify the cash payments made against employee reimbursements.
    #     """

    #     cols=['PAYMENT_METHOD']
    #     # data["CASH_REIMBURSEMENTS"] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['DOC_TYPE'].isin(self.cash_reimbursement_doctype)) | (data['ACCOUNT_CODE'].astype('int').isin(self.cash_reimbursement_acc_num) ) | (data['INVOICE_AMOUNT']>self.cash_reimbursement_amount)) &  (data['PAYMENT_METHOD'] == 'Cash'),1,0))
    #     data["CASH_REIMBURSEMENTS"] = np.where(data[cols].isnull().any(axis=1),0,np.where(( (data['DOC_TYPE'].isin(self.cash_reimbursement_doctype) ) & (data['INVOICE_AMOUNT']>self.cash_reimbursement_amount)) &  (data['PAYMENT_METHOD'] == 'Cash'),1,0))

    def invoice_po_quantity_mismatch(self,data):
        """
        Identify the quantity mismatch between PO quantity and AP Invoice quantity. ( POQuantity == InvoiceQuantity)
        """
        cols=['INVOICE_QUANTITY','PO_QUANTITY']
        data["DIFFERENCE_IN_INVOICE_QUANTITY"] = np.where(data[cols].isnull().any(axis=1),0,np.where(data['INVOICE_QUANTITY']!=data['PO_QUANTITY'],1,0))
        capture_log_message(log_message='Invoice PO Quantity Mismatch Rule Checked')

    def invoice_po_price_mismatch(self,data):
        """
        Identify the quantity mismatch between PO price and AP Invoice price. 
        """
        cols=['INVOICE_PRICE','PO_PRICE']
        data["DIFFERENCE_IN_INVOICE_PRICE"] = np.where(data[cols].isnull().any(axis=1),0,np.where(data['INVOICE_PRICE']!=data['PO_PRICE'],1,0))
        capture_log_message(log_message='Invoice PO Price Mismatch Rule Checked')

    def Old_Unpaid_Invoice(self,data):
        """
        Flag Invoives which are not paid n days after DUE Date
        n-number of days configuraiton is required
        """
        cols = ['UNPAID_DAYS','INVOICE_NUMBER']
        
        # total_invoices = list(data['INVOICE_NUMBER'].unique())
        # paid_invoices = list(data[~data['PAYMENT_DATE'].isna()]['INVOICE_NUMBER'].unique())
        # unpaid_invoices = set(total_invoices).difference(paid_invoices)
    
        # data['OLD_UNPAID_INVOICE'] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['UNPAID_DAYS']>self.unpaid_threshold_days) & (data['INVOICE_NUMBER'].isin(unpaid_invoices))),1,0))
        data['OLD_UNPAID_INVOICE'] = np.where(data[cols].isnull().any(axis=1),0,np.where(((data['UNPAID_DAYS']>self.unpaid_threshold_days) & (data['PAYMENT_DATE'].isnull())),1,0))
        capture_log_message(log_message='Old Unpaid Invoice Rule Checked')

    def similarity_check(self,df,check_column,grouping_column,which_one):
        from rapidfuzz import fuzz
        col_name = 'DUPLICATES_'+str(which_one)
        
        df[col_name] = ""
        
        subset_df = df[df[check_column]==1]
        
        for distinct_val in set(subset_df[grouping_column]):
            subset = subset_df[subset_df[grouping_column]==distinct_val]
            for pair in itertools.combinations(set(list(subset['INVOICE_NUMBER'])),2):
                match = fuzz.ratio(pair[0],pair[1])
                if match>60:
                    df.loc[df.INVOICE_NUMBER == pair[0], col_name] =  ",".join(df.loc[df.INVOICE_NUMBER==pair[0]]['INVOICE_ID_COPY'].to_list())
                    df.loc[df.INVOICE_NUMBER == pair[1], col_name] =  ",".join(df.loc[df.INVOICE_NUMBER==pair[1]]['INVOICE_ID_COPY'].to_list())

                else:
                    continue

        df['DUPLICATE_INV_'+str(which_one)] = np.where(~(df[col_name]==""),1,0)
        capture_log_message(log_message='Similarity Check Rule Checked')

    def Duplicate_Invoices(self,df):
        """
        Flag Duplicate Invoices
        """
        cols =['DUPLICATE_INV_1','DUPLICATE_INV_2','DUPLICATE_INV_3','DUPLICATE_INV_4','DUPLICATE_INV_5'] #,'DUPLICATE_INV_7','DUPLICATE_INV_8'
        duplicate_cols = ["DUPLICATES_"+column.strip("_")[-1] for column in cols ]
        
        df['INVOICE_DATE'] = df['INVOICE_DATE'].astype(str)
        df['INVOICE_ID_COPY']=df['INVOICE_ID'].astype(str)
        
        df['DUPLICATE_INV_1'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE','INVOICE_AMOUNT'])['INVOICE_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_1'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_2'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE'])['INVOICE_AMOUNT'].transform('nunique')>1,1,0)
        df['DUPLICATES_2'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_DATE'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_3'] = np.where(df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_AMOUNT'])['INVOICE_DATE'].transform('nunique')>1,1,0)
        df['DUPLICATES_3'] =df.groupby(['INVOICE_NUMBER','SUPPLIER_ID','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_4'] = np.where(df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT','INVOICE_DATE'])['SUPPLIER_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_4'] =df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT','INVOICE_DATE'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        df['DUPLICATE_INV_5'] = np.where(df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT'])['SUPPLIER_ID'].transform('nunique')>1,1,0)
        df['DUPLICATES_5'] =df.groupby(['INVOICE_NUMBER','INVOICE_AMOUNT'])['INVOICE_ID_COPY'].transform(lambda x:','.join(x.unique()))
        
        # df['DUPLICATE_INV_6_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','INVOICE_DATE','SUPPLIER_ID'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DUPLICATE_INV_6_CHECK','SUPPLIER_ID',6)

        # df['DIFF_SUPPLIER_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','INVOICE_DATE'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DIFF_SUPPLIER_CHECK','INVOICE_DATE',7)
        
        # df['DIFF_DATE_CHECK'] = np.where(df.groupby(['INVOICE_AMOUNT','SUPPLIER_ID'])['INVOICE_NUMBER'].transform('nunique')>1,1,0)
        # self.similarity_check(df,'DIFF_DATE_CHECK','SUPPLIER_ID',8)
        
        df['DUPLICATE_INVOICES_MAPPING'] = df[duplicate_cols].apply(lambda x: ','.join(x),axis=1).apply(lambda x: ','.join(pd.unique(x.split(','))))
        df['DUPLICATE_INVOICES_MAPPING'] = df['DUPLICATE_INVOICES_MAPPING'].apply(lambda x : ",".join(x.split(",")[1:]))
        df['DUPLICATE_INVOICES_MAPPING'] = np.where(df.DUPLICATE_INVOICES_MAPPING==",","",df.DUPLICATE_INVOICES_MAPPING)
        df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
        df['DUPLICATE_INVOICE_POSTING'] = np.where((df[cols].sum(axis=1)>0) & ~(df['STRIP_INVOICE'].isin(self.strip_list)),1,0)
        capture_log_message(log_message='Duplicate Invoices Rule Checked')

    def Rule_Scores_Calculation(self,data):
        """
        Function to calculate the Rule Score
        """
        capture_log_message(log_message='Rules Score Calculation Started')
        data['RULES_RISK_SCORE_RAW'] = 0
        for rulename,weight in self.rule_weights.items():
            data['RULES_RISK_SCORE_RAW']+= data[rulename]*weight
        max_raw_score = data['RULES_RISK_SCORE_RAW'].max()
        if max_raw_score > 0:
            data['RULES_RISK_SCORE'] = (data['RULES_RISK_SCORE_RAW']/max_raw_score.max()).round(2)
        else:
            data['RULES_RISK_SCORE'] = 0.0
        rule_name_cols = [rulename+"_NAME" for rulename,weight in self.rule_weights.items()]
        rule_dict = {rule : rule+"_NAME" for rule,weight in self.rule_weights.items()}
        
        for col in rule_name_cols:
         data[col]=""
        for rule,rulename in rule_dict.items():
            data.loc[(data[rule] > 0), rulename] = rule
        
        data['CONTROL_DEVIATION'] = data[rule_name_cols].values.tolist()
        data['CONTROL_DEVIATION'] = data['CONTROL_DEVIATION'].apply(lambda x: [i for i in x if i != ''])
        data['CONTROL_DEVIATION'] = data['CONTROL_DEVIATION'].apply(lambda x: ', '.join(x))

        for col in rule_name_cols:
            del data[col]
        capture_log_message(log_message='Rules Score Calculation Completed')
        return data

    def Rule_Score_Calculation_AccountDOC(self,data):
        
        capture_log_message(log_message='Account Doc Level Rules Score Calculation Started')

        rules = list(self.rule_weights.keys())
        agg_dict = {rule: "max" for rule in rules}
        agg_dict.update({"RULES_RISK_SCORE_RAW":   "max",
                         "RULES_RISK_SCORE":       "max", 
                         "BLENDED_RISK_SCORE_RAW": "max", 
                         "BLENDED_RISK_SCORE":     "max", 
                         "DEVIATION":              "max" })
        rules_accountdoc = data.groupby('ACCOUNT_DOC_ID',as_index=False).agg(agg_dict)

        # control deviation calculation
        rule_names = np.array(rules)
        mask_df = rules_accountdoc[rules].values > 0
        rules_accountdoc['CONTROL_DEVIATION'] = [", ".join(rule_names[mask]) for mask in mask_df ]
        capture_log_message(log_message='Account Doc Level Rules Score Calculation Completed')
        return rules_accountdoc

    def Run_Rules(self,data):
        Prep = Preparation()
        data = Prep.Data_Prep_for_Rules(data)
        capture_log_message(log_message='Data Preparation for Rules Completed')
        self.build_rules(data.columns.to_list())
        # reversal_pay_types1 = data[(data['ENTRY_TYPE'].isin(self.rev_pay_types)) & (data['INVOICE_AMOUNT']>0)]['STRIP_INVOICE'].to_list()
        # reversal_pay_types2 = data[~(data['ENTRY_TYPE'].isin(self.rev_pay_types)) & (data['INVOICE_AMOUNT']>0)]['STRIP_INVOICE'].to_list()
        # self.strip_list = reversal_pay_types1+reversal_pay_types2

        # data = data[~(data['ENTRY_TYPE'].isin(self.rev_pay_types)) | (data['INVOICE_AMOUNT']<0)]
        capture_log_message(log_message='Rules Calculation Started')
    
        for rule in self.rule_weights.keys():
            capture_log_message(log_message='Rule to be executed:{}'.format(rule))
            self.rule_functions[rule](data)
                                            
        capture_log_message(log_message='Rules Calculation Completed')       
        
        data = self.Rule_Scores_Calculation(data) 
        df_rules_scored = data.copy()

        id_column = ['TRANSACTION_ID','ACCOUNT_DOC_ID']
        rule_cols = [rulename for rulename,weight in self.rule_weights.items()]
        scores_column = ['CONTROL_DEVIATION','RULES_RISK_SCORE_RAW','RULES_RISK_SCORE']

        data = data[id_column+rule_cols+scores_column]
        
        return data, df_rules_scored

def main():

    DB = MySQL_DB('DB.json')
    connection = DB.connect_to_database()
    # start_db_read = datetime.now(timezone.utc)
    # df= pd.read_sql("""select tran.TRANSACTION_ID,tran.ENTERED_BY,doc.ENTRY_ID,tran.ACCOUNT_DOC_ID,tran.INVOICE_ID,tran.TRANSACTION_DESCRIPTION,tran.GL_ACCOUNT_DESCRIPTION,
    # tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.PAYMENT_DATE,tran.DISCOUNT_TAKEN,tran.CREDIT_PERIOD,tran.DISCOUNT_PERIOD,tran.DOC_TYPE,tran.COMPANY_ID AS COMPANYID,tran.VENDORID,
    # tran.ACCOUNT_TYPE,tran.POSTED_BY,tran.POSTING_DATE,tran.ENTRY_DATE,tran.SYSTEM_UPDATED_DATE,tran.DUE_DATE,tran.PAYMENT_TERMS, vend.VENDORCODE,tran.INVOICE_NUMBER,tran.SUPPLIER_ID,tran.INVOICE_AMOUNT,tran.INVOICE_DATE,tran.CREDIT_PERIOD,tran.TRANSACTION_CODE,comp.COMPANY_CODE,vend.VENDORCODE,loc.LOCATION_CODE
    # from ap_transaction tran
    # left join mscompany comp  on tran.COMPANY_ID=comp.COMPANYID
    # left join ap_vendorlist vend  on tran.VENDORID=vend.VENDORID
    # left join mslocation loc on tran.LOCATION_ID=loc.LOCATIONID
    # left join ap_accountdocuments doc on tran.ACCOUNT_DOC_ID=doc.ACCOUNT_DOC_ID where tran.IS_SCORED=0;""",connection) #left join msentrytype entry on tran.ENTRY_TYPE_ID=entry.ENTRY_TYPE_ID
    # configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='apframework' and STATUS=1",con=connection)
    # finish_db_read = datetime.now(timezone.utc)
    # capture_log_message(log_message='TIme Taken for Reading {shape} Dimensioned Dataframe {time}'.format(shape=df.shape,time=finish_db_read-start_db_read))
    # vend= pd.read_sql("""select VENDORCODE,IS_SENSITIVE_CHANGE from msvendor;""",connection)
    # connection.close()

    # Rules = Rules_Framework(configurations,vend,list(df.columns))
    # print(Rules)

    # Rules_scored = Rules.Run_Rules(df)


if __name__ == "__main__":
    start_time = time.time()
    main()
    
