import json
import pandas as pd
import numpy as np
from code1 import src_load
import os
import hist_data.utilities as utils
from Vendor_master import vendor_utils
from code1.logger import capture_log_message
from flask import g



class VendorMaster:
    def __init__(self,configs) -> None:
        try:
            self.configs = dict(zip(configs.KEYNAME,configs.KEYVALUE))
            
            capture_log_message(f"config is {self.configs}")
            
            self.vendor_master_rules = {'unusual_vendor':self.vendor_not_in_vendor_master, 'key_field_change_for_vendor':self.key_field_changes_for_vendors, 
                                        'incorrect_vendor_updates':self.incorrect_vendor_updates_indexing_issues, 'vat_or_tax_errors':self.vat_or_tax_errors, 
                                        'modification_request_occurence':self.modification_request_occurrence, 'duplicate_vendors':self.duplicate_vendors_within_and_across_bus,
                                        'inactive_vendors':self.inactive_vendors,'unusual_vendor_activity':self.unusual_vendor_activity}

            self.vendor_master_names = {'unusual_vendor':'Unusual Vendor', 'key_field_change_for_vendor':'Key Field Change for Vendor', 
                                        'incorrect_vendor_updates':'Incorrect Vendor Updates', 'modification_request_occurence':'Modification Request Occurence', 
                                        'duplicate_vendors':'Duplicate Vendors', 'vat_or_tax_errors':'Vat or Tax Errors',
                                        'inactive_vendors':'Inactive Vendors','unusual_vendor_activity':'Unusual Vendor Activity'}

            self.list_of_vendor_master_rules_to_be_executed = {rulename.split("weight_")[1]:weight for rulename,weight in self.configs.items() if rulename.startswith("weight")}
            capture_log_message(f"list_of_vendor_master_rules_to_be_executed are {self.list_of_vendor_master_rules_to_be_executed}")
            
            self.key_fields_for_vendor_master_changes = json.loads(self.configs.get("key_fields_for_vendor_master_changes","[]"))
            self.key_fields_for_indexing_issues = json.loads(self.configs.get("key_fields_for_indexing_issues","[]"))
            self.modification_request_time_period = json.loads(self.configs.get("modification_request_time_period",0))
            self.modification_request_threshold = json.loads(self.configs.get("modification_request_threshold",0))
            self.inactive_vendor_timeperiod = json.loads(self.configs.get("inactive_vendor_timeperiod",0))
        except Exception as e:
            capture_log_message(log_message=f"Error in VendorMaster initialization: {e}")
            raise e
        capture_log_message('Vendor master class initialized')
    def vendor_not_in_vendor_master(self, data):
        
        """
        If the vendor is not present in the vendor master, but present in the AP data, flag it.
        """
        if 'comments' not in data.columns:
            data['comments'] = ''
        capture_log_message('Checking for Unusual Vendor')
        data.loc[:,'unusual_vendor'] = 0
        ap_data = vendor_utils.fetch_ap_data() # Fetch AP data
        capture_log_message(f"AP data fetched, Data shape is {ap_data.shape}") # Fetch AP data
        list_of_vendor_codes_in_ap_data = ap_data['SUPPLIER_ID'].unique()
        capture_log_message('List of vendor ids in AP data fetched, unique count is {}'.format(len(list_of_vendor_codes_in_ap_data)))
        list_of_vendor_codes_in_vendor_master = data['VENDORCODE'].unique()
        capture_log_message('List of vendor codes in vendor master fetched, unique count is {}'.format(len(list_of_vendor_codes_in_vendor_master)))
        vendor_not_in_vendor_master = [vendor_id for vendor_id in list_of_vendor_codes_in_ap_data if vendor_id not in list_of_vendor_codes_in_vendor_master]
        capture_log_message('No. of vendors not present in vendor master count is {},actual missing vendors are {}'.format(len(vendor_not_in_vendor_master),vendor_not_in_vendor_master))
        if len(vendor_not_in_vendor_master)==0:
            capture_log_message('No Unusual Vendor Found')
        else:
            # Store information about unusual vendors in the comments column, with vendor code value as NULL
            capture_log_message('Unusual Vendor Found that not present in vendor master')
            # check whether vendor master has entry to store unusual vendor details
            # If there is row already present with Null vendor code, then append the comments to that row with the unusual vendor details
            # Else create a new row with Null vendor code and store the unusual vendor details in the comments column
            if data['VENDORCODE'].isna().any():
                capture_log_message(log_message="There are rows with Null vendor code in score table")
                null_vendor_row = data[data['VENDORCODE'].isna()]
                if not null_vendor_row['comments'].isna().all():
                    data.loc[data['VENDORCODE'].isna(), 'comments'] = null_vendor_row['comments'].str.lower() + ', ' + ', '.join(vendor_not_in_vendor_master).lower()
                else:
                    data.loc[data['VENDORCODE'].isna(), 'comments'] = ', '.join(vendor_not_in_vendor_master).lower()
            else:
                capture_log_message(log_message="There are No rows with Null vendor code in score table")
                new_row = pd.Series({'unusual_vendor':1 ,'comments': ', '.join(vendor_not_in_vendor_master).lower()})
                data.loc[len(data)] = new_row

        capture_log_message('Unusual Vendor Rule completed')


    
        

    def key_field_changes_for_vendors(self, data):
       """
       Tracks modifications to critical vendor master data fields, such as bank account details,
       tax identification numbers, or payment terms. This metric helps detect unauthorized or
       suspicious changes requiring review.
      
       Also add key fields changes in vendor_master_key_field_changes Table
      
       """
       key_fields_to_monitor = self.key_fields_for_vendor_master_changes
       # find out what are the vendors subjected to key field changes
       capture_log_message('Checking for Key field changes for the vendors, key fields:{}'.format(key_fields_to_monitor))
       # Filter only the vendor codes where status column has more than 1 values -  some sort of key field changes
       # and check if any of the key fields have more than 1 unique values, which suggests that value for those fields have changed for the vendor
       vendor_codes_with_key_field_changes = data.groupby('VENDORCODE').filter(
           lambda x: x['STATUS'].nunique() > 1 and   # Status column has more than 1 value
           any(x[field].nunique() > 1 for field in key_fields_to_monitor))['VENDORCODE'].unique() # Key fields have more than 1 unique value
       capture_log_message("List of vendors with key field changes:{}".format(vendor_codes_with_key_field_changes))
       data['key_field_change_for_vendor'] = np.where(data['VENDORCODE'].isin(vendor_codes_with_key_field_changes), 1, 0)
       capture_log_message('Key field changes for vendors completed')





    
        


    def incorrect_vendor_updates_indexing_issues(self, data):
        """
        Highlights errors or misalignments in vendor records caused by indexing issues,
        such as duplicate indexing or misplaced entries, which can disrupt payment processes or reporting.
        """
        capture_log_message('Checking for Incorrect_vendor_updates_indexing_issues')
        capture_log_message('Fields for indexing issues:{}'.format(self.key_fields_for_indexing_issues))
        self.key_fields_for_indexing_issues = [str(field).lower()+'_mismatch' for field in self.key_fields_for_indexing_issues]
        capture_log_message('Fields for indexing issues after formatting:{}'.format(self.key_fields_for_indexing_issues))
        # vendor_ids = data['VENDORID'].astype(int).to_list()
        vendor_ids = vendor_utils.convert_vendor_id_to_list(data)
        vendor_anomaly_index_df = src_load.fetch_iv_module_data(fields=self.key_fields_for_indexing_issues, vendor_ids=vendor_ids)
        # data['incorrect_vendor_updates'] = np.where(data['id'].isin(vendor_anomaly_index_df.loc[vendor_anomaly_index_df['vat_mismatch'] == 1, 'vendor_id']), 1, 0)
        capture_log_message('IV data with anomaly column related to incorrect_vendor_updates fetched, shape is {}'.format(vendor_anomaly_index_df.shape))

        if vendor_anomaly_index_df.empty:
            capture_log_message('No IV data with anomaly column related to incorrect_vendor_updates found')
            data['incorrect_vendor_updates'] = 0
        else:
            vendor_anomaly_condition = vendor_anomaly_index_df[self.key_fields_for_indexing_issues].eq(1).any(axis=1)
            capture_log_message(f"vendor_anomaly_condition is {vendor_anomaly_condition}")
            vendor_anomaly_ids = vendor_anomaly_index_df.loc[vendor_anomaly_condition, 'VENDORID']
            data['incorrect_vendor_updates'] = np.where(data['VENDORID'].isin(vendor_anomaly_ids), 1, 0)
        # IF we have to use vendor_code for setting 1s and 0s
        # data['flag'] = data.groupby('VENDORCODE')['incorrect_vendor_updates'].transform('max')

    def vat_or_tax_errors(self,data):
        """
        Highlights errors or misalignments in vendor records caused by indexing issues,
        such as duplicate indexing or misplaced entries, which can disrupt payment processes or reporting.
        """


        capture_log_message(log_message='Vat_or_tax_errors Rule Started')
        fields = 'vat_mismatch'
        vendor_ids = vendor_utils.convert_vendor_id_to_list(data)
        # vendor_ids = data['VENDORID'].astype(int).to_list()
        vendor_anomaly_index_df = src_load.fetch_iv_module_data(fields=[fields], vendor_ids=vendor_ids)
        capture_log_message('IV data with anomaly column related to vat_or_tax_errors fetched, shape is {}'.format(vendor_anomaly_index_df.shape))
        if vendor_anomaly_index_df.empty:
            capture_log_message('No IV data with anomaly column related to vat_or_tax_errors found')
            data['vat_or_tax_errors'] = 0
        else:
            data['vat_or_tax_errors'] = np.where(data['VENDORID'].isin(vendor_anomaly_index_df.loc[vendor_anomaly_index_df[fields] == 1, 'VENDORID']), 1, 0)
        
        capture_log_message(log_message='Vat_or_tax_errors Rule Checked')
        
        
    def modification_request_occurrence(self, data):
        """
        For each vendor (grouped by 'code'), this function calculates if the count of 
        records with modified_date within the window (max(modified_date) - threshold_period_months)
        exceeds threshold_count. It operates on a copy of df and then maps the result back to df 
        based on the unique identifier column (e.g., 'id').

        Args:
        df (pd.DataFrame): DataFrame with columns 'id', 'code', and 'modified_date'.
        threshold_period_months (int): The period (in months) to look back from the max date.
        threshold_count (int): Allowed number of records in the period.
        
        Returns:
        pd.DataFrame: Original DataFrame with an added 'exceeds_threshold' column.
        """

        capture_log_message(log_message='Modification_request_occurrence Rule Started')
        # Work on a copy of the DataFrame
        df_copy = data.copy()

        # Ensure the modified_date column is datetime
        df_copy['MODIFIED_DATE'] = pd.to_datetime(df_copy['MODIFIED_DATE'])

        # Group by vendor (using 'code' as the grouping key) and compute the flag on the copy
        processed_copy = df_copy.groupby('VENDORCODE', group_keys=False).apply(
                        lambda grp: vendor_utils.process_group(grp=grp, 
                                                               threshold_period_in_months=self.modification_request_time_period,
                                                               threshold_count=self.modification_request_threshold))

        # Map the computed flag back to the original DataFrame using the unique identifier ('id')
        # Assuming 'id' is unique across all rows
        mapping = processed_copy.set_index('VENDORID')['modification_request_occurence']
        data['modification_request_occurence'] = data['VENDORID'].map(mapping)       

        capture_log_message(log_message='Modification_request_occurrence Rule Checked')

    def duplicate_vendors_within_and_across_bus(self, data):
       """
       Check for duplicate vendors within BUs and across BUs.
       """
       capture_log_message(log_message='Duplicate vendors Started')
       data[['duplicate_vendors', 'comments']] = vendor_utils.find_duplicate_vendors(data)
      
       capture_log_message(log_message='Duplicate vendors Rule Checked')
       
       
    def inactive_vendors(self, data):
        """
        Check for inactive vendors.
        If a vendor does not post an invoice in the said n time period, mark the vendor as inactive.
        Show inactive vendor count and show inactive vendors list.
        """

        capture_log_message(log_message='Inactive vendors Rule Started')
        data['inactive_vendors'] = vendor_utils.calculate_inactive_vendors(data,self.inactive_vendor_timeperiod)
        capture_log_message(log_message='Inactive vendors Rule Checked')
        # Show inactive vendors list
        inactive_vendors_list = data[data['inactive_vendors']==1]['VENDORID'].unique()
        capture_log_message(f"Inactive Vendors List: {inactive_vendors_list}")

        # Show inactive vendor count
        inactive_vendor_count = len(inactive_vendors_list)
        capture_log_message(f"Inactive Vendor Count: {inactive_vendor_count}")

        capture_log_message(log_message='Inactive vendors Rule Checked')
        
    def unusual_vendor_activity(self, data):
        """
        Check for unusual vendor activity based on invoice amount.
        If the invoice amount for a certain vendor is significantly different than the usual amount for that vendor,
        mark the vendor as unusual.

        Args:
            data (pd.DataFrame): DataFrame containing vendor data
            
        """
        import os
        list_of_vendor_codes_in_vendor_master = data['VENDORCODE'].unique()
        default_value = 0
        vendor_dict = {vendor_code: default_value for vendor_code in list_of_vendor_codes_in_vendor_master}
        ap_data = vendor_utils.fetch_ap_data() # Fetch AP data
        ap_data['is_current_data'] = False
        max_batch_id = ap_data['batch_id'].max()
        
        capture_log_message('AP data fetched, Data shape is {}'.format(ap_data.shape))
        capture_log_message('Max batch id is {}'.format(max_batch_id))
        # Where the batch id is max, assign is_current_data as True
        mask = ap_data['batch_id'] == max_batch_id
        ap_data.loc[mask, 'is_current_data'] = True
        doc_wise_apdf = ap_data.groupby('ACCOUNTING_DOC',as_index=False).agg({"COMPANY_NAME":'first','INVOICE_NUMBER':'first','INVOICE_DATE':'first',
                                      'SUPPLIER_ID':'first','SUPPLIER_NAME':'first','DEBIT_AMOUNT':'sum','POSTED_DATE':'first','is_current_data':'first','batch_id':'max',})
        no_of_days_for_hist_data = os.getenv('NO_OF_DAYS_FOR_UNUSUSAL_VENDOR_ACTIVITY',90)
        capture_log_message('Time Period for fetching hist data for unusual vendor activity is {}'.format(no_of_days_for_hist_data))
        max_date = doc_wise_apdf['POSTED_DATE'].max()
        cutoff_date = max_date - pd.DateOffset(days=int(no_of_days_for_hist_data))
        filtered_doc_wise_apdf = doc_wise_apdf[doc_wise_apdf['POSTED_DATE'] >= cutoff_date]
        for key,_ in vendor_dict.items():
            flag = vendor_utils.detect_unusual_vendor_activity(vendor_code=key,vendor_data=filtered_doc_wise_apdf)
            vendor_dict[key] = flag
        data['unusual_vendor_activity'] = data['VENDORCODE'].map(vendor_dict)
           
       


    
    def Vendor_Rule_Scores_Calculation(self,data):
       """
       Function to calculate the Rule Score
       """
       capture_log_message(log_message='Vendor rules Score Calculation Started')


       df = data.copy()
       df['vendor_master_deviation'] = ""
       df['vendor_master_risk_score_raw'] = 0


       for rulename,weight in self.list_of_vendor_master_rules_to_be_executed.items():
           df['vendor_master_deviation']+= np.where(df[rulename]>0,self.vendor_master_names[rulename]+",","")
           df['vendor_master_risk_score_raw']+= df[rulename]*float(weight)


       df['vendor_master_risk_score'] = df['vendor_master_risk_score_raw']/df['vendor_master_risk_score_raw'].max()
      
       rule_cols = list(self.list_of_vendor_master_rules_to_be_executed.keys())


       df.rename(columns={'VENDORID':'vendor_id'}, inplace=True)
       rule_cols = rule_cols + ['vendor_master_deviation','vendor_master_risk_score_raw','vendor_master_risk_score','vendor_id','comments']


       df = df[rule_cols]
       capture_log_message(log_message='Vendor rules Score Calculation Completed')
       return df


    def Run_Vendor_Rules(self, data):
        """
        Run all vendor-related rules on the provided data.
        """
        
        capture_log_message('Run all vendor_master rules that are configured to be executed')
        all_vendor_master_rules = [rule for rule,_ in self.list_of_vendor_master_rules_to_be_executed.items()]
        capture_log_message(f"list of all rules :{all_vendor_master_rules}")
        
        for rule,weight in self.list_of_vendor_master_rules_to_be_executed.items():
            capture_log_message(log_message='Vendor rule to be executed:{}'.format(rule))
            self.vendor_master_rules[rule](data)
        
        return data