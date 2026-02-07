import pandas as pd
import numpy as np
from flask import g
from collections import OrderedDict
from datetime import datetime, timezone
from AP_Module.db_connector import MySQL_DB
# from AP_Module.Iforest import Stat_Framework
from AP_Module.Rules import Rules_Framework
# from AP_Module.AI import AI_Framework
from AP_Module.Data_Preparation import Preparation
from duplicate_invoices import predict
import traceback
# # from AP_Module.logger import logger
# from code1.logger import logger as main_logger
from code1.logger import add_data_to_external_api_call, capture_log_message, process_data_for_sending_internal_mail,\
                             update_current_module, update_total_account_doc_for_audit_id
from AP_Module.exceptions import AIScoringException,StatScoringException,RulesScoringException,ScoringDataStorageException,DuplicateScoringException
import utils
import re

from Ingestor.fetch_data import read_ddf_from_path, find_month_label_based_on_date, get_quarters
from pipeline_data import PipelineData
# Lock for thread safety
# lock = threading.Lock()
import os
from src_load import get_max_duplicate_id , get_max_pair_id, get_max_series_id
from Optimisation_module.optimisation_utils import is_optimisation_model_available
from Optimisation_module.Optimised_Rules import optimise_rule_scores, optimised_rules_risk_score, optimised_deviation_calculation, optimised_acc_doc_lvl_rule_scores


from itertools import combinations
from AP_Module.helper import check_supplier_match, deduplicate_groups, filter_out_duplicate_credit_debit_pairs, get_matching_invoice_rows,\
                            is_sequential_series,preprocess_name, similar_supplier_names,get_matching_rows_with_same_invoice_date

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(script_path)
base_directory = os.path.dirname(parent_directory)
historical_data_directory = os.path.join(base_directory,'hist_data')


def Account_Doc_level(df,Rule_object,weights):
    """
    Aggregating Scores at Accounting Doc level
    -----------------------------------------
    Input : 
            df :scored transaction data frame to aggregate
            rule_object : to access the function used for remapping
            weights : framework weights
    """
    # Prep = Preparation()
    capture_log_message(log_message='Account Doc Level Function Started')
    ai_stat_accountdoc = df.groupby('ACCOUNT_DOC_ID',as_index=False).agg(OrderedDict([('STAT_SCORE',['max']),('AI_RISK_SCORE_RAW',['max'])]))
    ai_stat_accountdoc.columns=['ACCOUNT_DOC_ID','STAT_SCORE','AI_RISK_SCORE_RAW']

    # min_val = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].min()
    # max_val = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].max()
    # ai_stat_accountdoc['AI_RISK_SCORE'] = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1))
    ai_stat_accountdoc['AI_RISK_SCORE'] = 0

    # min_val = ai_stat_accountdoc['STAT_SCORE'].min()
    # max_val = ai_stat_accountdoc['STAT_SCORE'].max()

    # ai_stat_accountdoc['STAT_SCORE_INDEXED'] = ai_stat_accountdoc['STAT_SCORE'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1))
    ai_stat_accountdoc['STAT_SCORE_INDEXED'] = 0
    
    rules_accountdoc = Rule_object.Rule_Score_Calculation_AccountDOC(df)
    
    final_df = rules_accountdoc.merge(ai_stat_accountdoc,how='left',on='ACCOUNT_DOC_ID',validate='one_to_one')
    capture_log_message(log_message='Account Doc Level Function Completed')
    return final_df

def Blended_Score_Calculation(Scored_DF,weights):
    """
    Calculating Blended Risk score for the transactions/accountingdocs
    ------------------------------------------------------------------
    Input : Scored DF : Scored dataframe with all 3 framework
            weights : weights for the frameworks
    """
    # AI_WEIGHT = weights['AI']
    # STAT_WEIGHT = weights['STAT']
    RULE_WEIGHT = weights['RULE']
    capture_log_message(log_message='Blended Score Calculation Started')
    # Scored_DF['BLENDED_RISK_SCORE_RAW'] = (Scored_DF['AI_RISK_SCORE']* AI_WEIGHT) + (Scored_DF['RULES_RISK_SCORE'] * RULE_WEIGHT) + (Scored_DF['STAT_SCORE_INDEXED'] * STAT_WEIGHT)
    Scored_DF['BLENDED_RISK_SCORE_RAW'] = Scored_DF['RULES_RISK_SCORE'] * RULE_WEIGHT
    # min_val,max_val = Scored_DF['BLENDED_RISK_SCORE_RAW'].min(),Scored_DF['BLENDED_RISK_SCORE_RAW'].max()
    # Scored_DF['BLENDED_RISK_SCORE'] = Scored_DF['BLENDED_RISK_SCORE_RAW'].apply(lambda x: (x - min_val)/(max_val-min_val))
    # Scored_DF['DEVIATION'] = np.where((Scored_DF['BLENDED_RISK_SCORE']>.7) | (Scored_DF['RULES_RISK_SCORE']>0),1,0)
    Scored_DF['BLENDED_RISK_SCORE'] = Scored_DF['RULES_RISK_SCORE']
    Scored_DF['DEVIATION'] = np.where((Scored_DF['RULES_RISK_SCORE']>0),1,0)
    capture_log_message(log_message='Blended Score Calculation Completed')
    return Scored_DF

def ap_main(audit_id):
    
    EXCEPTION_DETAILS = {
        AIScoringException: {
            'log_message': 'Error occurred during AP Scoring - AI Module Failed: {}',
            'error_name': utils.AI_SCORING_FAILED,
            'description': 'AP Scoring - AI Module Failed',
        },
        StatScoringException: {
            'log_message': 'Error occurred during AP Scoring - STAT Module Failed: {}',
            'error_name': utils.STAT_SCORING_FAILED,
            'description': 'AP Scoring - STAT Module Failed',
        },
        RulesScoringException: {
            'log_message': 'Error occurred during AP Scoring - RULES Module Failed: {}',
            'error_name': utils.RULES_SCORING_FAILED,
            'description': 'AP Scoring - RULES Module Failed',
        },
        DuplicateScoringException: {
            'log_message': 'Error occurred during AP Scoring - DUPLICATE INVOICE Module Failed: {}',
            'error_name': utils.DUPLICATE_SCORING_FAILED,
            'description': 'AP Scoring - DUPLICATE INVOICE Module Failed',
        },
        ScoringDataStorageException: {
            'log_message': 'Error occurred during AP Scoring - ISSUES WITH STORING SCORE RESULTS: {}',
            'error_name': utils.ISSUES_WITH_STORING_RESULTS,
            'description': 'AP Scoring - ISSUES WITH STORING SCORE RESULTS',
        },
}

    
    
    update_current_module('scoring')
    scoring_start_time = datetime.now(timezone.utc)
    try:
        DB = MySQL_DB('AP_Module/DB.json') 
        # process_details = {"success":0,"failure":0,"message":"Scoring In Progress"}

        #Updating ML Execution Job Status to In Progress
        # Update_Job = MySQL_DB('Exec_DB.json')
        # Update_Job.update_job_status(1,process_details)
        with DB.connect_to_database() as connection:
            start_db_read = datetime.now(timezone.utc)
            capture_log_message(log_message='Reading data from database')
            # df= pd.read_sql(f"""select tran.TRANSACTION_ID,tran.audit_id,tran.ENTERED_BY,doc.ENTRY_ID,tran.ACCOUNT_DOC_ID,tran.INVOICE_ID,tran.TRANSACTION_DESCRIPTION,tran.GL_ACCOUNT_DESCRIPTION,
            # tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.PAYMENT_DATE,tran.DISCOUNT_TAKEN,tran.DISCOUNT_PERIOD,tran.DOC_TYPE,tran.PURCHASE_ORDER_NUMBER,tran.PURCHASE_ORDER_DATE,tran.REQUISITION_DATE,tran.GRN_DATE,tran.GRN_NUMBER,
            # tran.POSTED_BY,tran.POSTING_DATE,tran.ENTRY_DATE,tran.SYSTEM_UPDATED_DATE,tran.DUE_DATE,tran.PAYMENT_TERMS,  tran.INVOICE_NUMBER,tran.SUPPLIER_ID,tran.INVOICE_AMOUNT,tran.TRANSPORTATION_DATE,
            # tran.INVOICE_DATE,tran.COMPANY_ID AS COMPANYID,tran.VENDORID,tran.CREDIT_PERIOD,tran.PAYMENT_METHOD,tran.TRANSACTION_CODE,coa.ACCOUNT_CODE,comp.COMPANY_CODE,vend.VENDORCODE,
            # vend.VENDOR_NAME as SUPPLIER_NAME,loc.LOCATION_CODE,tran.INVOICE_QUANTITY,tran.PO_QUANTITY,tran.INVOICE_PRICE,tran.PO_PRICE,tran.DISCOUNT_PERCENTAGE
            # from ap_transaction_{audit_id} tran

            # left join mschartofaccounts coa on tran.ACCOUNT_ID=coa.ACCOUNTID
            # left join mscompany comp  on tran.COMPANY_ID=comp.COMPANYID
            # left join ap_vendorlist vend  on tran.VENDORID=vend.VENDORID
            # left join mslocation loc on tran.LOCATION_ID=loc.LOCATIONID
            # left join ap_accountdocuments_{audit_id} doc on tran.ACCOUNT_DOC_ID=doc.ACCOUNT_DOC_ID  where tran.IS_SCORED=0 and tran.audit_id = {audit_id};""",con=connection)
            
            configurations = pd.read_sql(f"SELECT KEYNAME,KEYVALUE from trconfiguration where module='apframework' and STATUS=1",con=connection)
            # finish_db_read = datetime.now(timezone.utc)
            # time_taken_to_read = finish_db_read-start_db_read
            # capture_log_message(log_message='Time taken for reading data from database:{}'.format(time_taken_to_read))
            vend= pd.read_sql("""select VENDORCODE,IS_SENSITIVE_CHANGE from msvendor;""",con=connection)
            ap_vendorlist = pd.read_sql(f"""select * from ap_vendorlist where STATUS=1""",con=connection)
            pipe_score_obj = PipelineData()
            df = pipe_score_obj.get_data(f'audit_{audit_id}')
            capture_log_message(f"DF of ap_main shape :{df.shape} ")

        # if df.shape[0]==0:
        #     process_details = {"success":0,"failure":df.shape[0],"message":"Records are already processed"}
        #     Update_Job.update_job_status(3,process_details)
        #     return 0

        df_rules = df.copy()
        df_ai = df.copy()
        df_stat = df.copy()

        model_name_ai = "20220706_AI_Model"
        model_name_stat = "20220706_Stat_Model"
        capture_log_message(log_message='AI Model Name = {ai} \n Stat Model Name = {stat}'.format(ai=model_name_ai,stat=model_name_stat))
        
        try:
            #Ai Scoring
            ai_scoring_start = datetime.now(timezone.utc)
            AI_Scored = pd.DataFrame(index=df_ai.index)
            AI_Scored['AI_RISK_SCORE_RAW'] = 0  # All raw scores set to 0
            AI_Scored['AI_RISK_SCORE'] = 0

            # AI = AI_Framework(model_name_ai)
            # AI_Scored = AI.AI_Scoring(df_ai)
            ai_scoring_end = datetime.now(timezone.utc)
            time_taken_for_ai_scoring = ai_scoring_end-ai_scoring_start
            capture_log_message(log_message='Time taken for AI Scoring:{}'.format(time_taken_for_ai_scoring))
            capture_log_message(log_message='Shape of data after AI Scoring:{}'.format(AI_Scored.shape))
        except Exception as e:
            raise AIScoringException(e)
        
        try:
            #Stat Scoring
            stat_scoring_start   = datetime.now(timezone.utc)
            Stat_Scored = pd.DataFrame(index=df_stat.index)
            Stat_Scored['STAT_SCORE'] = 0  # All raw scores set to 0
            Stat_Scored['STAT_SCORE_INDEXED'] = 0

            # Stat_Model = Stat_Framework(model_name_stat)
            # Stat_Scored = Stat_Model.Stat_Scoring(df_stat)
            stat_scoring_end = datetime.now(timezone.utc)
            time_taken_for_stat_scoring = stat_scoring_end-stat_scoring_start
            capture_log_message(log_message='Time taken for Stat Scoring:{}'.format(time_taken_for_stat_scoring))
            capture_log_message(log_message='Shape of data after Stat Scoring:{}'.format(Stat_Scored.shape))
        except Exception as e:
            raise StatScoringException(e)
        
        try:
            #Rule Scoring
            rule_scoring_start = datetime.now(timezone.utc)
            Rule = Rules_Framework(configurations,vend, list(df_rules.columns),ap_vendorlist=ap_vendorlist)
            Rule_Scored, df_rules_scored = Rule.Run_Rules(df_rules)
            rule_scoring_end  = datetime.now(timezone.utc)
            time_taken_for_rule_scoring = rule_scoring_end-rule_scoring_start
            capture_log_message(log_message='Time taken for Rule Scoring:{}'.format(time_taken_for_rule_scoring))
            capture_log_message(log_message='Shape of data after Rule Scoring:{}'.format(Rule_Scored.shape))
        except Exception as e:
            raise RulesScoringException(e)


        #combining all the framework scores and suppressing the scores for selected transactions
        Scored_DF = pd.concat([Rule_Scored,Stat_Scored,AI_Scored],axis=1)
        # Scored_DF.to_csv("scoring.csv")
        capture_log_message(log_message='Shape of Scored data(concatenated data):{}'.format(Scored_DF.shape))
        capture_log_message(log_message=' Shape of Scored data(concatenated data):{}'.format(Scored_DF.shape))
        
        # configurations = pd.DataFrame([['RISK_WEIGHT_AI',4],['RISK_WEIGHT_STAT', 3],['RISK_WEIGHT_RULE',4]],columns=['KEYNAME','KEYVALUE'])
        config = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
        framework_weights = {rulename.split("WEIGHT_")[1]:float(weight) for rulename,weight in config.items() if rulename.startswith("RISK")}
        capture_log_message(log_message='The Config are {}'.format(config),store_in_db=False)
        
        # import ast
        # scenarios = ast.literal_eval(config.get('duplicate_invoice_scenarios'))
        # thresholds = ast.literal_eval(config.get('duplicate_invoice_scenario_threshold'))
        # # Strip, convert keys to int, values to float
        # scenario_threshold_map = {
        #     int(str(k).strip()): float(str(v).strip())
        #     for k, v in zip(scenarios, thresholds)
        # }
        # g.scenario_threshold_map = scenario_threshold_map
        # capture_log_message(f"Scenario Threshold Map: {g.scenario_threshold_map}")

        Scored_DF = Blended_Score_Calculation(Scored_DF,framework_weights)
        Scored_DF['audit_id'] = audit_id
        capture_log_message(log_message=f'AUD_{audit_id}Shape of data after Blended score calculation:{Scored_DF.shape}')
        
        Scored_DF_AccountDOC = Account_Doc_level(Scored_DF,Rule,framework_weights)
        Scored_DF_AccountDOC['audit_id'] = audit_id
        capture_log_message(log_message=f'AUD_{audit_id} Shape of data after Account doc level score calculation:{Scored_DF_AccountDOC.shape}')

        dup_inv_data = df.groupby('ACCOUNT_DOC_ID').first().reset_index()
        
        capture_log_message(f"AUD_{audit_id} Shape of data at accout doc level for duplicate invoice detection:{dup_inv_data.shape}")

        configs_unique = configurations.drop_duplicates(subset=['KEYNAME','KEYVALUE'])
        config = dict(zip(configs_unique.KEYNAME, configs_unique.KEYVALUE))
        rule_weights = {
            key.split("WEIGHT_", 1)[1]: float(val)
            for key, val in config.items()
            if key.startswith("WEIGHT_")
        }
        opt_rule_weights ={key : float(val)  for key, val in config.items() if key.startswith("OPTIMISED_")} 
        
        # Scored_DF = optimise_rule_scores(Scored_DF, df_rules_scored, rule_weights, opt_rule_weights)

        # Scored_DF = optimised_rules_risk_score(Scored_DF, rule_weights)

        # Opt_Scored_DF = pd.concat([Scored_DF,Stat_Scored,AI_Scored],axis=1)
            
        # Opt_Scored_DF = optimised_deviation_calculation(Scored_DF)

        # Opt_Scored_DF_AccountDOC =optimised_acc_doc_lvl_rule_scores(Opt_Scored_DF, rule_weights)

        # Final_AccountDOC = (Scored_DF_AccountDOC.merge(Opt_Scored_DF_AccountDOC,
        #                                             on="ACCOUNT_DOC_ID",
        #                                             how="left",
        #                                             suffixes=("", "_opt") )
        #                                         )
        # Update this code to have correct mounted directory while deploying in production
        import os
        base_path = os.getenv('UPLOADS')
        master_folder = os.getenv('MASTER_FOLDER', 'dow_transformation')
        mounted_directory = os.path.join(base_path, master_folder) if base_path else None
        if mounted_directory is  None or mounted_directory =="":
            processed_output_folder = os.path.join(g.client_folder_path,'filtered_rows_output')
        else:
            processed_output_folder = os.path.join(mounted_directory, 'filtered_rows_output')
        if not os.path.exists(processed_output_folder):
            os.makedirs(processed_output_folder)
            capture_log_message(f"Created processed output folder: {processed_output_folder}")
        else:
            capture_log_message(f"Processed output folder already exists: {processed_output_folder}")
        # Store the folder path in global variable for use throughout the function
        g.processed_output_folder = processed_output_folder

        def cleanup_duplicate_columns(df, log_prefix=""):
            """
            Helper function to remove duplicate column names from a DataFrame.
            When multiple merges/renames create duplicate columns with different dtypes,
            this can break pandas operations like .query(). This function keeps the first
            occurrence of each duplicated column name.
            
            Args:
                df (pd.DataFrame): DataFrame potentially containing duplicate column names
                log_prefix (str): Descriptive prefix for logging which operation triggered cleanup
                
            Returns:
                pd.DataFrame: DataFrame with duplicate column names removed
            """
            if df.columns.duplicated().any():
                duplicated_cols = df.columns[df.columns.duplicated()].unique().tolist()
                capture_log_message(f"{log_prefix} Found duplicate columns: {duplicated_cols}")
                # Keep only the first occurrence of each duplicated column
                df = df.loc[:, ~df.columns.duplicated(keep='first')]
                capture_log_message(f"{log_prefix} Cleaned up duplicate columns. New shape: {df.shape}, Remaining columns: {list(df.columns)[:10]}...")  # Show first 10 cols
            return df

        def handle_duplicate_invoice_results(df):
            """
            manually remove duplicate invoice results based on DEBIT_CREDIT_INDICATOR 
            """
            capture_log_message(f"Calling handle_duplicate_invoice_results function, shape of data is {df.shape}")
            
            # Clean up duplicate column names that may have been created during merges
            if df.columns.duplicated().any():
                capture_log_message(f"Found duplicate column names: {df.columns[df.columns.duplicated()].tolist()}")
                # Keep only the first occurrence of each duplicated column
                df = df.loc[:, ~df.columns.duplicated(keep='first')]
                capture_log_message(f"Cleaned up duplicate columns. New shape: {df.shape}, New columns: {list(df.columns)}")
            
            # Deduplicate results across many scenarios
            main_df=deduplicate_groups(df.copy())
            capture_log_message(f"After Deduplication, shape of data is {main_df.shape}")
            threshold_map = {1:60,2:60,3:60,4:60,5:60,6:60}
            threshold_series = main_df['SCENARIO_ID'].map(threshold_map)
            threshold_filtered_rows = main_df[main_df['DUPLICATE_RISK_SCORE'] < threshold_series]
            if not threshold_filtered_rows.empty:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"duplicate_threshold_filtered_audit_{audit_id}_{timestamp}.csv"
                    csv_filepath = os.path.join(g.processed_output_folder, csv_filename)
                    
                    threshold_filtered_rows.to_csv(csv_filepath, index=True)
                    capture_log_message(f"Saved {len(threshold_filtered_rows)} threshold-filtered rows to: {csv_filepath}")
                    
                except Exception as csv_error:
                    capture_log_message(current_logger=g.error_logger, log_message=f"Error saving threshold-filtered rows to CSV: {str(csv_error)}")
            else:
                capture_log_message("No rows were filtered out by the threshold criteria.")

            main_df = main_df[main_df['DUPLICATE_RISK_SCORE'] >= threshold_series]
            original_main_df = main_df.copy()
            capture_log_message(f"After threshold filter: {main_df.shape}")
            # main_df.to_csv('duplicate_res_debugging.csv',index=False)
            ids_to_be_dropped = []
            rows_to_drop = []
            all_pairs_dropped = set()  # <--- collect all debit/credit pairs here
            grouped_df = main_df.groupby('DUPLICATE_ID')

            for id, mydf in grouped_df:
                # Check if 'is_current_data' column exists and filter groups with all False values
                if 'is_current_data' in mydf.columns:
                    if not any(mydf['is_current_data'].unique()):
                        ids_to_be_dropped.append(id)
                        continue
                
                # Get debit/credit indicators for the group
                # For DOW, H is Debit and S is Credit
                credit_mask = mydf['DEBIT_CREDIT_INDICATOR'] == 'S'
                debit_mask  = mydf['DEBIT_CREDIT_INDICATOR'] == 'H'
                
                # Check if all indicators are 'H' or all are 'S'
                all_credit = credit_mask.all()
                all_debit = debit_mask.all()
                
                # If all credit or all debit, keep the group as is
                if all_credit or all_debit:
                    continue
                
                
                # Mixed 'H' and 'S' indicators - apply matching logic
                credit_rows = mydf[credit_mask].copy()
                debit_rows = mydf[debit_mask].copy()
                
                
                # Create matching pairs based on SUPPLIER_NAME and INVOICE_AMOUNT_ABS
                group_rows_to_drop = []
                matched_credit_indices = set()# Track matched debit rows more efficiently
                
                # Sort both by POSTED_DATE ascending for deterministic pairing
                credit_rows = credit_rows.sort_values('POSTED_DATE')
                debit_rows = debit_rows.sort_values('POSTED_DATE')
                
                for _, debit_row in debit_rows.iterrows():
                    debit_amount = debit_row['INVOICE_AMOUNT_ABS']
                    debit_supplier = debit_row['SUPPLIER_NAME']
                    debit_date = debit_row['POSTED_DATE']
                    
                    # Available credits not yet matched
                    available_credits = credit_rows[
                        ~credit_rows.index.isin(matched_credit_indices)
                    ]

                    # Filter: same supplier & amount, and posted_date <= debit_date
                    candidates = available_credits[
                        (available_credits['SUPPLIER_NAME'] == debit_supplier) &
                        (available_credits['INVOICE_AMOUNT_ABS'] == debit_amount) &
                        (available_credits['POSTED_DATE'] <= debit_date)
                    ]
                    if not candidates.empty:
                        matching_credit_idx = candidates['POSTED_DATE'].idxmax()

                        # Record both for dropping
                        group_rows_to_drop.append(debit_row.name)
                        group_rows_to_drop.append(matching_credit_idx)

                        # Track matched credit row
                        matched_credit_indices.add(matching_credit_idx)

                
                # Calculate remaining rows after dropping matched pairs
                remaining_rows = len(mydf) - len(group_rows_to_drop)
                
                # If only 1 row remains after matching, drop the entire group
                if remaining_rows < 2:
                    ids_to_be_dropped.append(id)
                    if group_rows_to_drop:
                        all_pairs_dropped.update(group_rows_to_drop)

                else:
                    # Add matched pairs to global drop list
                    rows_to_drop.extend(group_rows_to_drop)
                    if group_rows_to_drop:
                        all_pairs_dropped.update(group_rows_to_drop)

            # --- consolidate matched pairs ---
            matched_pairs_dropped = pd.DataFrame(columns=main_df.columns)
            ids_to_be_dropped = list(set(ids_to_be_dropped))

            dropped_df = main_df[main_df['DUPLICATE_ID'].isin(ids_to_be_dropped)].copy()

            if rows_to_drop:
                rows_to_drop_unique = list(set(rows_to_drop))
                matched_pairs_dropped = main_df.loc[rows_to_drop_unique].copy()
                main_df = main_df.drop(rows_to_drop)

            try:
                combined_csv_df = pd.concat([matched_pairs_dropped, dropped_df], ignore_index=True).drop_duplicates().reset_index(drop=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_filename = f"duplicate_total_credit_debit_pairs_audit_{audit_id}_{timestamp}.csv"
                csv_filepath = os.path.join(g.processed_output_folder, csv_filename)
                combined_csv_df.to_csv(csv_filepath, index=True)
                capture_log_message(f"Saved combined credit-debit pairs (CSV includes singletons) to: {csv_filepath} (count: {len(combined_csv_df)})")
            except Exception as csv_error:
                capture_log_message(current_logger=g.error_logger,
                                    log_message=f"Error saving matched-pair rows to CSV: {str(csv_error)}")

            
            final_df = main_df[~main_df['DUPLICATE_ID'].isin(ids_to_be_dropped)].copy()
            final_df['DUPLICATE_ID'] = pd.factorize(final_df['DUPLICATE_ID'])[0] + 1

            # --- build the combined credit/debit pairs DataFrame ---
            if all_pairs_dropped:
                # safe concat (handles list-of-DataFrames); drop duplicates and reset index
                capture_log_message(f"No of rows before dropping duplicate rows in credit debit pairs: {len(all_pairs_dropped)}")
                credit_debit_pairs_df = original_main_df.loc[list(all_pairs_dropped)].copy().drop_duplicates().reset_index(drop=True)
                capture_log_message(f"No of rows after dropping duplicate rows in credit debit pairs: {len(credit_debit_pairs_df)}")
            else:
                credit_debit_pairs_df = pd.DataFrame(columns=main_df.columns)

            return final_df, credit_debit_pairs_df
        
        
        
        

                
        
        
        

        def assign_pair_id_and_upload_pairs(credit_debit_pairs_df, month, max_pair_id):
            """
            Assign PAIR_ID to credit_debit_pairs_df based on max_pair_id parameter
            Filters duplicates and uploads to database
            Args:
                credit_debit_pairs_df (pd.DataFrame): Credit/Debit pairs from handle_duplicate_invoice_results()
                month (str): Month label for table name (e.g., 'm03_2024')
                max_pair_id (int): Maximum PAIR_ID from all existing tables
            Returns:
                None
            """
            try:
                table_name = f'credit_debit_pairs_{month}'
                existing_pairs_df = DB.read_table(table_name)
                
                if existing_pairs_df.empty:
                    capture_log_message(f"Table {table_name} is empty, starting with PAIR_ID={max_pair_id + 1}")
                    
                    # Validate before factorize
                    if credit_debit_pairs_df.empty:
                        capture_log_message(f"Warning: No data to upload for {table_name}")
                        return None
                    
                    # No existing data, assign PAIR_IDs starting from max_pair_id + 1
                    credit_debit_pairs_df['PAIR_ID'] = pd.factorize(credit_debit_pairs_df['DUPLICATE_ID'])[0] + max_pair_id + 1
                    capture_log_message(f"Assigned PAIR_IDs {max_pair_id + 1} to {len(credit_debit_pairs_df)} (new table)")
                else:
                    capture_log_message(f"Found {len(existing_pairs_df)} existing records in {table_name}")
                    # Filter out duplicates
                    new_rows = filter_out_duplicate_credit_debit_pairs(existing_pairs_df, credit_debit_pairs_df)
                    
                    if new_rows.empty:
                        capture_log_message("All records are duplicates, no new records to upload")
                        return None
                                        
                    # Assign PAIR_ID to new rows based on DUPLICATE_ID
                    new_rows['PAIR_ID'] = pd.factorize(new_rows['DUPLICATE_ID'])[0] + max_pair_id + 1
                    credit_debit_pairs_df = new_rows.copy()

                credit_debit_pairs_df['audit_id'] = audit_id
                credit_debit_pairs_df['PO_NUMBER'] = credit_debit_pairs_df['PURCHASE_ORDER_NUMBER']

                required_columns = [
                    'audit_id', 'PAIR_ID', 'COMPANY_CODE', 'SUPPLIER_NAME',
                    'INVOICE_NUMBER', 'INVOICE_DATE', 'INVOICE_AMOUNT',
                    'DEBIT_CREDIT_INDICATOR', 'POSTED_DATE', 'ENTRY_ID',
                    'PO_NUMBER','REGION','DOC_TYPE','is_current_data'
                ]
                
                # Upload to database
                DB.upload_data_to_database(credit_debit_pairs_df[required_columns], table_name)
                capture_log_message(f"Successfully uploaded {len(credit_debit_pairs_df)} credit/debit pair records to {table_name}")
               
                return None
            
            except Exception as e:
                capture_log_message(current_logger=g.error_logger, 
                                    log_message=f"Error in assign_pair_id_and_upload_pairs: {str(e)}")
                return None

        def save_dropped_series_pattern_rows_to_db(dropped_df, audit_id, company_dict, vendor_dict, user_dict, max_series_id):
            """
            Save series pattern dropped rows to database with quarter-wise sharding
            Args:
                dropped_df: DataFrame containing dropped series pattern rows
                audit_id: Audit ID
                company_dict, vendor_dict, user_dict: Mapping dictionaries
                max_series_id: Maximum DUPLICATES_ID from all existing series tables
            """
            
            # Debug: Log available columns before processing
            capture_log_message(f"Columns in dropped_df before processing: {list(dropped_df.columns)}")

            dropped_df.rename(columns={"POSTING_DATE":"POSTED_DATE","COMPANY_CODE":"COMPANY",
                                            "DUPLICATE_ID":"DUPLICATES_ID","SUPPLIER_ID":"SUPPLIER",
                                            "DUPLICATE_RISK_SCORE":"RISK_SCORE"},inplace= True) # type: ignore

            dropped_df['COMPANYID'] =  dropped_df['COMPANY'].apply(lambda x: company_dict.get(x,0))
            dropped_df['POSTED_BY'] = dropped_df['POSTED_BY'].apply(lambda x: user_dict.get(x,x))
            dropped_df['VENDORID']  = dropped_df['VENDORCODE'].apply(lambda x:vendor_dict.get(x,0))
            # logging for debugging ,value counts of these columns
            capture_log_message(f"Dropped Series Pattern Rows - COMPANYID value counts:\n{dropped_df['COMPANYID'].value_counts()}")
            capture_log_message(f"Dropped Series Pattern Rows - VENDORID value counts:\n{dropped_df['VENDORID'].value_counts()}")
            capture_log_message(f"Dropped Series Pattern Rows - POSTED_BY value counts:\n{dropped_df['POSTED_BY'].value_counts()}")
            
            dropped_df.rename(columns={'SUPPLIER_NAME':'VENDOR_NAME'},inplace=True)
            
            
            dup_inv_cols = ['audit_id',"POSTED_DATE","POSTED_BY","RISK_SCORE","SCENARIO_ID","VENDORID","COMPANYID",
                            "COMPANY","DUPLICATES_ID","NO_OF_DUPLICATES","INVOICE_DATE","INVOICE_NUMBER",
                            "INVOICE_AMOUNT","PrimaryKeySimple","ENTRY_ID",'PAYMENT_DATE','DEBIT_CREDIT_INDICATOR',
                            'PURCHASE_ORDER_NUMBER','MONTH_LABEL','REGION','DOC_TYPE','is_current_data','VENDORCODE','VENDOR_NAME']
            dropped_df['audit_id'] = audit_id
            dropped_df_to_upload = dropped_df[dup_inv_cols].copy()
            # Clean up any duplicate columns from prior merge operations
            dropped_df_to_upload = cleanup_duplicate_columns(dropped_df_to_upload, log_prefix="[SERIES_PATTERN_CLEANUP_AFTER_COLUMN_SELECTION]")
            
            # Apply ID offset
            dropped_df_to_upload['DUPLICATES_ID'] = dropped_df_to_upload['DUPLICATES_ID'] + max_series_id
            
            # Calculate duplicate_label for series invoices
            def get_series_label(group):
                """Extract MONTH_LABEL from current data rows, fallback to any row if none exist"""
                try:
                    current_labels = group.loc[group['is_current_data'], 'MONTH_LABEL']
                    return current_labels.iloc[0] if not current_labels.empty else group['MONTH_LABEL'].iloc[0]
                except (KeyError, IndexError) as e:
                    capture_log_message(current_logger=g.error_logger,
                                      log_message=f"Error extracting series label: {str(e)}. Using first available MONTH_LABEL")
                    return group['MONTH_LABEL'].iloc[0] if 'MONTH_LABEL' in group.columns and not group.empty else 'unknown'
            
            series_label_dict = dropped_df_to_upload.groupby('DUPLICATES_ID').apply(get_series_label).to_dict()
            dropped_df_to_upload['duplicate_label'] = dropped_df_to_upload['DUPLICATES_ID'].map(series_label_dict)
            
            # Loop through quarters and upload
            for mth in dropped_df_to_upload['duplicate_label'].unique().tolist():
                series_df_mth = dropped_df_to_upload[dropped_df_to_upload['duplicate_label'] == str(mth)]
                series_df_mth_copy = series_df_mth.copy()
                series_df_mth_copy.drop("duplicate_label", axis=1, inplace=True)
                series_df_mth_copy.drop('MONTH_LABEL', axis=1, inplace=True)
                capture_log_message(log_message='Uploading series duplicate invoices for quarter {}, data shape:{}'.format(str(mth), series_df_mth_copy.shape))
                DB.upload_data_to_database(series_df_mth_copy, f'series_duplicate_invoices_{str(mth)}')
            
            capture_log_message(log_message='Uploading series duplicate invoices Finished')
            return None
    
        def filter_series_invoices_from_duplicate_results(df,company_dict,vendor_dict,user_dict):
            """
            This function is a post processing function on duplicate invoice module, 
            it makes sure that the final duplicate invoice results does not have any Series like pattern within Invoice Numbers

            Series Pattern:
                INV12345 vs INV12355    -- Series pattern
                AUX-43564 vs AUX-43511  -- Series Pattern
            """

            capture_log_message(f"No of rows before dropping invoices that match the series pattern:{df.shape}")

            def process_group(group):
                to_drop = set()
        
                # Check all possible pairs
                for i, j in combinations(group.index, 2):
                    inv1 = group.at[i, "INVOICE_NUMBER"]
                    inv2 = group.at[j, "INVOICE_NUMBER"]
        
                    if is_sequential_series(str(inv1), str(inv2)):
                        # mark both as drop (you can choose only one if needed)
                        to_drop.add(i)
                        to_drop.add(j)
        
                # Drop identified rows
                group = group.drop(index=list(to_drop))
        
                # If fewer than 2 rows remain, drop the whole group
                if len(group) < 2:
                    return pd.DataFrame(columns=group.columns)
        
                return group


            # Store original data to track what gets dropped
            original_df = df.copy()
            
            filtered_df = df.groupby("DUPLICATE_ID", group_keys=False).apply(process_group)
            
            # Find dropped rows by comparing original vs filtered
            original_indices = set(original_df.index)
            filtered_indices = set(filtered_df.index)
            dropped_indices = original_indices - filtered_indices
            
            # Save dropped rows to CSV
            if dropped_indices:
                try:
                    dropped_series_rows = original_df.loc[list(dropped_indices)].copy()
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"duplicate_series_pattern_dropped_audit_{audit_id}_{timestamp}.csv"
                    csv_filepath = os.path.join(g.processed_output_folder, csv_filename)
                    
                    dropped_series_rows.to_csv(csv_filepath, index=True)
                    capture_log_message(f"Saved {len(dropped_series_rows)} series-pattern rows to: {csv_filepath}")
                    save_dropped_series_pattern_rows_to_db(dropped_series_rows, audit_id, company_dict, vendor_dict, user_dict, max_series_id)
                    
                except Exception as csv_error:
                    import traceback
                    traceback_str = traceback.format_exc()
                    capture_log_message(current_logger=g.error_logger, 
                                    log_message=f"Error saving series-pattern rows to CSV: {str(csv_error)}\n{traceback_str}")

            capture_log_message(f"No of rows after dropping the series pattern matching invoices:{filtered_df.shape}")
            
            # Validate before factorize
            if filtered_df.empty:
                capture_log_message("Warning: All series pattern invoices were dropped, returning empty DataFrame")
                return filtered_df
            
            filtered_df['DUPLICATE_ID'] = pd.factorize(filtered_df['DUPLICATE_ID'])[0]+1

            return filtered_df
        


        def handle_historical_data(df):
            current_df = df.copy()
          
            historical_data_df = read_ddf_from_path(g.client_folder_path, g.hist_date_strt, g.hist_date_end)
            if historical_data_df.empty:
                capture_log_message("No historical data files found in path!!")
                # Return current data with is_current_data flag set
                current_df['is_current_data'] = True
                return current_df
            historical_data_df = historical_data_df.groupby('ACCOUNTING_DOC').first().reset_index()
            historical_data_df['ACCOUNTING_DOC'] = historical_data_df['ACCOUNTING_DOC'].astype(str).str.split('-').str[0]
            historical_data_df['COMPANY_NAME'] = historical_data_df['COMPANY_NAME'].astype(str)
            historical_data_df['SUPPLIER_ID'] = historical_data_df['SUPPLIER_ID'].astype(str)
            historical_data_df.rename(columns={'SUPPLIER_ID':'VENDORCODE',
                                        'POSTED_DATE':'POSTING_DATE',"ACCOUNTING_DOC":'ENTRY_ID'},inplace=True)
            historical_data_df['audit_id'] = g.audit_id

            current_data_batch_id = current_df['batch_id'].unique()
            if len(current_data_batch_id)>1:
                capture_log_message(f"WARNING: More than one batch_id found in current data: {current_data_batch_id}",current_logger=g.error_logger)

            elif len(current_data_batch_id)==1:
                g.current_data_batch_id = current_data_batch_id[0]
                # Make sure historical data does not include current batch_id
                # Check if 'batch_id' column exists in historical data
                if 'batch_id' in historical_data_df.columns:
                    capture_log_message(f"Shape of historical data before filtering current batch_id: {historical_data_df.shape}")
                    historical_data_df = historical_data_df[historical_data_df['batch_id'] != g.current_data_batch_id]
                    capture_log_message(f"Shape of historical data after filtering current batch_id: {historical_data_df.shape}")
                    capture_log_message(f"Filtered historical data to exclude current batch_id: {g.current_data_batch_id}")
                    # If no historical data remains, just use current data
                    if historical_data_df.empty:
                        capture_log_message("No historical data remaining after filtering by current batch_id, returning only current data")
                        current_df['is_current_data'] = True
                        return current_df
                else:
                    capture_log_message(f"WARNING: 'batch_id' column not found in historical data, skipping batch_id filtering",current_logger=g.error_logger)
            else:
                capture_log_message(f"WARNING: No batch_id found in current data, historical data will not be filtered by batch_id",current_logger=g.error_logger)
            
            # Safety check: ensure dataframe is not empty before accessing columns
            if not historical_data_df.empty:
                g.unique_company_codes_in_hist_data = list(historical_data_df['COMPANY_CODE'].unique())
                g.unique_vendor_names_in_hist_data = historical_data_df.drop_duplicates(subset=['VENDORCODE','SUPPLIER_NAME'],keep='first')[['VENDORCODE','SUPPLIER_NAME']]
                g.unique_user_names_in_hist_data = list(historical_data_df['POSTED_BY'].unique())
            else:
                g.unique_company_codes_in_hist_data = []
                g.unique_vendor_names_in_hist_data = historical_data_df[['VENDORCODE','SUPPLIER_NAME']]  # Empty dataframe with correct structure
                g.unique_user_names_in_hist_data = []

            # DB.add_values_in_ap_vendorlist_table()
            DB.add_values_in_mscompany_table()
            DB.add_values_in_msfmsuser_table()
            
            if 'PURCHASE_ORDER_NUMBER' not in current_df.columns:
                current_df['PURCHASE_ORDER_NUMBER'] = None
                
            if 'PURCHASE_ORDER_NUMBER' not in historical_data_df.columns:
                historical_data_df['PURCHASE_ORDER_NUMBER'] =  None
            
            columns_to_filter = ['audit_id','ENTRY_ID','INVOICE_NUMBER','INVOICE_DATE','INVOICE_AMOUNT','POSTED_BY','POSTING_DATE',
                                 'COMPANY_CODE','SUPPLIER_NAME','PAYMENT_DATE','VENDORCODE','DEBIT_CREDIT_INDICATOR',
                                 'PURCHASE_ORDER_NUMBER','MONTH_LABEL','REGION','DOC_TYPE']
            current_df = current_df[columns_to_filter]
            historical_data_df = historical_data_df[columns_to_filter]

            current_df['is_current_data'] = True
            historical_data_df['is_current_data'] = False

            capture_log_message(f"Current data column list: {current_df.columns.tolist()}")
            capture_log_message(f"Historical data column list: {historical_data_df.columns.tolist()}")


            capture_log_message(f"Shape of historical data df: {historical_data_df.shape}")
            current_vendors_list = current_df['SUPPLIER_NAME'].unique()
            capture_log_message(f"Length of unique vendors' list: {len(current_vendors_list)}")
            vendor_mask = similar_supplier_names(historical_data_df, current_vendors_list)
            capture_log_message(f"Shape of filtered historical df by similar vendors: {vendor_mask.sum()}")
            invoice_mask = get_matching_invoice_rows(historical_data_df, current_df['INVOICE_NUMBER'].unique())
            capture_log_message(f"Shape of filtered historical df by matching invoices: {invoice_mask.sum()}")
            invoice_date_mask = get_matching_rows_with_same_invoice_date(historical_data_df,current_df['INVOICE_DATE'].unique())
            capture_log_message(f"Shape of filtered historical df by matching invoice dates: {invoice_date_mask.sum()}")
            # Merge filtered dataframes and drop duplicates
            filtered_df = historical_data_df[vendor_mask | invoice_mask | invoice_date_mask].copy()
            filtered_df = filtered_df.reset_index(drop=True)  # Reset index to avoid reindexing errors
            capture_log_message(f"Shape of filtered historical df after merging invoices wise and vendor wise filtering: {filtered_df.shape}")
            combined_df = pd.concat([filtered_df, current_df],ignore_index=True)
            capture_log_message(f"Shape of final combined df: {combined_df.shape}")
            return combined_df
        
        try:
            #Duplicate Invoice Module
            # historical_data_df = read_ddf_from_path(g.client_folder_path, g.hist_date_strt, g.hist_date_end)

            if hasattr(g,'hist_date_strt')  and hasattr(g,'hist_date_end') and g.hist_date_strt is not None and g.hist_date_end is not None:
                capture_log_message(f"Inside Handle historical data:{dup_inv_data.shape}")
                dup_inv_data_hist = handle_historical_data(dup_inv_data)
                capture_log_message(f"Final Shape of Data sent to duplicate invoice {dup_inv_data_hist.shape}")
                # Apply reversal filtering before duplicate invoice processing
                capture_log_message("Applying reversal entry filtering before duplicate invoice detection")
                # dup_inv_data_hist = filter_reversal_entries_before_duplicate_check(dup_inv_data_hist)
                
                capture_log_message(f"Final Shape of Data sent to duplicate invoice after filtering:{dup_inv_data_hist.shape}")
                vals = dup_inv_data_hist['SUPPLIER_NAME'].value_counts(dropna=False)
                capture_log_message(f"Vendor count:,{vals}")
                capture_log_message(f"Value count of debit credit indicator :{dup_inv_data_hist['DEBIT_CREDIT_INDICATOR'].value_counts()}")
                output = predict.make_prediction(input_data=dup_inv_data_hist.copy())
                
            else:
                capture_log_message("No historical data handling for duplicate invoice detection")
                capture_log_message(f"Hist Start and hist End date :{g.hist_date_strt},{g.hist_date_end}")
                capture_log_message(f"Shape of Data before duplicate invoice filtering:{dup_inv_data.shape}")
                if 'PURCHASE_ORDER_NUMBER' not in dup_inv_data.columns:
                    dup_inv_data['PURCHASE_ORDER_NUMBER'] = None
                columns_to_filter = ['audit_id','ENTRY_ID','INVOICE_NUMBER','INVOICE_DATE','INVOICE_AMOUNT','POSTED_BY','POSTING_DATE',
                                     'COMPANY_CODE','SUPPLIER_NAME','PAYMENT_DATE','VENDORCODE','DEBIT_CREDIT_INDICATOR','PURCHASE_ORDER_NUMBER',
                                     'MONTH_LABEL','REGION','DOC_TYPE']
                dup_inv_data = dup_inv_data[columns_to_filter]
                dup_inv_data['is_current_data'] = True
                # Apply reversal filtering before duplicate invoice processing
                capture_log_message("Applying reversal entry filtering before duplicate invoice detection")
                # dup_inv_data = filter_reversal_entries_before_duplicate_check(dup_inv_data)
                
                capture_log_message(f"Final Shape of Data sent to duplicate invoice after filtering:{dup_inv_data.shape}")
                vals = dup_inv_data['SUPPLIER_NAME'].value_counts(dropna=False)
                capture_log_message(f"Vendor count:,{vals}")
                capture_log_message(f"Value count of debit credit indicator :{dup_inv_data['DEBIT_CREDIT_INDICATOR'].value_counts()}")
                output = predict.make_prediction(input_data=dup_inv_data.copy())
                
            # Get all max IDs upfront for sharded tables
            list_of_months = get_quarters()
            max_duplicate_id = get_max_duplicate_id(list_of_months)
            max_pair_id = get_max_pair_id(list_of_months)  # Need to create this
            max_series_id = get_max_series_id(list_of_months)  # Need to create this
                
            company = DB.read_table_for_client(client_id=g.client_id,tablename="mscompany",columns=['COMPANYID','COMPANY_CODE'])
            vendor = DB.read_table_for_client(client_id=g.client_id,tablename='ap_vendorlist',columns=['VENDORID','VENDORCODE'])
            users = DB.read_table_for_client(client_id=g.client_id,tablename='msfmsuser',columns=['FMSUSERID','FMSUSER_CODE'])
            
            company_dict = dict(zip(company.COMPANY_CODE,company.COMPANYID))
            users_dict = dict(zip(users.FMSUSER_CODE,users.FMSUSERID))
            vendor_dict = dict(zip(vendor.VENDORCODE,vendor.VENDORID))
            
            capture_log_message(f"Company Dict Sample: {list(company_dict.items())[:25]}")
            capture_log_message(f"Vendor Dict Sample: {list(vendor_dict.items())[:25]}")
            capture_log_message(f"Users Dict Sample: {list(users_dict.items())[:25]}")
            
            if output['output'] is not None:
                capture_log_message('Duplicate Invoice Module is Completed')
                df_output = output['output']
                # Clean up any duplicate columns from prediction model output at source
                df_output = cleanup_duplicate_columns(df_output, log_prefix="[DUPLICATE_OUTPUT_SOURCE_CLEANUP]")
                if not df_output.empty:
                    df_output["DUPLICATE_RISK_SCORE"] = (df_output["DUPLICATE_RISK_SCORE"]).round(2)

                    df_output.rename(columns={"POSTING_DATE":"POSTED_DATE"},inplace= True)
                    capture_log_message(f"List of all columns in duplicate invoice results: {df_output.columns.tolist()}")
                    df_output, filtered_cred_deb_pairs_df  = handle_duplicate_invoice_results(df_output)

                    if not filtered_cred_deb_pairs_df.empty:
                        # Calculate duplicate_label for credit_debit_pairs
                        def get_pair_label(group):
                            """Extract MONTH_LABEL from current data rows, fallback to any row if none exist"""
                            try:
                                current_labels = group.loc[group['is_current_data'], 'MONTH_LABEL']
                                return current_labels.iloc[0] if not current_labels.empty else group['MONTH_LABEL'].iloc[0]
                            except (KeyError, IndexError) as e:
                                capture_log_message(current_logger=g.error_logger,
                                                  log_message=f"Error extracting pair label: {str(e)}. Using first available MONTH_LABEL")
                                return group['MONTH_LABEL'].iloc[0] if 'MONTH_LABEL' in group.columns and not group.empty else 'unknown'
                        
                        pair_label_dict = filtered_cred_deb_pairs_df.groupby('DUPLICATE_ID').apply(get_pair_label).to_dict()
                        filtered_cred_deb_pairs_df['duplicate_label'] = filtered_cred_deb_pairs_df['DUPLICATE_ID'].map(pair_label_dict)
                        
                        # Loop through quarters and upload
                        for mth in filtered_cred_deb_pairs_df['duplicate_label'].unique().tolist():
                            pairs_df_mth = filtered_cred_deb_pairs_df[filtered_cred_deb_pairs_df['duplicate_label'] == str(mth)]
                            pairs_df_mth_copy = pairs_df_mth.copy()
                            pairs_df_mth_copy.drop("duplicate_label", axis=1, inplace=True)
                            pairs_df_mth_copy.drop('MONTH_LABEL', axis=1, inplace=True)
                            
                            # Calculate number of unique DUPLICATE_IDs in this quarter for ID offset tracking
                            num_unique_pairs_in_quarter = pairs_df_mth_copy['DUPLICATE_ID'].nunique()
                            
                            capture_log_message(log_message='Uploading credit/debit pairs for quarter {}, data shape:{}, unique pairs:{}'.format(str(mth), pairs_df_mth_copy.shape, num_unique_pairs_in_quarter))
                            assign_pair_id_and_upload_pairs(pairs_df_mth_copy, mth, max_pair_id)
                            
                            # Update max_pair_id to prevent ID collision across quarters
                            max_pair_id += num_unique_pairs_in_quarter
                            capture_log_message(log_message='Updated max_pair_id to {} after processing quarter {}'.format(max_pair_id, str(mth)))
                        
                        capture_log_message(log_message='Uploading credit/debit pairs Finished')
                    else:
                        capture_log_message("No filtered_credit/debit pairs generated from duplicate detection")

                    df_output = filter_series_invoices_from_duplicate_results(df_output,company_dict,vendor_dict,users_dict)
                    df_output["DUPLICATE_RISK_SCORE"] = (df_output["DUPLICATE_RISK_SCORE"]).round(2)

                    duplicate_res_summary = df_output.groupby('SCENARIO_ID').agg(
                        NO_OF_ROWS=('PrimaryKeySimple', 'count'),
                        NO_OF_GROUPS=("DUPLICATE_ID", 'nunique')
                    ).reset_index()


                    score_distribution = (df_output.groupby(["SCENARIO_ID", "ScoreBucket"]).size().unstack(fill_value=0))
                    capture_log_message('Final Duplicate invoice results summary')
                    capture_log_message(f"{duplicate_res_summary}")
                    capture_log_message(f"{score_distribution}") # type: ignore
                    
                    df_output.rename(columns={"COMPANY_CODE":"COMPANY","DUPLICATE_ID":"DUPLICATES_ID","SUPPLIER_ID":"SUPPLIER","DUPLICATE_RISK_SCORE":"RISK_SCORE"},inplace= True)
                    capture_log_message(f" VENDOR__CODE {df_output['VENDORCODE'].value_counts().head(10)}")
                    capture_log_message(f"VENDOR__NAME {df_output['SUPPLIER_NAME'].value_counts().head(10)}")
                    df_output['VENDORCODE'] = df_output['VENDORCODE'].astype(str)
                    df_output['COMPANYID'] =  df_output['COMPANY'].apply(lambda x: company_dict.get(x,0))
                    df_output['POSTED_BY'] = df_output['POSTED_BY'].apply(lambda x: users_dict.get(x,x))
                    df_output['VENDORID']  = df_output['VENDORCODE'].apply(lambda x:vendor_dict.get(x,0))
                    # Capture value counts for debugging
                    capture_log_message(f"VENDORID counts after mapping: {df_output['VENDORID'].value_counts()}")
                    capture_log_message(f"COMPANYID counts after mapping: {df_output['COMPANYID'].value_counts()}")
                    capture_log_message(f"POSTED_BY counts after mapping: {df_output['POSTED_BY'].value_counts()}")
                    df_output.rename(columns={"SUPPLIER_NAME":"VENDOR_NAME"},inplace= True)
                    # df_output.to_excel('Duplicate_invoice_results.xlsx',index=False)
                    dup_inv_cols = ['audit_id',"POSTED_DATE","POSTED_BY","RISK_SCORE","SCENARIO_ID","VENDORID","COMPANYID","COMPANY",
                                    "DUPLICATES_ID","NO_OF_DUPLICATES","INVOICE_DATE","INVOICE_NUMBER","INVOICE_AMOUNT","PrimaryKeySimple",
                                    "ENTRY_ID",'PAYMENT_DATE','DEBIT_CREDIT_INDICATOR',
                                    'PURCHASE_ORDER_NUMBER','MONTH_LABEL','REGION','DOC_TYPE','VENDORCODE','VENDOR_NAME','is_current_data'] # ACCOUNT_DOC_ID
                    # REmoved cols - POSTED_BY,VENDORID,COMPANYID
                    # dup_inv_cols = ['audit_id',"POSTED_DATE","RISK_SCORE","COMPANY","DUPLICATES_ID","NO_OF_DUPLICATES","INVOICE_DATE","INVOICE_NUMBER","INVOICE_AMOUNT","PrimaryKeySimple","ENTRY_ID"]
                    
                    df_output = df_output[dup_inv_cols]
                    # Clean up any duplicate columns that may have been created during merge operations
                    df_output = cleanup_duplicate_columns(df_output, log_prefix="[DUPLICATE_RESULTS_CLEANUP_AFTER_COLUMN_SELECTION]")
                    
                    df_output['DUPLICATES_ID'] = df_output['DUPLICATES_ID'] + max_duplicate_id
                    df_output['POSTED_DATE'] = pd.to_datetime(df_output['POSTED_DATE'])

                    df_output['audit_id'] =  audit_id
                    df_output['PURCHASE_ORDER_NUMBER'] = df_output['PURCHASE_ORDER_NUMBER'].replace(0,None).astype('string')

                    # if two diff credit debit indicators are present within same group filter and drop those rows

                    mask = df_output.groupby('DUPLICATES_ID')['DEBIT_CREDIT_INDICATOR'].transform('nunique')>1
                    capture_log_message(f'No of duplicate groups having both credit and debit entries:{mask.sum()}')
                    dropped_rows = df_output[mask]
                    df_output = df_output[~mask]
                    capture_log_message(f'Shape after dropping duplicate groups having both credit and debit entries:{df_output.shape}')
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"dupl_invalid_groups_{audit_id}_{timestamp}.csv"
                    csv_filepath = os.path.join(g.processed_output_folder, csv_filename)
                    dropped_rows.to_csv(csv_filepath, index=True)
                    capture_log_message(f"Saved {len(dropped_rows)} invalid duplicate groups rows to: {csv_filepath}")

                    optimize_flag, model_path = is_optimisation_model_available(module='Duplicate_Invoice')
                    opt_status_flag = "OPTIMISED_DUPLICATE_INVOICE_POSTING" in opt_rule_weights
                    capture_log_message(f"Optimisation model for duplicate invoice: model_flag: {optimize_flag}, opt_status_flag: {opt_status_flag}")
                    if optimize_flag and opt_status_flag:
                        from Optimisation_module.Duplicate_Invoice.Components.predict import optimize_duplicate_scores
                        capture_log_message("Started Duplicate Invoice Optimization")
                        df_output['OPTIMIZED_RISK_SCORE'] = optimize_duplicate_scores(original_df=df_output,pipeline_path=model_path)
                        capture_log_message("Completed Duplicate Invoice Optimization")
                    else:
                        capture_log_message("Optimization model for duplicate invoice not available or optimisation not enabled, skipping optimization step")

            else:
                raise DuplicateScoringException('No data returned from duplicate invoice module')
        
        except Exception as e:
            raise DuplicateScoringException(e)


        try:
            
            capture_log_message(log_message='Upload to DB Started')
            start_upload_time = datetime.now(timezone.utc)
            capture_log_message(log_message='Uploading Transaction Scores to ap_transactionscore table')    
            DB.upload_data_to_database(Scored_DF,f'ap_transactionscore_{audit_id}')
            capture_log_message(log_message='Uploading Transaction Scores Finished')
            capture_log_message(log_message='Uploaded the Transaction Scores to ap_transactionscore table,data shape:{}'.format(Scored_DF.shape))
            
            capture_log_message(log_message='Uploading AccountDoc Scores to ap_accountdocscore table')
            DB.upload_data_to_database(Scored_DF_AccountDOC,f'ap_accountdocscore_{audit_id}')
            capture_log_message(log_message='Uploading AccountDoc Scores Finished')
            capture_log_message(log_message='Uploaded AccountDoc Scores to ap_accountdocscore table,data shape:{}'.format(Scored_DF_AccountDOC.shape))
            
            capture_log_message(log_message='Uploading Duplicate invoices to duplicate_invoices table')
            capture_log_message(log_message='Shape of data to be uploaded to duplicate_invoices table:{}'.format(df_output.shape))
            
            # Create a new column called duplicate_label
            # Group rows by duplicate id, and assign the max of MONTH_LABEL as duplicate_label
            # df_output['duplicate_label'] = df_output.groupby('DUPLICATES_ID')['MONTH_LABEL'].transform('max')


            def get_duplicate_label(group):
                """Extract MONTH_LABEL from current data rows, fallback to any row if none exist"""
                try:
                    current_labels = group.loc[group['is_current_data'], 'MONTH_LABEL']
                    return current_labels.iloc[0] if not current_labels.empty else group['MONTH_LABEL'].iloc[0]
                except (KeyError, IndexError) as e:
                    capture_log_message(current_logger=g.error_logger,
                                      log_message=f"Error extracting duplicate label: {str(e)}. Using first available MONTH_LABEL")
                    return group['MONTH_LABEL'].iloc[0] if 'MONTH_LABEL' in group.columns and not group.empty else 'unknown'
            
            # Create dict mapping DUPLICATES_ID to duplicate_label - check if dataframe is empty and has the column
            if not df_output.empty and 'DUPLICATES_ID' in df_output.columns:
                duplicate_label_dict = df_output.groupby('DUPLICATES_ID').apply(get_duplicate_label).to_dict()
                df_output['duplicate_label'] = df_output['DUPLICATES_ID'].map(duplicate_label_dict)
            else:
                capture_log_message(f"Warning: df_output is empty or missing 'DUPLICATES_ID' column. Skipping duplicate label assignment.")
                # to do: check what gets returned when no duplicates found
            
            
            
            if output['output'] is not None:
                if not df_output.empty:
                    for mth in df_output['duplicate_label'].unique().tolist():
                        df_output_mth = df_output[df_output['duplicate_label'] == str(mth)]
                        df_output_mth.drop("duplicate_label", axis=1, inplace=True)
                        df_output_mth.drop('MONTH_LABEL', axis=1, inplace=True)
                        capture_log_message(log_message='Uploading Duplicate invoices for quarter {}, data shape:{}'.format(str(mth),df_output_mth.shape))
                        DB.upload_data_to_database(df_output_mth,f'duplicate_invoices_{str(mth)}')
                    capture_log_message(log_message='Uploading Duplicate invoices Finished')
                    capture_log_message(log_message='Uploaded Duplicate invoices to duplicate_invoices table,data shape:{}'.format(df_output.shape))
            else:
                capture_log_message(log_message='No Duplicate invoices Found')
                
            end_upload_time = datetime.now(timezone.utc)
            capture_log_message(log_message='Upload to DB FInished')
            
            DB.update_tables(audit_id)
            capture_log_message(log_message="IS_SCORED Flag Updated")
        except Exception as e:
            raise ScoringDataStorageException(e)
    
        
        capture_log_message(log_message='Upload score details, time taken:{}'.format(end_upload_time-start_upload_time))
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        
        capture_log_message(current_logger=g.stage_logger,log_message='AP Scoring Completed Transaction level',
                            data_shape=Scored_DF.shape,time_taken=time_taken_for_scoring,start_time=scoring_start_time,
                            end_time=scoring_end_time)
        
        capture_log_message(current_logger=g.stage_logger,log_message='AP Scoring Completed AccountDoc level',
                            data_shape=Scored_DF_AccountDOC.shape,time_taken=time_taken_for_scoring,
                            start_time=scoring_start_time,end_time=scoring_end_time)
        
        # process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=True,
        #                                        date_list=[scoring_start_time],volume_list=[Scored_DF.shape],
        #                                        time_taken_list=[time_taken_for_scoring,],
        #                                        description_list=['AP Scoring Completed'],
        #                                        )
        g.ap_scoring_shape = Scored_DF.shape[0]
        # add_data_to_external_api_call(key='log',json_value={'volume':Scored_DF.shape[0]})
        update_total_account_doc_for_audit_id(Scored_DF_AccountDOC.shape[0])
        return True
        
    
    except tuple(EXCEPTION_DETAILS.keys()) as e:
        import traceback
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        details = EXCEPTION_DETAILS[type(e)]
        err_msg = str(e)+str(traceback.format_exc())
        capture_log_message(current_logger=g.error_logger,
                            log_message='Error occurred during AP Scoring:{}'.format(err_msg),
                            time_taken=time_taken_for_scoring,
                            error_name=details['error_name'])
        
        # process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=False,
        #                                        date_list=[scoring_start_time],volume_list=[],
        #                                        time_taken_list=[time_taken_for_scoring,],
        #                                        description_list=[details['description']])
        
        return False
        
    except Exception as e:
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        import traceback
        traceback_str = traceback.format_exc()
        capture_log_message(current_logger=g.error_logger,log_message='Error occurred during AP Scoring:{}'.format(traceback_str)
                           ,time_taken=time_taken_for_scoring, error_name=utils.GENERAL_SCORING_FAILED)
      
        # process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=False,
        #                                       date_list=[scoring_start_time],volume_list=[],
        #                                       time_taken_list=[time_taken_for_scoring,],
        #                                       description_list=['AP Scoring Failed'],
        #                                       )
      
        return False




    
