import pandas as pd
import numpy as np
from flask import g
from collections import OrderedDict
from GL_Module.db_connector import MySQL_DB
from datetime import datetime, timezone,timedelta
from GL_Module.IForest import Stat_Framework
from GL_Module.Rules import Rules_Framework
from GL_Module.AI import AI_Framework
from GL_Module.Data_Preparation import Preparation
from GL_Module.Monitoring import MonitoringFramework
from GL_Module.Recurring_entry import RecurringEntriesDetector
import traceback
# from GL_Module.logger import logger
# from code1.logger import logger as main_logger
from code1.logger import add_data_to_external_api_call, capture_log_message, process_data_for_sending_internal_mail, update_current_module,update_total_account_doc_for_audit_id
from GL_Module.exceptions import AIScoringException,StatScoringException,RulesScoringException,ScoringDataStorageException,DuplicateScoringException
import utils
from pipeline_data import PipelineData
from Optimisation_module_GL.gl_optimisation_utils import is_optimisation_model_available
from Optimisation_module_GL.GL_Optimised_Rules import optimise_rule_scores, optimised_rules_risk_score, optimised_blended_score_calculation, optimised_acc_doc_lvl_rule_scores


def suppression_rule(data):
    """
    Rule used to Nullify the suppressed transactions from Supperssion rule in rules framework
    """
    cols_to_change = ['AI_RISK_SCORE_RAW','AI_RISK_SCORE','STAT_SCORE','STAT_SCORE_INDEXED']

    data.loc[data['SUPPRESSED_TRAN']>0,cols_to_change] = 0

    data.drop('SUPPRESSED_TRAN',axis=1,inplace=True)

    return data
      
def Account_Doc_level(df,Rule_object:Rules_Framework,weights,monitoring_object:MonitoringFramework):
    """
    Aggregating Scores at Accounting Doc level
    -----------------------------------------
    Input : 
            df :scored transaction data frame to aggregate
            rule_object : to access the function used for remapping
            weights : framework weights
    """

    Prep = Preparation()
    capture_log_message(log_message="Account Doc Level Function Started")
    ai_stat_accountdoc = df.groupby('ACCOUNTDOCID',as_index=False).agg(OrderedDict([('STAT_SCORE',['max']),('AI_RISK_SCORE_RAW',['max'])]))
    ai_stat_accountdoc.columns=['ACCOUNTDOCID','STAT_SCORE','AI_RISK_SCORE_RAW']
    
    min_val = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].min()
    max_val = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].max()

    ai_stat_accountdoc['AI_RISK_SCORE'] = ai_stat_accountdoc['AI_RISK_SCORE_RAW'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1))
    
    min_val = ai_stat_accountdoc['STAT_SCORE'].min()
    max_val = ai_stat_accountdoc['STAT_SCORE'].max()
    # print(min_val,max_val)
    ai_stat_accountdoc['STAT_SCORE_INDEXED'] = ai_stat_accountdoc['STAT_SCORE'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1))
    
    rules_accountdoc = Rule_object.Rule_Score_Calculation_AccountDOC(df)
    monitoring_accountdoc = monitoring_object.monitoring_rules_values_at_account_doc_level(df)
    capture_log_message(f"Shape of Monitoring df at acocunt doc level:{monitoring_accountdoc.shape}")
    
    rules_monitoring_accountdoc = rules_accountdoc.merge(monitoring_accountdoc,how='left',on='ACCOUNTDOCID',validate='one_to_one')

    final_df = rules_monitoring_accountdoc.merge(ai_stat_accountdoc,how='left',on='ACCOUNTDOCID',validate='one_to_one')
    capture_log_message(log_message="Account Doc Level Function Completed")
    return final_df

def Blended_Score_Calculation(Scored_DF,weights):
    """
    Calculating Blended Risk score for the transactions/accountingdocs
    ------------------------------------------------------------------
    Input : Scored DF : Scored dataframe with all 3 framework
            weights : weights for the frameworks
    """
    AI_WEIGHT = weights['ai']
    STAT_WEIGHT = weights['stat']
    RULE_WEIGHT = weights['rule']
    capture_log_message(log_message="Blended Score Calculation Started")
    Scored_DF['BLENDED_RISK_SCORE_RAW'] = (Scored_DF['AI_RISK_SCORE']* AI_WEIGHT) + (Scored_DF['RULES_RISK_SCORE'] * RULE_WEIGHT) + (Scored_DF['STAT_SCORE_INDEXED'] * STAT_WEIGHT)
    
    min_val,max_val = Scored_DF['BLENDED_RISK_SCORE_RAW'].min(),Scored_DF['BLENDED_RISK_SCORE_RAW'].max()
    Scored_DF['BLENDED_RISK_SCORE'] = Scored_DF['BLENDED_RISK_SCORE_RAW'].apply(lambda x: (x - min_val)/(max_val-min_val))
    ###Tweaking score for Unusual Accounting Pattern
    if 'UNUSUAL_ACCOUNTING_PATTERN' in list(Scored_DF.columns):
       import random
       indices = Scored_DF[(Scored_DF['UNUSUAL_ACCOUNTING_PATTERN']!=0) & (Scored_DF['BLENDED_RISK_SCORE'] < 0.7)].index
       for idx in indices:
          Scored_DF.loc[idx,'BLENDED_RISK_SCORE'] = random.randrange(70,100)/100
    Scored_DF['DEVIATION'] = np.where((Scored_DF['BLENDED_RISK_SCORE']>.7) | (Scored_DF['RULES_RISK_SCORE']>0),1,0)
    capture_log_message(log_message="Blended Score Calculation Completed")
    return Scored_DF

def gl_main(audit_id):
    '''
    Main Function of GL
    '''
    EXCEPTION_DETAILS = {
        AIScoringException: {
            'log_message': 'Error occurred during GL Scoring - AI Module Failed: {}',
            'error_name': utils.AI_SCORING_FAILED,
            'description': 'GL Scoring - AI Module Failed',
        },
        StatScoringException: {
            'log_message': 'Error occurred during GL Scoring - STAT Module Failed: {}',
            'error_name': utils.STAT_SCORING_FAILED,
            'description': 'GL Scoring - STAT Module Failed',
        },
        RulesScoringException: {
            'log_message': 'Error occurred during GL Scoring - RULES Module Failed: {}',
            'error_name': utils.RULES_SCORING_FAILED,
            'description': 'GL Scoring - RULES Module Failed',
        },
        DuplicateScoringException: {
            'log_message': 'Error occurred during GL Scoring - DUPLICATE INVOICE Module Failed: {}',
            'error_name': utils.DUPLICATE_SCORING_FAILED,
            'description': 'GL Scoring - DUPLICATE INVOICE Module Failed',
        },
        ScoringDataStorageException: {
            'log_message': 'Error occurred during GL Scoring - ISSUES WITH STORING SCORE RESULTS: {}',
            'error_name': utils.ISSUES_WITH_STORING_RESULTS,
            'description': 'GL Scoring - ISSUES WITH STORING SCORE RESULTS',
        },
    }



    update_current_module('scoring')
    scoring_start_time = datetime.now(timezone.utc)
    try:
        DB = MySQL_DB('GL_Module/DB.json') 
        connection = DB.connect_to_database()

        
        

        # process_details = {"success":0,"failure":0,"message":"Scoring In Progress"}

        # Updating ML Execution Job Status to In Progress
        # Update_Job = MySQL_DB('Exec_DB.json')
        # Update_Job.update_job_status(1,process_details)

        start_db_read = datetime.now(timezone.utc)
        capture_log_message(log_message="Start Reading Data")

        # df = pd.read_sql(f"""SELECT tran.TRANSACTIONID,tran.TRANSACTION_DESC,tran.ACCOUNTDOCID,tran.ENTERED_DATE,tran.ENTERED_BY as ENTERED_BY_USERID,
        # tran.POSTED_DATE,tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.IS_REVERSAL,tran.IS_REVERSED,
        # doc.DOCUMENT_TYPE_CODE as DOC_TYPE,com.COMPANY_CODE,
        # acctdoc.ACCOUNTDOC_CODE,
        # acc.ACCOUNT_CODE,acc.ACCOUNT_DESCRIPTION,
        # posted.FMSUSERID as POSTED_BY_USERID,posted.FMSUSER_CODE as POSTED_BY,
        # entered.FMSUSER_CODE as ENTERED_BY
        # from rpttransaction tran
        # left join msfmsuser entered on tran.ENTERED_BY = entered.FMSUSERID
        # left join msdocumenttype doc on tran.DOCTYPEID=doc.DOCUMENTID
        # left join rptaccountdocument acctdoc on tran.ACCOUNTDOCID=acctdoc.ACCOUNTDOCID
        # left join mschartofaccounts acc on tran.ACCOUNTID=acc.ACCOUNTID
        # left join mscompany com on tran.COMPANYID=com.COMPANYID
        # left join msfmsuser posted on acctdoc.POSTED_BY=posted.FMSUSERID where tran.IS_SCORED=0 and tran.audit_id = {audit_id};""",con=connection)
        
        configurations = pd.read_sql(f"SELECT KEYNAME,KEYVALUE from trconfiguration where module='framework' and STATUS=1",con=connection)
        # finish_db_read = datetime.now(timezone.utc)
        
        # capture_log_message(log_message='Time Taken for Reading {shape} Dimensioned Dataframe {time}'.format(shape=df.shape,time=finish_db_read-start_db_read)
        #                     )
        # connection.close()

        pipe_score_obj = PipelineData()
        df = pipe_score_obj.get_data(f'audit_{audit_id}')
        capture_log_message(f"DF of gl_main shape :{df.shape} ")
        # configs = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
        # if df.shape[0]==0:
        #     process_details = {"success":0,"failure":df.shape[0],"message":"Records are already processed"}
        #     Update_Job.update_job_status(3,process_details)
        #     return 0

        df_rules = df.copy()
        df_ai = df.copy()
        df_stat = df.copy()
        df_monitor = df.copy()
        df_recurring = df.copy()

        model_name_ai = "20241020_AI"
        model_name_stat = "20250521_Stat"
        # model_name_ai = "20220318_AI_Model"
        # model_name_stat = "20220711_Stat_Model"
        capture_log_message(log_message="AI Model Name = {ai} \n Stat Model Name = {stat}".format(ai=model_name_ai,stat=model_name_stat))

        try:
            #Ai Scoring
            ai_scoring_start = datetime.now(timezone.utc)
            AI = AI_Framework(model_name_ai)
            AI_Scored = AI.AI_Scoring(df_ai)
            ai_scoring_end = datetime.now(timezone.utc)
            capture_log_message(log_message='Time Taken for AI Scoring {time}'.format(time=ai_scoring_end-ai_scoring_start))
            capture_log_message(log_message='Shape of data after AI scoring:{shape}'.format(shape=AI_Scored.shape))
        except Exception as e:
            raise AIScoringException(e)
        
        try:
            #Stat Scoring
            stat_scoring_start = datetime.now(timezone.utc)
            # Stat_Scored = pd.DataFrame(index=df_stat.index)
            # Stat_Scored['STAT_SCORE'] = 0  # All raw scores set to 0
            # Stat_Scored['STAT_SCORE_INDEXED'] = 0

            Stat_Model = Stat_Framework(model_name_stat)
            Stat_Scored = Stat_Model.Stat_Scoring(df_stat)
            stat_scoring_end = datetime.now(timezone.utc)
            capture_log_message(log_message='Time Taken for Stat Scoring {time}'.format(time=stat_scoring_end-stat_scoring_start))
            capture_log_message(log_message='Shape of data after Stat scoring:{shape}'.format(shape=Stat_Scored.shape))
        except Exception as e:
            raise StatScoringException(e)
        
        try:
            #Rule Scoring
            rule_scoring_start = datetime.now(timezone.utc)
            Rule = Rules_Framework(configurations)
            Rule_Scored, df_rules_scored = Rule.Run_Rules(df_rules)
            rule_scoring_end = datetime.now(timezone.utc)
            capture_log_message(log_message='Time Taken for Rule Scoring {time}'.format(time=rule_scoring_end-rule_scoring_start))
            capture_log_message(log_message='Shape of data after Rule scoring:{shape}'.format(shape=Rule_Scored.shape))
        except Exception as e:
            raise RulesScoringException(e)
        
        config = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
        capture_log_message(log_message="Configurations are {}".format(config),store_in_db=False)

        #Monitoring framework
        monitoring_start = datetime.now(timezone.utc)
        monitoring = MonitoringFramework(config)
        Monitoring_result = monitoring.run_all_monitoring_rules(df_monitor)
        monitoring_end = datetime.now(timezone.utc)
        capture_log_message(log_message='Time Taken for Monitoring Scoring {time}'.format(time=monitoring_end-monitoring_start))
        capture_log_message(log_message='Shape of data after Monitoring scoring:{shape}'.format(shape=Monitoring_result.shape))

        #Recurring entries detector
        recurring_entry_start = datetime.now(timezone.utc)
        recurring_df = DB.read_table("recurring_entries")
        recurring_detector = RecurringEntriesDetector(transactions_df=df_recurring,recurring_df=recurring_df)
        recurring_status_df = recurring_detector.detect()
        capture_log_message(log_message="Uploading Recurring Entries Status to DB")
        DB.upload_data_to_database( recurring_status_df, "recurring_entries_status")
        capture_log_message(log_message=f"Uploaded Recurring Entries Status (shape={recurring_status_df.shape})")
        recurring_entry_end = datetime.now(timezone.utc)
        capture_log_message(log_message=f"Time Taken for Recurring Entries: {recurring_entry_end - recurring_entry_start}")
        

        #combining all the framework scores and suppressing the scores for selected transactions
        Scored_DF = pd.concat([Rule_Scored,Stat_Scored,AI_Scored,Monitoring_result],axis=1)
        # Scored_DF = suppression_rule(Scored_DF)
        capture_log_message(log_message='Shape of data after concatenation:{shape}'.format(shape=Scored_DF.shape))
        
        framework_weights = {rulename.split("weight_")[1]:float(weight) for rulename,weight in config.items() if rulename.startswith("risk")}
        
        Scored_DF = Blended_Score_Calculation(Scored_DF,framework_weights)
        Scored_DF['audit_id'] = audit_id
        # print(Scored_DF.columns)
        #Scored_DF.to_csv('Scored_Files/Scored_Transactions.csv')
        capture_log_message(log_message='Shape of data after Blended_Score_Calculation :{}'.format(Scored_DF.shape))
        
        Scored_DF_AccountDOC = Account_Doc_level(Scored_DF,Rule,framework_weights,monitoring)
        Scored_DF_AccountDOC['audit_id'] = audit_id
        #Scored_DF_AccountDOC.to_csv('Scored_Files/Scored_AccountDOC.csv')
        capture_log_message(log_message='Shape of data after Account_doc_level scoring:{}'.format(Scored_DF_AccountDOC.shape))
        
        configs_unique = configurations.drop_duplicates(subset=['KEYNAME','KEYVALUE'])
        config = dict(zip(configs_unique.KEYNAME, configs_unique.KEYVALUE))
        rule_weights = {
            key.split("WEIGHT_", 1)[1]: float(val)
            for key, val in config.items()
            if key.startswith("WEIGHT_")
        }
        opt_rule_weights ={key : float(val)  for key, val in config.items() if key.startswith("OPTIMISED_")} 
        
        Scored_DF = optimise_rule_scores(Scored_DF, df_rules_scored, rule_weights, opt_rule_weights)

        Scored_DF = optimised_rules_risk_score(Scored_DF, rule_weights)

        # Opt_Scored_DF = pd.concat([Scored_DF,Stat_Scored,AI_Scored],axis=1)
            
        Opt_Scored_DF = optimised_blended_score_calculation(Scored_DF)

        Opt_Scored_DF_AccountDOC =optimised_acc_doc_lvl_rule_scores(Opt_Scored_DF, rule_weights)

        Final_AccountDOC = (Scored_DF_AccountDOC.merge(Opt_Scored_DF_AccountDOC,
                                                    on="ACCOUNTDOCID",
                                                    how="left",
                                                    suffixes=("", "_opt") )
                                                )

        try:
            #Uploading transactionscores and accountingdoc scores to scores table in the database
            capture_log_message(log_message="Upload to DB Started")    
            start_upload_time = datetime.now(timezone.utc)
            capture_log_message(log_message="Uploading Transaction Scores rpttransactionscore table")    
            DB.upload_data_to_database(Opt_Scored_DF,f'rpttransactionscore_{audit_id}')
            capture_log_message(log_message=" Uploaded the Transaction Scores rpttransactionscore table,data shape:{}".format(Scored_DF.shape))
            
            capture_log_message(log_message="Uploading AccountDoc Scores rptaccountdocscore table")
            DB.upload_data_to_database(Final_AccountDOC,f'rptaccountdocscore_{audit_id}')
            
            capture_log_message(log_message=" Uploaded AccountDoc Scores rptaccountdocscore table,data shape:{}".format(Scored_DF_AccountDOC.shape))
            end_upload_time = datetime.now(timezone.utc)
            
            capture_log_message(log_message="Upload to DB FInished")
            capture_log_message(log_message="Time Taken for Upload to DB:{}".format(end_upload_time-start_upload_time))
            #updating scores and flag in
            capture_log_message(log_message="Updating the IS_SCORED Flag")
            DB.update_tables(audit_id)
            capture_log_message(log_message="IS_SCORED Flag Updated")
        
        except Exception as e:
            raise ScoringDataStorageException(e)
       
       
        # Updating ML Execution Job status to Finished
        # process_details = {"success":Scored_DF.shape[0],"failure":df.shape[0]-Scored_DF.shape[0],"message":"Scoring Completed"}
        # Update_Job.update_job_status(2,process_details)
        
        # print("Upload of {dim} Dimensional DataFrame took {time} Time".format(dim=Scored_DF.shape,time=end_upload_time-start_upload_time))
        # DB.update_adf_process_status(2)
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time - scoring_start_time
        capture_log_message(current_logger=g.stage_logger,log_message='GL Scoring completed Transaction level',
                            start_time=scoring_start_time,end_time=scoring_end_time,
                            time_taken=time_taken_for_scoring,data_shape=Scored_DF.shape)
        
        capture_log_message(current_logger=g.stage_logger,log_message='GL Scoring completed AccountDoc level',
                            time_taken=time_taken_for_scoring,data_shape=Scored_DF_AccountDOC.shape)
        
        # process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=True,
        #                                        date_list=[scoring_start_time],volume_list=[Scored_DF.shape],
        #                                        time_taken_list=[time_taken_for_scoring],description_list=['GL Scoring Completed'])
        g.gl_scoring_shape = Scored_DF.shape
        add_data_to_external_api_call(key='log',json_value={'volume':Scored_DF.shape[0]})
        update_total_account_doc_for_audit_id(Scored_DF_AccountDOC.shape[0])
        return True
    


    except tuple(EXCEPTION_DETAILS.keys()) as e:
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        details = EXCEPTION_DETAILS[type(e)]
        err_msg = str(e)+ str(traceback.format_exc())
        capture_log_message(current_logger=g.error_logger,
                            log_message='Error occurred during GL Scoring:{}'.format(err_msg),
                            time_taken=time_taken_for_scoring,
                            error_name=details['error_name'])
        
        process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=False,
                                               date_list=[scoring_start_time],volume_list=[],
                                               time_taken_list=[time_taken_for_scoring,],
                                               description_list=[details['description']])
        
        return False
    
    except Exception as e:
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time - scoring_start_time
        err_msg = str(e)+ str(traceback.format_exc())
        capture_log_message(current_logger=g.error_logger,log_message='GL Scoring Failed:{}'.format(err_msg),
                            start_time=scoring_start_time,end_time=scoring_end_time,
                            time_taken=time_taken_for_scoring,
                            error_name=utils.GENERAL_SCORING_FAILED)
        
        process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.DATA_SCORING_STAGE,is_success=False,
                                               date_list=[scoring_start_time],
                                               volume_list=[],
                                               time_taken_list=[time_taken_for_scoring],description_list=['GL Scoring Failed'])
        
        
        return False

if __name__ == "__main__":
    # try:
    gl_main()
    # except Exception as e:
    #     Update_Job = MySQL_DB('Exec_DB.json')
    #     process_details = {'success':0,'failure':0,'message':str(e)}
    #     Update_Job.update_job_status(3,process_details)
        
