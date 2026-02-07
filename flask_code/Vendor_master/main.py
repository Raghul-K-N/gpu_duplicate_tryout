import numpy as np
import pandas as pd
from flask import g
import utils
from datetime import datetime, timezone
from Vendor_master.vendor_master import VendorMaster
from AP_Module.db_connector import MySQL_DB
from code1.logger import add_data_to_external_api_call, capture_log_message, process_data_for_sending_internal_mail, update_current_module
from AP_Module.exceptions import AIScoringException,ScoringDataStorageException


def vendor_master_main():

    EXCEPTION_DETAILS = {
        AIScoringException: {
            'log_message': 'Error occurred during Vendor Master Scoring - Module Failed: {}',
            'error_name': 'SCORING_FAILED',
            'description': 'Vendor Master Scoring - Module Failed',
        },
        ScoringDataStorageException: {
            'log_message': 'Error occurred during Vendor Master Scoring - ISSUES WITH STORING SCORE RESULTS: {}',
            'error_name': utils.ISSUES_WITH_STORING_RESULTS,
            'description': 'Vendor Master Scoring - ISSUES WITH STORING SCORE RESULTS',
        }
    }

    scoring_start_time = datetime.now(timezone.utc)
    try:
        DB = MySQL_DB('AP_Module/DB.json')
        with DB.connect_to_database() as connection:
            start_db_read = datetime.now(timezone.utc)
            capture_log_message(log_message='Reading data from database')

            vendor_data = pd.read_sql("""select * from ap_vendorlist;""",con=connection)

            configurations = pd.read_sql(f"SELECT KEYNAME,KEYVALUE from trconfiguration where module='vendor_master' and STATUS=1;",con=connection)
            finish_db_read = datetime.now(timezone.utc)
            time_taken_to_read = finish_db_read-start_db_read
            capture_log_message(log_message='Time taken for reading data from database:{}'.format(time_taken_to_read))
            
        if vendor_data.empty:
            capture_log_message(log_message='No vendor data found in ap_vendorlist table. Skipping scoring process.')

            scoring_end_time = datetime.now(timezone.utc)
            time_taken_for_scoring = scoring_end_time - scoring_start_time
            
            capture_log_message(current_logger=g.stage_logger,
                              log_message='AP Scoring Completed - No data to process',
                              data_shape=(0, 0), time_taken=time_taken_for_scoring,
                              start_time=scoring_start_time, end_time=scoring_end_time)
            
            process_data_for_sending_internal_mail(subject='Scoring Status', stage=utils.VENDOR_MASTER,
                                                 is_success=True, date_list=[scoring_start_time],
                                                 volume_list=[(0, 0)], time_taken_list=[time_taken_for_scoring],
                                                 description_list=['VM Scoring Completed - No data to process'])
            
            add_data_to_external_api_call(key='log', json_value={'volume': 0})
            return True
            
        df_vendor = vendor_data.copy()
        capture_log_message(log_message='Found {} vendors to process'.format(len(df_vendor)))

        try:
            #VM Scoring
            vm_scoring_start = datetime.now(timezone.utc)
            vendor_master = VendorMaster(configurations)
            capture_log_message("Vendor master configuration completed!")
            Scored_DF = vendor_master.Run_Vendor_Rules(df_vendor)
            capture_log_message("Vendor Rules ran successfully!")
            capture_log_message(log_message='Shape of data after VM Scoring:{}'.format(Scored_DF.shape))
            Vend_Scored_DF = vendor_master.Vendor_Rule_Scores_Calculation(Scored_DF)
            capture_log_message(log_message=f'Shape of data after Vendor score calculation:{Vend_Scored_DF.shape}')
            capture_log_message("Vendor score calculation has been completed!")
            vm_scoring_end = datetime.now(timezone.utc)
            time_taken_for_vm_scoring = vm_scoring_end-vm_scoring_start
            capture_log_message(log_message='Time taken for VM Scoring:{}'.format(time_taken_for_vm_scoring))
            
        except Exception as e:
            raise AIScoringException(e)
        
        
        try:
            
           capture_log_message(log_message='Upload to DB Started')
           start_upload_time = datetime.now(timezone.utc)
           capture_log_message(log_message='Uploading vendor_master Scores to vendor_master_score table')
           DB.upload_data_to_database(Vend_Scored_DF,'vendor_master_score')
           capture_log_message(log_message='Uploading vendor_master Scores Finished')
           capture_log_message(log_message='Uploaded the vendor_master Scores to vendor_master_score table,data shape:{}'.format(Vend_Scored_DF.shape))
           capture_log_message(log_message='Reading Data from Score Table')
           with DB.connect_to_database() as connection:
               Score_Table_Data = pd.read_sql("""select * from vendor_master_score;""",con=connection)
           vendor_data.rename(columns={'VENDORID':'vendor_id'},inplace = True)
           capture_log_message(log_message='Completed Reading Data from Score Table with shape:{}'.format(Score_Table_Data.shape))
           Score_Table_Data_filtered = Score_Table_Data[Score_Table_Data['key_field_change_for_vendor'] == 1]
           capture_log_message(log_message='Shape of data after filtering key_field_change for vendor: {}'.format(Score_Table_Data_filtered.shape))
           if not Score_Table_Data_filtered.empty:
               merged_df = vendor_data.merge(Score_Table_Data_filtered, on='vendor_id', how='inner')
               merged_df.rename(columns={'STATUS':'is_change'},inplace=True)
               key_field_changes_df = merged_df[['vendor_id','vendor_master_score_id','is_change']]
               capture_log_message(log_message='Shape of data after key_field_changes_df:{}'.format(key_field_changes_df.shape))
               DB.upload_data_to_database(key_field_changes_df,'vendor_master_key_field_changes')
               capture_log_message(log_message='Uploaded Data to vendor_master_key_field_changes table')
           else:
               capture_log_message(log_message='No key field changes detected, skipping vendor_master_key_field_changes upload')

           end_upload_time = datetime.now(timezone.utc)
           capture_log_message(log_message='Upload to DB Finished')
                      
        except Exception as e:
            raise ScoringDataStorageException(e)
        
        capture_log_message(log_message='Upload score details, time taken:{}'.format(end_upload_time-start_upload_time))
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        
        capture_log_message(current_logger=g.stage_logger,log_message='AP Scoring Completed Transaction level',
                            data_shape=Vend_Scored_DF.shape,time_taken=time_taken_for_scoring,start_time=scoring_start_time,
                            end_time=scoring_end_time)
        
        process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.VENDOR_MASTER,is_success=True,
                                               date_list=[scoring_start_time],volume_list=[Scored_DF.shape],
                                               time_taken_list=[time_taken_for_scoring,],
                                               description_list=['VM Scoring Completed'],
                                               )
        
        add_data_to_external_api_call(key='log',json_value={'volume':Vend_Scored_DF.shape[0]})
        return True
    
    except tuple(EXCEPTION_DETAILS.keys()) as e:
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        details = EXCEPTION_DETAILS[type(e)]
        
        capture_log_message(current_logger=g.error_logger,
                            log_message='{} Error occurred during Vendor Master Scoring:{}'.format(details['description'],str(e)),
                            time_taken=time_taken_for_scoring,
                            error_name=details['error_name'])
        process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.VENDOR_MASTER,is_success=False,
                                               date_list=[scoring_start_time],volume_list=[],
                                               time_taken_list=[time_taken_for_scoring,],
                                               description_list=[details['description']])
        return False

    except Exception as e:
        scoring_end_time = datetime.now(timezone.utc)
        time_taken_for_scoring = scoring_end_time-scoring_start_time
        capture_log_message(current_logger=g.error_logger,log_message='Error occurred during AP Scoring:{}'.format(str(e))
                           ,time_taken=time_taken_for_scoring, error_name=utils.GENERAL_SCORING_FAILED)

        process_data_for_sending_internal_mail(subject='Scoring Status',stage=utils.VENDOR_MASTER,is_success=False,
                                              date_list=[scoring_start_time],volume_list=[],
                                              time_taken_list=[time_taken_for_scoring,],
                                              description_list=['Vendor Master Scoring - Module Failed'],
                                              )
        return False