seed_value = 10
# from asyncio.log import logger
import random
random.seed(seed_value)

import numpy as np
np.random.seed(seed_value)
import tensorflow as tf

import os
import sys
import pickle
from datetime import datetime, timezone
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler,StandardScaler
from GL_Module.db_connector import MySQL_DB
from GL_Module.Data_Preparation import Preparation
# from GL_Module.logger import logger
import json
from code1.logger import capture_log_message


class Stat_Framework:

    def __init__(self,model_name):
        """
        Initializing the model vavriables
        """
        self.model_name = model_name
        self.min_max_name = "GL_Module/"+self.model_name+"_MinMax.pkl"
    
    def remap(self,x, oMin, oMax, nMin, nMax ):
        """
        Function to remap the values between 0 and 1
        """
        #range check
        if oMin == oMax:
            capture_log_message(log_message="Warning: Zero input range")
            return None

        if nMin == nMax:
            capture_log_message(log_message="Warning: Zero output range")
            return None

        #check reversed input range
        reverseInput = False
        oldMin = min( oMin, oMax )
        oldMax = max( oMin, oMax )
        if not oldMin == oMin:
            reverseInput = True

        #check reversed output range
        reverseOutput = False   
        newMin = min( nMin, nMax )
        newMax = max( nMin, nMax )
        if not newMin == nMin :
            reverseOutput = True

        portion = (x-oldMin)*(newMax-newMin)/(oldMax-oldMin)
        if reverseInput:
            portion = (oldMax-x)*(newMax-newMin)/(oldMax-oldMin)

        result = portion + newMin
        if reverseOutput:
            result = newMax - portion

        return result

    def IF_Training(self,data):
        """
        Training the Isolation Forest Model
        """

        X_train = pd.DataFrame(data)
        capture_log_message(log_message="Training Stat Model 1 Started")
        # IF_Model_1 = IsolationForest(n_estimators=250,max_samples= 1.0,bootstrap=True, max_features=1.0,random_state=42)
        IF_Model_1 = IsolationForest(n_estimators=50,max_samples= 0.5,bootstrap=True, max_features=1.0,random_state=42)

        IF_Model_1.fit(X_train)
        capture_log_message(log_message="Training Stat Model 1 Finished")

        filename = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_1.sav')
        pickle.dump(IF_Model_1, open(filename, 'wb'))

        # capture_log_message(log_message="Training Stat Model 2 Started")
        # IF_Model_2 = IsolationForest(n_estimators=200,max_samples = 1.0,bootstrap=True, max_features=1.0,random_state=42)
        # IF_Model_2.fit(X_train)
        # capture_log_message(log_message="Training Stat Model 2 Finished")

        # filename = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_2.sav')
        # pickle.dump(IF_Model_2, open(filename, 'wb'))
        # capture_log_message(log_message="Training Stat Model 3 Started")
        # IF_Model_3 = IsolationForest(n_estimators=150,max_samples= 1.0,bootstrap=True, max_features=1.0,random_state=42)
        # IF_Model_3.fit(X_train)
        # capture_log_message(log_message="Training Stat Model 3 Finished")

        # filename = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_3.sav')
        # pickle.dump(IF_Model_3, open(filename, 'wb'))


    def reindex(self,df,columnname):
        """
        Function to reindex the values of IForest output

        """
        df[columnname] = 1 - df[columnname] # Since the anomalies are negative values , we are reversing it for better calculation

        min_anom = df[df[columnname]>1][columnname].min()
        max_anom = df[df[columnname]>1][columnname].max()
        min_non_anom = df[df[columnname]<1][columnname].min()
        max_non_anom = df[df[columnname]<1][columnname].max()
        #remapping the scores which came below 0 (anomalies) between 0.7 and 1 and the rest between 0
        df[columnname+"_INDEXED"]=df[columnname].apply(lambda x: self.remap(x,min_anom,max_anom,0.7,1) if x>1 else self.remap(x,min_non_anom,max_non_anom,0,0.69)) 

        return df

    def IF_Scoring(self,data):
        """
        Scoring for Isolation Forest Model
        """
        IF_Model_path = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_1.sav')
        capture_log_message(f"IF model path:{IF_Model_path}")
        IF_Model_1 = pickle.load(open(IF_Model_path, 'rb'))
        capture_log_message(log_message="Scoring Stat Model 1 Started")
        IF_Scores_1 = IF_Model_1.decision_function(data)
        capture_log_message(log_message="Scoring Stat Model 1 Finished")

        # IF_Model_path = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_2.sav')
        # IF_Model_2 = pickle.load(open(IF_Model_path, 'rb'))
        
        # capture_log_message(log_message="Scoring Stat Model 2 Started")
        # IF_Scores_2 = IF_Model_2.decision_function(data)
        # capture_log_message(log_message="Scoring Stat Model 2 Finished")

        # IF_Model_path = os.path.join('GL_Module/pickle_files', 'IF_'+self.model_name+'_3.sav')
        # IF_Model_3 = pickle.load(open(IF_Model_path, 'rb'))
            
        # capture_log_message(log_message="Scoring Stat Model 3 Started")        
        # IF_Scores_3 = IF_Model_3.decision_function(data)
        # capture_log_message(log_message="Scoring Stat Model 3 Finished")

        # IF_Scores = pd.DataFrame({'IF_1': IF_Scores_1,'IF_2': IF_Scores_2,'IF_3': IF_Scores_3},index=data.index)
        
        IF_Scores = pd.DataFrame({'IF_1': IF_Scores_1,},index=data.index)

        # for column in IF_Scores.columns:
        #     IF_Scores = self.reindex(IF_Scores,column)

        # IF_Scores['STAT_SCORE'] = IF_Scores['IF_1'] + IF_Scores['IF_2'] + IF_Scores['IF_3']
        IF_Scores['STAT_SCORE'] = IF_Scores['IF_1'] 

        IF_Scores = self.reindex(IF_Scores,'STAT_SCORE')

        IF_Scores = IF_Scores[['STAT_SCORE','STAT_SCORE_INDEXED']]
        
        #IF_Scores.to_csv("IF_SCORES.csv")

        return IF_Scores

    def sync_columns(self,df):
        """
        Syncing columns to score model
        -----------------------------
        This function is used to sync the the columns of training and scoring 
        """

        ##trained model columns
        model_col_list_file = os.path.join('GL_Module/csv_files', self.model_name+'_Stat_col_list.csv')
        df_list = pd.read_csv(model_col_list_file)
        model_col_list = list(df_list['0'])
        extra_columns = list(set(model_col_list)-set(df.columns))

        #columns of data to be scored are equated with the columns of data which we used to train
        #filling the missing columns by zeros
        # if len(extra_columns)>0:
        #     for column in extra_columns:
        #         df[column] = 0
        extra_data = pd.DataFrame(0, index=df.index, columns=extra_columns)
        df = pd.concat([df, extra_data], axis=1)
        
        df = df[model_col_list]
        capture_log_message(log_message="Column Syncing Completed")

        return df


    def Stat_Scoring(self,data):
        """
        Function to faciliate overall scoring for IForest models 
        -------------------------------------------------------
        Input : Data to be scored
        Output: Anomaly scores
        """

        #Data Prepartion
        Prep = Preparation()
        df = Prep.Data_Prep_for_Stat(data)
        capture_log_message(log_message="Data Preparation of Stat Completed")
        df = self.sync_columns(df) ## Syncing column names used in the model and the prepared data
        #Normalizing the data using training scalar
        Minmax = pickle.load(open(self.min_max_name, 'rb'))
        Minmax.clip=False
        X_test = pd.DataFrame(Minmax.transform(df))

        capture_log_message(log_message="Scoring for Stat Model Started")

        scores = self.IF_Scoring(X_test)
        capture_log_message(log_message="Scoring for Stat Model Finished")
        # scores.to_csv(os.path.join('csv_files', self.model_name+'_Stat_Scores.csv'))
        return scores


    def Stat_Training(self,data):
        """
        Training IForest models
        -----------------------
        Input : training data 

        """
        #data preparation
        Prep = Preparation()
        df = Prep.Data_Prep_for_Stat(data)
         #Taking the current column names for reference while scoring 
        df_columns = pd.DataFrame(df.columns.to_list())
        df_col_list = os.path.join('GL_Module/csv_files', self.model_name+'_Stat_col_list.csv')
        df_columns.to_csv(df_col_list, index=False)

        #minmax scaling
        Minmax = MinMaxScaler()
        X_train = Minmax.fit_transform(df)
        pickle.dump(Minmax, open(self.min_max_name, 'wb'))
        self.IF_Training(X_train)


def main():
    
    DB = MySQL_DB('DB.json')
    connection = DB.connect_to_database()
    start_db_read = datetime.now(timezone.utc)
    capture_log_message(log_message="Started Reading Data")
    df = pd.read_sql("""SELECT tran.TRANSACTIONID,tran.TRANSACTION_DESC,tran.ACCOUNTDOCID,tran.ENTERED_DATE,tran.ENTERED_BY as ENTERED_BY_USERID,
    tran.POSTED_DATE,tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,
    doc.DOCUMENT_TYPE_CODE as DOC_TYPE,com.COMPANY_CODE,
    acctdoc.ACCOUNTDOC_CODE,
    acc.ACCOUNT_CODE,acc.ACCOUNT_DESCRIPTION,
    posted.FMSUSERID as POSTED_BY_USERID,posted.FMSUSER_CODE as POSTED_BY,
    entered.FMSUSER_CODE as ENTERED_BY
    from rpttransaction tran
    left join msfmsuser entered on tran.ENTERED_BY = entered.FMSUSERID
    left join msdocumenttype doc on tran.DOCTYPEID=doc.DOCUMENTID
    left join rptaccountdocument acctdoc on tran.ACCOUNTDOCID=acctdoc.ACCOUNTDOCID
    left join mschartofaccounts acc on tran.ACCOUNTID=acc.ACCOUNTID
    left join mscompany com on tran.COMPANYID=com.COMPANYID
    left join msfmsuser posted on acctdoc.POSTED_BY=posted.FMSUSERID;""",con=connection)
    configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='framework' and STATUS=1",con=connection)
    configs = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
    model_name = "20220711_Stat_Model"
    # model_name = datetime.today().strftime('%Y%m%d')+"_Stat_Model"
    Stat_Model = Stat_Framework(model_name)
   # Stat_Model.Stat_Training(df)
    Stat_Model.Stat_Scoring(df)


if __name__ =="__main__":
    main()
