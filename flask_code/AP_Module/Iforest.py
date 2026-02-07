# seed_value = 10
# import random
# random.seed(seed_value)
# # from asyncio.log import logger
# import numpy as np
# np.random.seed(seed_value)
# import tensorflow as tf

# import os
# import sys
# import pickle
# from datetime import datetime, timezone
# import pandas as pd
# from sklearn.ensemble import IsolationForest
# from AP_Module.db_connector import MySQL_DB
# from sklearn.preprocessing import MinMaxScaler,StandardScaler
# from AP_Module.Data_Preparation import Preparation
# from code1.logger import capture_log_message
# import utils

# class Stat_Framework:

#     def __init__(self,model_name):

#         self.model_name = model_name
#         self.min_max_name = "AP_Module/"+self.model_name+"_MinMax.pkl"

#     def remap(self,x, oMin, oMax, nMin, nMax ):

#         #range check
#         if oMin == oMax:
#             capture_log_message(log_message="Warning: Zero input range")
#             return None

#         if nMin == nMax:
#             capture_log_message(log_message="Warning: Zero output range")
#             return None

#         #check reversed input range
#         reverseInput = False
#         oldMin = min( oMin, oMax )
#         oldMax = max( oMin, oMax )
#         if not oldMin == oMin:
#             reverseInput = True

#         #check reversed output range
#         reverseOutput = False   
#         newMin = min( nMin, nMax )
#         newMax = max( nMin, nMax )
#         if not newMin == nMin :
#             reverseOutput = True

#         portion = (x-oldMin)*(newMax-newMin)/(oldMax-oldMin)
#         if reverseInput:
#             portion = (oldMax-x)*(newMax-newMin)/(oldMax-oldMin)

#         result = portion + newMin
#         if reverseOutput:
#             result = newMax - portion

#         return result

#     def IF_Training(self,data):
#         """
#         Training the Isolation Forest Model
#         """

#         X_train = pd.DataFrame(data)
#         capture_log_message(log_message='Training Stat Model 1 Started')
#         IF_Model_1 = IsolationForest(n_estimators=300,max_samples=0.6,bootstrap=True, max_features=1.0,random_state=42)
#         IF_Model_1.fit(X_train)
#         capture_log_message(log_message='Training Stat Model 1 Completed')

#         filename = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_1.sav')
#         pickle.dump(IF_Model_1, open(filename, 'wb'))

#         capture_log_message(log_message='Training Stat Model 2 Started')
#         IF_Model_2 = IsolationForest(n_estimators=200,max_samples =0.7,bootstrap=True, max_features=1.0,random_state=42)
#         IF_Model_2.fit(X_train)
#         capture_log_message(log_message='Training Stat Model 2 Completed')

#         filename = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_2.sav')
#         pickle.dump(IF_Model_2, open(filename, 'wb'))
	    
#         capture_log_message(log_message='Training Stat Model 3 Started')
#         IF_Model_3 = IsolationForest(n_estimators=150,max_samples=0.8,bootstrap=True, max_features=1.0,random_state=42)
#         IF_Model_3.fit(X_train)
#         capture_log_message(log_message='Training Stat Model 3 Completed')

#         filename = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_3.sav')
#         pickle.dump(IF_Model_3, open(filename, 'wb'))


#     def reindex(self,df,columnname):

#         df[columnname] = 1 - df[columnname]

#         min_anom = df[df[columnname]>1][columnname].min()
#         max_anom = df[df[columnname]>1][columnname].max()
#         min_non_anom = df[df[columnname]<1][columnname].min()
#         max_non_anom = df[df[columnname]<1][columnname].max()
#         df[columnname+"_INDEXED"]=df[columnname].apply(lambda x: self.remap(x,min_anom,max_anom,0.7,1) if x>1 else self.remap(x,min_non_anom,max_non_anom,0,0.69))

#         return df

#     def reindex_previous(self,df,columnname):

#         df[columnname+"_NORM"] = 1 - df[columnname]

#         min_anom = df[df[columnname+"_NORM"]>1][columnname+"_NORM"].min()
#         max_anom = df[df[columnname+"_NORM"]>1][columnname+"_NORM"].max()
#         min_non_anom = df[df[columnname+"_NORM"]<1][columnname+"_NORM"].min()
#         max_non_anom = df[df[columnname+"_NORM"]<1][columnname+"_NORM"].max()
#         df[columnname+"_INDEXED"]=df[columnname+"_NORM"].apply(lambda x: self.remap(x,min_anom,max_anom,0.7,1) if x>1 else self.remap(x,min_non_anom,max_non_anom,0,0.69))

#         return df

#     def IF_Scoring(self,data):
#         """
#         Scoring for Isolation Forest Model
#         """
#         IF_Model_path = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_1.sav')
#         IF_Model_1 = pickle.load(open(IF_Model_path, 'rb'))
#         capture_log_message(log_message='Scoring Stat Model 1 Started')
#         IF_Scores_1 = IF_Model_1.decision_function(data)
#         capture_log_message(log_message='Scoring Stat Model 1 Completed')
        
#         IF_Model_path = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_2.sav')
#         IF_Model_2 = pickle.load(open(IF_Model_path, 'rb'))
#         capture_log_message(log_message='Scoring Stat Model 2 Started')
#         IF_Scores_2 = IF_Model_2.decision_function(data)
#         capture_log_message(log_message='Scoring Stat Model 2 Completed')
        
#         IF_Model_path = os.path.join('AP_Module/pickle_files', 'IF_'+self.model_name+'_3.sav')
#         IF_Model_3 = pickle.load(open(IF_Model_path, 'rb'))
#         capture_log_message(log_message='Scoring Stat Model 3 Started')
#         IF_Scores_3 = IF_Model_3.decision_function(data)
#         capture_log_message(log_message='Scoring Stat Model 3 Completed')
#         IF_Scores = pd.DataFrame({'IF_1': IF_Scores_1,'IF_2': IF_Scores_2,'IF_3': IF_Scores_3},index=data.index)

#         # for column in IF_Scores.columns:
#         #     IF_Scores = self.reindex(IF_Scores,column)

#         IF_Scores['STAT_SCORE'] = IF_Scores['IF_1'] + IF_Scores['IF_2'] + IF_Scores['IF_3']

#         IF_Scores = self.reindex(IF_Scores,'STAT_SCORE')

#         # IF_Scores = IF_Scores.rename(columns={'STAT_SCORE':'STAT_SCORE_RAW'})

#         IF_Scores = IF_Scores[['STAT_SCORE','STAT_SCORE_INDEXED']]
        
#         return IF_Scores

#     def sync_columns(self,df):
#         """
#         Syncing columns to score model
#         """
#         model_col_list_file = os.path.join('AP_Module/csv_files', self.model_name+'_Stat_col_list.csv')
#         df_list = pd.read_csv(model_col_list_file)
#         model_col_list = list(df_list['0'])
#         extra_columns = list(set(model_col_list)-set(df.columns))
#         capture_log_message(log_message='Extra columns to be synced {}'.format(str(extra_columns)))
#         if len(extra_columns)>0:
#             for column in extra_columns:
#                 df[column] = 0
#         df = df[model_col_list]
#         capture_log_message(log_message='Columns Synced')

#         return df


#     def Stat_Scoring(self,data):

#         Prep = Preparation()
#         df = Prep.Data_Prep_for_Stat(data)
#         capture_log_message(log_message='Data Preparation of Stat Completed')
#         df = self.sync_columns(df)
#         Minmax = pickle.load(open(self.min_max_name, 'rb'))
#         Minmax.clip=False
#         X_test = pd.DataFrame(Minmax.transform(df))
        
#         capture_log_message(log_message='Scoring for Stat Model Started')

#         scores = self.IF_Scoring(X_test)
#         capture_log_message(log_message='Scoring for Stat Model Completed')
#         return scores


#     def Stat_Training(self,data):

#         Prep = Preparation()
#         df = Prep.Data_Prep_for_Stat(data)
#         df_columns = pd.DataFrame(df.columns.to_list())
#         df_col_list = os.path.join('AP_Module/csv_files', self.model_name+'_Stat_col_list.csv')
#         df_columns.to_csv(df_col_list, index=False)

#         Minmax = MinMaxScaler()
#         X_train = Minmax.fit_transform(df)
#         pickle.dump(Minmax, open(self.min_max_name, 'wb'))

#         self.IF_Training(X_train)


# def main():
    
#     DB = MySQL_DB('DB.json')
#     connection = DB.connect_to_database()
#     start_db_read = datetime.now(timezone.utc)
#     df= pd.read_sql("""select tran.TRANSACTION_ID,tran.ENTERED_BY,doc.ENTRY_ID,tran.TRANSACTION_DESCRIPTION,tran.GL_ACCOUNT_DESCRIPTION,
#     tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.PAYMENT_DATE,
#     tran.ACCOUNT_TYPE,tran.POSTED_BY,tran.POSTING_DATE,tran.ENTRY_DATE,tran.SYSTEM_UPDATED_DATE,tran.DUE_DATE,tran.PAYMENT_TERMS,  tran.INVOICE_NUMBER,tran.SUPPLIER_ID,tran.INVOICE_AMOUNT,tran.INVOICE_DATE,tran.CREDIT_PERIOD,tran.TRANSACTION_CODE,comp.COMPANY_CODE,vend.VENDORCODE,loc.LOCATION_CODE,entry.ENTRY_TYPE as ENTRY_TYPE
#     from ap_transaction tran
#     left join msentrytype entry on tran.ENTRY_TYPE_ID=entry.ENTRY_TYPE_ID
#     left join mscompany comp  on tran.COMPANY_ID=comp.COMPANYID
#     left join ap_vendorlist vend  on tran.VENDORID=vend.VENDORID
#     left join mslocation loc on tran.LOCATION_ID=loc.LOCATIONID
#     left join ap_accountdocuments doc on tran.ACCOUNT_DOC_ID=doc.ACCOUNT_DOC_ID;""",con=connection)
#     configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='apframework' and STATUS=1",con=connection)
#     finish_db_read = datetime.now(timezone.utc)
#     capture_log_message(log_message='Time Taken for Reading {shape} Dimensioned Dataframe {time}'.format(shape=df.shape,time=finish_db_read-start_db_read))
#     connection.close()
    
#     model_name = datetime.today().strftime('%Y%m%d')+"_Stat_Model"
#     Stat_Model = Stat_Framework(model_name)
#     # Stat_Model.Stat_Training(df)
#     Stat_Model.Stat_Scoring(df)

# if __name__ =="__main__":
#     main()
