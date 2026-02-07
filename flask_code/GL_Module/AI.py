seed_value = 10
import random
import numpy as np
import tensorflow as tf
tf.random.set_seed(seed_value)
import os
import sys
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.callbacks import ModelCheckpoint, TensorBoard
from tensorflow.keras import regularizers
from tensorflow.keras import initializers
from tensorflow.keras import backend as K
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
import pickle
from GL_Module.Data_Preparation import Preparation
from GL_Module.db_connector import MySQL_DB
import json
# from GL_Module.logger import logger
session_conf = tf.compat.v1.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
sess = tf.compat.v1.Session(graph=tf.compat.v1.get_default_graph(), config=session_conf)
# tf.compat.v1.keras.backend.set_session(sess)
# from code1.logger import logger as main_logger
from code1.logger import capture_log_message
class AI_Framework:

    def __init__(self,model_name):
        """
        Initializing class variables for processing

        """
        self.model_name = model_name
        self.min_max_name = "GL_Module/"+self.model_name+"_MinMax.pkl" #name of the scalar to be accessed
        self.epochs = 50
        self.batch_size = 512

    def AutoEncoder(self,X_train):
        """
        AutoEncoder Architecture and Training

        """
        dimension = X_train.shape[1]
        capture_log_message(log_message='Input Dimenstion of Data to AI Model'+ str(dimension))

        input_layer = Input(shape=(dimension, ))
        encoder = Dense(dimension, activation="relu",activity_regularizer=regularizers.l1(1e-5),
                    kernel_initializer=initializers.he_uniform(seed=seed_value),bias_initializer=initializers.Constant(value=0.1))(input_layer)
        
        encoder = Dense(int(dimension / 2),activation="relu",kernel_initializer=initializers.he_uniform(seed=seed_value),
                    bias_initializer=initializers.Constant(value=0.1))(encoder)
        
        encoder = Dense(int(dimension / 4),activation="relu",kernel_initializer=initializers.he_uniform(seed=seed_value),
                    bias_initializer=initializers.Constant(value=0.1))(encoder)
        
        decoder = Dense(int(dimension / 2),activation='relu',kernel_initializer=initializers.he_uniform(seed=seed_value),
                    bias_initializer=initializers.Constant(value=0.1))(encoder)
        
        decoder = Dense(dimension,activation='relu',kernel_initializer=initializers.he_uniform(seed=seed_value),
                    bias_initializer=initializers.Constant(value=0.1))(decoder)

        model = Model(inputs=input_layer, outputs=decoder)
        model.compile(optimizer='adam',loss='mean_squared_error',metrics=['accuracy'])

        model_path = os.path.join('GL_Module/pickle_files',self.model_name +'.keras')
    
        checkpointer = ModelCheckpoint(filepath=model_path,verbose=0,save_best_only=True)
        tensorboard = TensorBoard(log_dir='logs',histogram_freq=0,write_graph=True,write_images=True)

        model.fit(X_train, X_train,epochs=self.epochs,batch_size=self.batch_size,shuffle=True,
                validation_split=0.05,verbose=1,callbacks=[checkpointer, tensorboard]).history

    def sync_columns(self,df):
        """
        Syncing columns to score model
        -----------------------------
        This function is used to sync the the columns of training and scoring 

        """
        #trained model columns
        model_col_list_file = os.path.join('GL_Module/csv_files', self.model_name+'_AI_col_list.csv')
        df_list = pd.read_csv(model_col_list_file)
        model_col_list = list(df_list['0'])
        extra_columns = list(set(model_col_list)-set(df.columns))

        #columns of data to be scored are equated with the columns of data which we used to train
        #filling the missing columns by zeros
        capture_log_message(log_message='Extra columns to be synced '+str(len(extra_columns)))
        # if len(extra_columns)>0:
        #     for column in extra_columns:
        #         df[column] = 0
        extra_data = pd.DataFrame(0, index=df.index, columns=extra_columns)
        df = pd.concat([df, extra_data], axis=1)
        df = df[model_col_list]
        
        capture_log_message(log_message='Shape of data after syncing columns '+str(df.shape))

        return df

    def AI_Training(self,data):
        """
        Training AutoEncoders
        ---------------------
        Input : Data to be trained
        Output : 

        """
        #Data Preparation
        Prep = Preparation()
        df = Prep.Data_Prep_for_AI(data)
        #Taking the current column names for reference while scoring 


        df_columns = pd.DataFrame(df.columns.to_list()) 
        df_col_list = os.path.join('GL_Module/csv_files', self.model_name+'_AI_col_list.csv')
        df_columns.to_csv(df_col_list, index=False)

        #MInmax scaling 
        Minmax = MinMaxScaler()
        X_train = Minmax.fit_transform(df)
        pickle.dump(Minmax, open(self.min_max_name, 'wb')) #Saving the scalar
        capture_log_message(log_message='Training started for AI Model')
        self.AutoEncoder(X_train)
        capture_log_message(log_message='Training completed for AI Model')

    def AI_Scoring(self,data):
        """
        Scoring using AutEncoders
        -------------------------
        Input : Data to be scored
        Output : Anomaly Scores for the transactions
        """

        #Data Preparation
        capture_log_message(log_message='Inside AI Scoring function, data shape:'+str(data.shape))
        Prep = Preparation()
        df = Prep.Data_Prep_for_AI(data)
        capture_log_message(log_message='Data Preparation for AI Completed')
        df = self.sync_columns(df) # Syncing column names used in the model and the prepared data
        capture_log_message(log_message='Shape of data after syncing columns '+str(df.shape))
        #Normalizing the data using training scalar
        Minmax = pickle.load(open(self.min_max_name, 'rb'))
        Minmax.clip = False
        X_test = Minmax.transform(df)

        #Loading model for scoring
        model_path = os.path.join('GL_Module/pickle_files',self.model_name+'.keras')
        model = load_model(model_path)
        capture_log_message(log_message='Model Loaded for Scoring {}'.format(self.model_name))

        #Scoring the data using model
        capture_log_message(log_message='Scoring for AI Model Started')
        capture_log_message(log_message='Shape of X_test data'+str(X_test.shape))

        X_pred = model.predict(X_test)
        X_test = X_test.reshape(X_test.shape[0], X_test.shape[1]) #Input neurons
        X_pred = X_pred.reshape(X_pred.shape[0], X_pred.shape[1]) #Output neurons

        #Setting the index for dataframe to store the scores
        scored = pd.DataFrame(index=df.index)
        scored['AI_RISK_SCORE_RAW'] = np.mean(np.abs(X_pred- X_test), axis=1) #calculating difference between predicted neurons and actual neurons
        min_val = scored['AI_RISK_SCORE_RAW'].min()
        max_val = scored['AI_RISK_SCORE_RAW'].max()
        scored['AI_RISK_SCORE'] = scored['AI_RISK_SCORE_RAW'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1)) #remapping the raw score between 0 and 1
        capture_log_message(log_message='Scoring for AI Model Completed')

        # scored.to_csv(os.path.join('csv_files',self.model_name+'_AI_Scored.csv'),index=False) #Storing the scores in csv file

        return scored


def main():

    DB = MySQL_DB('DB.json')
    connection = DB.connect_to_database()
    start_db_read = datetime.now(timezone.utc)
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
    model_name = "20220318_AI_Model"
    AI = AI_Framework(model_name)
   # AI.AI_Training(df)
    AI.AI_Scoring(df)

if __name__ == "__main__":
    main()
