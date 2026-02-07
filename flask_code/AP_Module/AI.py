# import numpy as np
# import os
# from datetime import datetime, timezone
# import pandas as pd
# from tensorflow.keras.models import Model, load_model
# from tensorflow.keras.layers import Input, Dense
# from tensorflow.keras.callbacks import ModelCheckpoint, TensorBoard
# from tensorflow.keras import regularizers
# from tensorflow.keras import initializers
# from sklearn.preprocessing import MinMaxScaler
# import pickle
# from AP_Module.Data_Preparation import Preparation
# from AP_Module.db_connector import MySQL_DB
# from code1.logger import capture_log_message
# import tensorflow as tf
# seed_value = 10
# tf.random.set_seed(seed_value)



# # # from AP_Module.logger import logger
# # from code1.logger import logger as main_logger


# session_conf = tf.compat.v1.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
# sess = tf.compat.v1.Session(graph=tf.compat.v1.get_default_graph(), config=session_conf)
# # tf.compat.v1.keras.backend.set_session(sess)
# # tf.compat.v1.keras.backend to tf.compat.v1.
# class AI_Framework:

#     def __init__(self,model_name):
#         """
#         Initializing class variables for processing

#         """
#         self.model_name = model_name
#         self.min_max_name = "AP_Module/"+self.model_name+"_MinMax.pkl" #name of the scalar to be accessed
#         self.epochs = 100
#         self.batch_size = 512

#     def AutoEncoder(self,X_train):
#         """
#         AutoEncoder Architecture and Training

#         """
#         dimension = X_train.shape[1]

#         input_layer = Input(shape=(dimension, ))
#         encoder = Dense(dimension, activation="relu",activity_regularizer=regularizers.l1(10e-5),
#                     kernel_initializer=initializers.he_uniform(seed=seed_value),bias_initializer=initializers.Constant(value=0.1))(input_layer)
#         encoder = Dense(int(dimension / 2),activation="relu",kernel_initializer=initializers.he_uniform(seed=seed_value),
#                     bias_initializer=initializers.Constant(value=0.1))(encoder)
#         decoder = Dense(int(dimension / 2),activation='relu',kernel_initializer=initializers.he_uniform(seed=seed_value),
#                     bias_initializer=initializers.Constant(value=0.1))(encoder)
#         decoder = Dense(dimension,activation='relu',kernel_initializer=initializers.he_uniform(seed=seed_value),
#                     bias_initializer=initializers.Constant(value=0.1))(decoder)

#         model = Model(inputs=input_layer, outputs=decoder)
#         model.compile(optimizer='adam',loss='mean_squared_error',metrics=['accuracy'])

#         model_path = os.path.join('AP_Module/pickle_files',self.model_name +'.h5')
    
#         checkpointer = ModelCheckpoint(filepath=model_path,verbose=0,save_best_only=True)
#         tensorboard = TensorBoard(log_dir='logs',histogram_freq=0,write_graph=True,write_images=True)

#         model.fit(X_train, X_train,epochs=self.epochs,batch_size=self.batch_size,shuffle=True,
#                 validation_split=0.05,verbose=1,callbacks=[checkpointer, tensorboard]).history

#     def sync_columns(self,df):
#         """
#         Syncing columns to score model
#         -----------------------------
#         This function is used to sync the the columns of training and scoring 

#         """
#         #trained model columns
#         model_col_list_file = os.path.join('AP_Module/csv_files', self.model_name+'_AI_col_list.csv')
#         df_list = pd.read_csv(model_col_list_file)
#         model_col_list = list(df_list['0'])
#         extra_columns = list(set(model_col_list)-set(df.columns))

#         #columns of data to be scored are equated with the columns of data which we used to train
#         #filling the missing columns by zeros
#         capture_log_message(log_message='Extra columns to be synced {}'.format(len(extra_columns)))
#         if len(extra_columns)>0:
#             for column in extra_columns:
#                 df[column] = 0
#         df = df[model_col_list]
#         capture_log_message(log_message='Columns Synced')
#         return df

#     def AI_Training(self,data):
#         """
#         Training AutoEncoders
#         ---------------------
#         Input : Data to be trained
#         Output : 

#         """
#         #Data Preparation
#         Prep = Preparation()
#         df = Prep.Data_Prep_for_AI(data)
#         capture_log_message(log_message='Data Preparation of AI Completed')
#         #Taking the current column names for reference while scoring 
#         df_columns = pd.DataFrame(df.columns.to_list()) 
#         df_col_list = os.path.join('AP_Module/csv_files', self.model_name+'_AI_col_list.csv')
#         df_columns.to_csv(df_col_list, index=False)

#         #MInmax scaling 
#         Minmax = MinMaxScaler()
#         X_train = Minmax.fit_transform(df)
#         pickle.dump(Minmax, open(self.min_max_name, 'wb')) #Saving the scalar
#         capture_log_message(log_message='Training started')
#         self.AutoEncoder(X_train)
#         capture_log_message(log_message='Training completed')

#     def AI_Scoring(self,data):
#         """
#         Scoring using AutEncoders
#         -------------------------
#         Input : Data to be scored
#         Output : Anomaly Scores for the transactions
#         """
        
        
        

#         #Data Preparation
#         capture_log_message(log_message='Inside AI Scoring function, data shape:{}'.format(data.shape))
#         Prep = Preparation()
#         df = Prep.Data_Prep_for_AI(data)
#         capture_log_message(log_message='Shape of df:{}'.format(df.shape))
#         capture_log_message(log_message='Data Preparation of AI Completed')
#         df = self.sync_columns(df) # Syncing column names used in the model and the prepared data
#         #Normalizing the data using training scalar
#         Minmax = pickle.load(open(self.min_max_name, 'rb'))
#         Minmax.clip = False
#         X_test = Minmax.transform(df)

#         #Loading model for scoring
#         model_path = os.path.join('AP_Module/pickle_files',self.model_name+'.h5')
#         model = load_model(model_path)
#         capture_log_message(log_message='Model used for scoring:{}'.format(self.model_name))

#         #Scoring the data using model
#         capture_log_message(log_message='Scoring for AI Model Started')
#         X_pred = model.predict(X_test)
#         X_test = X_test.reshape(X_test.shape[0], X_test.shape[1]) #Input neurons
#         X_pred = X_pred.reshape(X_pred.shape[0], X_pred.shape[1]) #Output neurons

#         #Setting the index for dataframe to store the scores
#         scored = pd.DataFrame(index=df.index)
#         scored['AI_RISK_SCORE_RAW'] = np.mean(np.abs(X_pred- X_test), axis=1) #calculating difference between predicted neurons and actual neurons
#         min_val = scored['AI_RISK_SCORE_RAW'].min()
#         max_val = scored['AI_RISK_SCORE_RAW'].max()
#         scored['AI_RISK_SCORE'] = scored['AI_RISK_SCORE_RAW'].apply(lambda x: Prep.remap(x,min_val,max_val,0,1)) #remapping the raw score between 0 and 1
#         capture_log_message(log_message='Scoring for AI Model Completed')
#         return scored


# def main():

#     DB = MySQL_DB('DB.json')
#     connection = DB.connect_to_database()
#     df = pd.read_sql("""select tran.TRANSACTION_ID,tran.ENTERED_BY,doc.ENTRY_ID,tran.TRANSACTION_DESCRIPTION,tran.GL_ACCOUNT_DESCRIPTION,
# tran.DEBIT_AMOUNT,tran.CREDIT_AMOUNT,tran.PAYMENT_DATE,
#     tran.ACCOUNT_TYPE,tran.POSTED_BY,tran.POSTING_DATE,tran.ENTRY_DATE,tran.SYSTEM_UPDATED_DATE,tran.DUE_DATE,tran.PAYMENT_TERMS,  tran.INVOICE_NUMBER,tran.SUPPLIER_ID,tran.INVOICE_AMOUNT,tran.INVOICE_DATE,tran.CREDIT_PERIOD,tran.TRANSACTION_CODE,comp.COMPANY_CODE,vend.VENDORCODE,loc.LOCATION_CODE,entry.ENTRY_TYPE as ENTRY_TYPE
#     from ap_transaction tran
#     left join msentrytype entry on tran.ENTRY_TYPE_ID=entry.ENTRY_TYPE_ID
#     left join mscompany comp  on tran.COMPANY_ID=comp.COMPANYID
#     left join ap_vendorlist vend  on tran.VENDORID=vend.VENDORID
#     left join mslocation loc on tran.LOCATION_ID=loc.LOCATIONID
#     left join ap_accountdocuments doc on tran.ACCOUNT_DOC_ID=doc.ACCOUNT_DOC_ID;""",con=connection)  
#     configurations = pd.read_sql("SELECT KEYNAME,KEYVALUE from trconfiguration where module='framework' and STATUS=1",con=connection)
#     configs = dict(zip(configurations.KEYNAME,configurations.KEYVALUE))
#     model_name = "20220706_AI_Model"
#     AI = AI_Framework(model_name)
#     # AI.AI_Training(df)
#     AI.AI_Scoring(df)

# if __name__ == "__main__":
#     main()
