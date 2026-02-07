import pandas as pd
import numpy as np
from datetime import datetime, timezone
df=pd.read_csv("GL.csv")
df['TRANSACTION_ID_GA'] = df[['Financial Doc Id - PK','Line Item Id - PK']].apply(lambda row: "_".join(row.values.astype('str')),axis=1)
df.loc[df['Debit Credit Flag'] == 'S', ['Debit Credit Flag']] = 'D'
df.loc[df['Debit Credit Flag'] == 'H', ['Debit Credit Flag']] = 'C'
df['DEBIT_CREDIT_INDICATOR']=df['Debit Credit Flag']
df['Entry Date'] = df['Entry Date'].astype('str').apply(lambda x : datetime.strptime(x,'%Y%m%d').strftime("%Y-%m-%d"))
df['Posting Date'] = df['Posting Date'].astype('str').apply(lambda x : datetime.strptime(x,'%Y%m%d').strftime("%Y-%m-%d"))
df['Line Credit Amt LOC'] = df['Line Credit Amt LOC'].apply(lambda x : x.replace(',', ''))
df['Line Credit Amt LOC']=df['Line Credit Amt LOC'].astype(float)
df['Line Debit Amt LOC'] = df['Line Debit Amt LOC'].apply(lambda x : x.replace(',', ''))
df['Line Debit Amt LOC']=df['Line Debit Amt LOC'].astype(float)
df['Entry Time'] = df['Entry Time'].apply(lambda x : datetime.strptime(str(x),'%H%M%S').strftime("%H:%M:%S"))
df["ENTERED_DATE"]=df['Entry Date']+" "+df['Entry Time']
df=df[df['Company Code Id']==1000]
# df.to_csv("GL_Preprocessed.csv",index=False)