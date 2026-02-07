import numpy as np
import pandas as pd
import datetime
from functools import reduce

filename = "D:\Deep Learning\Techvantage\THINKRISK\AP Module\data_files\AP_GL_Final_2018.csv"
# filename = "AP_GL_Final_2018_small.csv"
df = pd.read_csv(filename,delimiter = "|" )

#getting the data points to inject anomalies
month_wise_dates = {"jan" : ['01/01/2018','01/31/2018'],
                   "feb" : ['02/01/2018','02/28/2018'],
                   "mar" : ['03/01/2018','03/31/2018'],
                   "apr" : ['04/01/2018','04/30/2018'],
                   "may" : ['05/01/2018','05/31/2018'],
                   "jun" : ['06/01/2018','06/30/2018'],
                   "jul" : ['07/01/2018','07/31/2018'],
                   "aug" : ['08/01/2018','08/31/2018'],
                   "sep" : ['09/01/2018','09/30/2018'],
                   "oct" : ['10/01/2018','10/31/2018'],
                   "nov" : ['11/01/2018','11/30/2018'],
                   "dec" : ['12/01/2018','12/31/2018']
                   }
dates = list(month_wise_dates.values())
df["POSTED_DATE"] = pd.to_datetime(df["POSTED_DATE"],format = "%Y-%m-%d")

import datetime
def time_filter(df,date,n_samples):
   
    date_1_start = pd.to_datetime(date[0])
    date_2_start = pd.to_datetime(date[1])
    df_month = df.loc[(df['POSTED_DATE'] > date_1_start) & (df['POSTED_DATE'] < date_2_start)]

#     date_2_end = pd.to_datetime(date[1])
#     date_1_end = date_2_end - datetime.timedelta(days=9)
#     df_month_end = df.loc[(df['POSTED_DATE'] > date_1_end) & (df['POSTED_DATE'] < date_2_end)]

    # df_month = pd.concat([df_month_start, df_month_end],axis=0)

    df_month_inv = df_month.iloc[np.where(df_month["ENTRY_TYPE"]=="INV")].sample(n=n_samples)
    df_month_pmnt = df_month.iloc[np.where(df_month["ENTRY_TYPE"]=="PMNT")].sample(n=n_samples)

    df_month_10 = pd.concat([df_month_inv, df_month_pmnt],axis=0)

    return df_month_10

month_new = []
for date in dates:
    month_new.append(time_filter(df,date,3000))
    

df_new = pd.concat(month_new, axis=0, ignore_index=True)

import os
import numpy as np
def trans_for_accountingDocs(df,account_num):
    df_account = df.loc[np.where(df["ACCOUNTING_DOC"] == account_num)]
    return df_account


list_trans = []
for account in df_new["ACCOUNTING_DOC"]:
    list_trans.append(trans_for_accountingDocs(df,account))


df_trans = pd.concat(list_trans, axis=0, ignore_index=True)