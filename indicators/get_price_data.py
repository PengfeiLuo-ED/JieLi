# -*- coding: utf-8 -*-
"""
Created on Thu Mar 07 09:51:36 2019

@author: BOPU
"""
import numpy as np
import pandas as pd
import requests
import datetime as dt
import os
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
sns.set()
os.chdir('F:\python files\jieli')
import pymssql
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm

def get_history_data(Code,End):    
    url = 'https://jybdata.iqdii.com/jybapp/price/stock/kline?code='+Code+'&count=200&fq=1&end='+End
    response = requests.get(url).json()
    data = response['data']['day']
    first_day = data[0][0]
    cols = ['open','high','low','close','volume','amount']
    matrix = np.zeros((len(data),len(cols)))
    for i in range(0,matrix.shape[0]):
        for j in range(0,matrix.shape[1]):
            matrix[i,j] = data[i][j+1]                
    def date_chg(date):
        date = int(date)
        year = date//10000
        month = date//100-year*100
        day = date - year*10000 - month*100   
        return dt.date(year,month,day)        
    time = []
    for i in range(0,len(data)):               
        time.append(date_chg(data[i][0]))          
    table = pd.DataFrame(matrix,index=pd.to_datetime(time),columns = cols)
    return [table,first_day]

#%%读取全部港股的名单
cursor = pymssql.connect(host='192.168.9.197',user='wanwei',password='wanwei',database='Wanvi_HK_F10Product',charset='utf8').cursor()
cursor.execute('SELECT Symbol from view_CompanySymbolRelation')
namelist = cursor.fetchall()
codelist = []
for i in range(0,len(namelist)):
    codelist.append('E'+namelist[i][0])
    
#%%根据名单读取每个港股的数据并保存
End = '20190312'
Dict = {}
for i in range(0,len(codelist)):
    print(i)
    Code = codelist[i]
    Data=[]
    try:
        result = get_history_data(Code,End)
    except:
        continue
    new_end = str(result[1]-1)
    Data.append(result[0])
    times=0
    while times < 10:
        try:
            result = get_history_data(Code,new_end)
            Data.append(result[0])
            new_end = str(result[1]-1)    
            times+=1
            #print(times)
        except:
            break
    data = pd.concat(Data,axis=0).sort_index() 
    Dict[Code] = data
    
#Dict['E00001'].close.plot()    

file = 'history_data.pkl'
with open(file,'wb') as f:
     pickle.dump(Dict,f)     
    
D_file = open('history_data.pkl','rb')
DD = pickle.load(D_file)

#%%计算动量指标

def get_full_data(code , end_date):
    Data=[]
    try:
        result = get_history_data(code , end_date)
    except:
        print('fetch_data_error =',code)
    new_end = str(result[1]-1)
    Data.append(result[0])
    times=0
    while times<=10:
        try:
            result = get_history_data(code,new_end)
            Data.append(result[0])
            new_end = str(result[1]-1)    
            times+=1
        except:
            break
    data = pd.concat(Data,axis=0).sort_index()    
    return data


def get_capm_factor(code='E00082' , benchmark='EHSI' , industry=0 , end_date='20190404' , rolling_window=250):
    stock_data = get_full_data(code , end_date)
    benchmark_data = get_full_data(benchmark , end_date)    
    table = pd.concat([stock_data.close,benchmark_data.close],axis=1).fillna(method='ffill')
    table = table.pct_change().dropna()
    table.columns = [code,benchmark]      
    matrix = np.zeros((len(table)-rolling_window+1,3))
    for i in range(0,len(table)-rolling_window+1):
        model = LinearRegression()  
        X = table.iloc[i:i+rolling_window,0].values 
        y = table.iloc[i:i+rolling_window,1].values
        #model = sm.OLS(table.iloc[i:i+rolling_window,0] , sm.add_constant(table.iloc[i:i+rolling_window,1]) )
        result = model.fit(X.reshape(-1,1),y.reshape(-1,1))
        alpha = result.intercept_[0] * 250
        beta = result.coef_[0][0]
        vol = np.std((y.reshape(-1,1) - result.predict(X.reshape(-1,1)))) * np.sqrt(250)
        matrix[i,0] = alpha
        matrix[i,1] = beta
        matrix[i,2] = vol     
    temp = np.sort(matrix[-2500:,0])    
    n = np.where(temp==matrix[-1,0])    
    history_number = matrix[-1,0]
    history_quantile = n[0][0]/len(temp)
    industry_number = 0
    industry_quantile = 0    
    return (history_number , history_quantile , industry_number , industry_quantile)












































































import pymssql

cursor = pymssql.connect('112.74.189.24', 'wiF10User', 'Fup@#!19$', 'Wanvi_HK_F10Product', port='21600').cursor()
cursor.execute('SELECT COUNT(*) from HK_STK_StockInfo;')
cursor.fetchone()

out: (2597, )






























