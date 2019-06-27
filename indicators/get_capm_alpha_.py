# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 17:15:07 2019

@author: BOPU
"""
import numpy as np
import pandas as pd
import requests
import datetime as dt
from sklearn.linear_model import LinearRegression
from get_industry import IndustryInfo

class get_capm_alpha(object):
    
    def __init__(self,code,benchmark,end_date,rolling_window):
        self.code = code
        self.benchmark = benchmark
        self.end_date = end_date
        self.rolling_window = rolling_window
        
    def get_history_data(self,code,end_date):    
        url = 'https://jybdata.iqdii.com/jybapp/price/stock/kline?code='+code+'&count=200&fq=1&end='+end_date
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
            month = date//100 - year*100
            day = date - year*10000 - month*100   
            return dt.date(year,month,day)        
        time = []
        for i in range(0,len(data)):               
            time.append(date_chg(data[i][0]))          
        table = pd.DataFrame(matrix , index = pd.to_datetime(time) , columns = cols)
        return [table,first_day]

    def get_full_data(self,code,end_date):
        Data=[]
        try:
            result = get_capm_alpha.get_history_data(self,code,end_date)
        except:
            print('Fetch data error =',self.code)
        new_end = str(result[1]-1)
        Data.append(result[0])
        times = 0
        while times <= 10:
            try:
                result = get_capm_alpha.get_history_data(self,code,new_end)
                Data.append(result[0])
                new_end = str(result[1]-1)    
                times += 1
            except:
                break
        data = pd.concat(Data,axis=0).sort_index()    
        return data
     
    def get_industry(self):
        industry = IndustryInfo().industry_name(self.code)
        industry_peers = IndustryInfo().industry_peers(self.code)
        return [industry , industry_peers]

    def get_factor_self(self):   
        stock_data = get_capm_alpha.get_full_data(self,self.code,self.end_date)
        benchmark_data = get_capm_alpha.get_full_data(self,self.benchmark,self.end_date)           
        table = pd.concat([stock_data.close.pct_change(),benchmark_data.close.pct_change()],axis=1).dropna()
        table.columns = [self.code,self.benchmark]    
        if len(table) >= self.rolling_window:
           matrix = np.zeros((len(table)-self.rolling_window+1,3))
           for i in range(0,len(matrix)):
               model = LinearRegression()  
               X = table.iloc[i:i+self.rolling_window,1].values 
               y = table.iloc[i:i+self.rolling_window,0].values
               result = model.fit(X.reshape(-1,1),y.reshape(-1,1))
               alpha = result.intercept_[0] * 250
               beta = result.coef_[0][0]
               vol = np.std((y.reshape(-1,1) - result.predict(X.reshape(-1,1)))) * np.sqrt(250)
               matrix[i,0] = alpha
               matrix[i,1] = beta
               matrix[i,2] = vol      
           temp = np.sort(matrix[-2500:,0])    
           n = np.where(temp == matrix[-1,0])    
           history_number = matrix[-1,0]         
           history_quantile = n[0][0]/len(temp) 
           return [history_number , history_quantile]               
        else:
           matrix = np.zeros((1,3)) 
           model = LinearRegression()  
           X = table.iloc[:,0].values 
           y = table.iloc[:,1].values
           result = model.fit(X.reshape(-1,1),y.reshape(-1,1))
           alpha = result.intercept_[0] * 250
           beta = result.coef_[0][0]
           vol = np.std((y.reshape(-1,1) - result.predict(X.reshape(-1,1)))) * np.sqrt(250)
           matrix[0,0] = alpha
           matrix[0,1] = beta
           matrix[0,2] = vol        
           history_number = matrix[-1,0]
           print('Price less than 1 year!')
           return [history_number , 1]    

    def get_factor_peer(self,Code):
        name = get_capm_alpha.get_industry(self)[1]
        print(get_capm_alpha.get_industry(self)[0])
        industry_vector = np.zeros(len(name))
        for i in range(0,len(name)): 
            self.code = name[i]
            print('reading industry peers =',self.code)
            data = get_capm_alpha.get_factor_self(self)       
            industry_vector[i] = data[0]
        industry_number = np.median(industry_vector)  
        self.code = Code
        history_data = get_capm_alpha.get_factor_self(self)
        temp = np.sort(industry_vector)
        n = np.where(temp == history_data[0])
        industry_quantile = n[0][0]/len(temp)          
        return [industry_number , industry_quantile , industry_vector]
        
        
if __name__ == '__main__':  
   data_self = get_capm_alpha('E00700','EHSI','20190404',250).get_factor_self() 
   print('history_number =',data_self[0])
   print('history_quantile =',data_self[1])
   data_peer = get_capm_alpha('E00700','EHSI','20190404',250).get_factor_peer('E00700')
   print('industry_number =',data_peer[0])
   print('industry_quantile =',data_peer[1])
   print('industry_vector =',data_peer[2])
    
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   