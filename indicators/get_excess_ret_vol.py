# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 16:13:51 2019

@author: bopu
"""

import requests
import numpy as np
import datetime as dt
import pandas as pd

class BaseComputation(object):
    computation_model_name: str = None
    stock_industry_map: dict = None
    industry_stocks_map: dict = None
    # region Stock Computation
    def fetch_stock_data(self, **kwargs):
        pass
    def pre_process_fetched_stock_data(self, **kwargs):
        pass
    def compute_factor(self, **kwargs):
        pass
    # endregion
    # region Peer Comparision Computation
    def fetch_industry_data(self, **kwargs):
        pass
    def compute_factor_in_peer(self, **kwargs):
        pass
    # endregion
class CompteExecessRetVol(BaseComputation):
    computation_model_name = '日超额收益波动率'
    _stock_data_length = 2400
    def __init__(self):
        self.fetch_industry_data()
    # region Stock Computation
    def fetch_stock_data(self, stock_code, end_date):
        raw_data = []
        current_count = 0
        while current_count < self._stock_data_length:
            if len(raw_data) == 0:
                this_end_date = end_date
            else:
                # reset date key point for next fetch
                this_end_date = raw_data[-1][0] - 1
            url = 'https://jybdata.iqdii.com/jybapp/price/stock/kline?code=' + \
                  stock_code + '&count=200&fq=1&end=' + str(this_end_date)
            response = requests.get(url).json()
            fetched_data: list = response['data']['day']
            if len(fetched_data) > 0:
                # for data concatenation
                current_count += len(fetched_data)
                fetched_data.sort(key=lambda entry: entry[0], reverse=True)
                raw_data.extend(fetched_data)
            else:
                # when no data can be fetch, need break
                break
        # organize data for output
        raw_data.sort(key=lambda entry: entry[0])

        if self._stock_data_length < len(raw_data):
            return raw_data[-self._stock_data_length::]
        else:
            return raw_data
    def pre_process_fetched_stock_data(self, raw_data):
        cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        matrix = np.zeros((len(raw_data), len(cols)))
        for i in range(0, matrix.shape[0]):
            for j in range(0, matrix.shape[1]):
                matrix[i, j] = raw_data[i][j + 1]
        def process_date_format(date):
            date = int(date)
            year = date // 10000
            month = date // 100 - year * 100
            day = date - year * 10000 - month * 100
            return dt.date(year, month, day)
        time = []
        for i in range(0, len(raw_data)):
            time.append(process_date_format(raw_data[i][0]))
        result = pd.DataFrame(matrix, index=pd.to_datetime(time), columns=cols)
        return result        
    def fetch_industry_data(self):
        data = pd.read_csv('ind_jieli.csv', encoding='utf-8')
        data['ind'] = data['GICS_ind'].apply(lambda item: item.split('_')[1])
        self.stock_industry_map = pd.Series(data['ind'].values, index=data['Code_jieli'].values).to_dict()
        self.industry_stocks_map = dict()
        for ind in set(data['ind']):
            x = data[data['ind'] == ind]['Code_jieli'].values
            self.industry_stocks_map[ind] = x                
    def compute_factor(self,stock_code, benchmark, end_date,rolling_window):
        self_ret = pd.DataFrame(self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))['close'].pct_change()).dropna()
        rf = pd.DataFrame(self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))['close'].pct_change()).dropna()
        temp = pd.concat([self_ret,rf],axis=1)
        temp = temp.dropna()
        temp.columns = ['close','rm']
        
        self_ret = temp['close'] - temp['rm']
        
        print(self_ret)
        
        self_ret = self_ret.rolling(min(rolling_window,len(self_ret))).std().dropna() * np.sqrt(245)
        history_number = self_ret.values[-1]
        temp_self = np.sort(self_ret.values)
        n = np.where(temp_self == history_number)
        history_quantile = 1 - n[0][0]/len(temp_self)
        return [history_number, history_quantile]        
    def compute_factor_in_peer(self, stock_code, benchmark, end_date,rolling_window):
        peers = self.industry_stocks_map.get(self.stock_industry_map.get(stock_code))
        industry_vector = np.zeros(len(peers))    
        for i in range(0,len(peers)):             
            current_code = peers[i]
            print('computing industry peers =', current_code)
            computed_result = self.compute_factor(current_code, benchmark, end_date, rolling_window)
            industry_vector[i] = computed_result[0]
        industry_vector = industry_vector[~np.isnan(industry_vector)]
        industry_number = np.median(industry_vector)
        history_data = self.compute_factor(stock_code,benchmark, end_date,rolling_window)
        temp = np.sort(industry_vector)
        n = np.where(temp == history_data[0])
        try:
            industry_quantile = 1 - n[0][0] / len(temp)
        except:
            industry_quantile = 1
        return [industry_number, industry_quantile, industry_vector]
        
        
if __name__ == '__main__':
   compute_ret_vol = CompteExecessRetVol()
   rst = compute_ret_vol.compute_factor('E00591', 'EHSI','20190403', 250)
   print(rst)
   #data_peer = compute_ret_vol.compute_factor_in_peer('E00700', 'EHSI','20190403', 250)
   #print('industry_number =', data_peer[0])
   #print('industry_quantile =', data_peer[1])
    
    
    
    
  
    
    
    
    
    
    
    
    
    
   
   
   
   
   
   
   
   
   
   
   
   
   
    