# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 17:18:46 2019

@author: bopu
"""

import requests
import numpy as np
import datetime as dt
import pandas as pd
import pymssql

class MySQL(object):
      def __init__(self,host,user,pwd,db):
          self.host = host
          self.user = user
          self.pwd = pwd
          self.db = db
      def GetConnect(self):
          if not self.db:
             raise(NameError,"没有设置数据库信息")
          self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset='utf8')
          if not self.conn:
             raise(NameError,"连接数据库失败")
          else:
             return self.conn
      def ExeQuery(self,sql):
          conn = self.GetConnect()
          result = pd.read_sql(sql,con=conn) 
          conn.close()
          return result   
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
class ComputeTurnover(BaseComputation):
      computation_model_name = '平均换手率'
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
      def compute_factor(self,stock_code, end_date, rolling_window):
          con = MySQL(host='192.168.9.197',user='wanwei',pwd='wanwei',db='Wanvi_HK_F10Product')   
          result = con.ExeQuery('SELECT * from HK_STK_ChangeOfShares')   
          result['Symbol'] = result['Symbol'].apply(lambda x: 'E'+x)        
          self_vol = pd.DataFrame(self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))['volume'])
          self_vol['ModifyTime'] = self_vol.index.values
          self_vol['ModifyTime'] = self_vol['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          share_data = result[result['Symbol'] == stock_code][['IssuedCapital','ModifyTime']]
          share_data['ModifyTime'] = share_data['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          self_data = pd.merge(self_vol,share_data,on = 'ModifyTime',how = 'outer').fillna(method = 'ffill')
          self_data['result'] = (self_data['volume'] / self_data['IssuedCapital'])
          result = pd.DataFrame(self_data['result'])
          result = result.dropna(axis = 0)
          if len(result) >= (rolling_window + 1) * 21:    
             result = np.log(result.rolling(21).sum()).dropna()
             length = int(np.floor(len(result)/21))
             #result = pd.DataFrame(result,index = np.arange(len(result))).tail(length*21)
             result = np.array(result[-length*21:])
             #result = pd.DataFrame(pd.DataFrame(np.reshape(result,[length,21])).loc[:,20])
             result = np.reshape(result,[length,21])
             result = pd.DataFrame(np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[:,20].values
             history_number = result[-1]
             temp_self = np.sort(result)
             n = np.where(temp_self == history_number)
             history_quantile = 1 - n[0][0]/len(temp_self)        
          else:           
             try:
                result = np.log(result.sum()).dropna()
                length = int(np.floor(len(result)/21))
                #result = pd.DataFrame(result,index = np.arange(len(result))).tail(length*21)
                result = np.array(result[-length*21:])
                #result = pd.DataFrame(pd.DataFrame(np.reshape(result,[length,21])).loc[:,20])
                result = np.reshape(result,[length,21])
                result = pd.DataFrame(np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[:,20].values
                history_number = result[-1]            
                temp_self = np.sort(result)
                n = np.where(temp_self == history_number)
                history_quantile = 1 - n[0][0]/len(temp_self)         
             except:
                print('error with '+stock_code) 
                history_number = np.nan 
                history_quantile = 1             
          return [history_number, history_quantile]            
      def compute_factor_in_peer(self, stock_code, end_date, rolling_window):
          peers = self.industry_stocks_map.get(self.stock_industry_map.get(stock_code))
          industry_vector = np.zeros(len(peers))    
          for i in range(0,len(peers)):             
              current_code = peers[i]
              print('computing industry peers =', current_code)
              computed_result = self.compute_factor(current_code, end_date, rolling_window)
              industry_vector[i] = computed_result[0]
          industry_vector = industry_vector[~np.isnan(industry_vector)]
          industry_number = np.median(industry_vector)
          history_data = self.compute_factor(stock_code, end_date, rolling_window)
          temp = np.sort(industry_vector)
          n = np.where(temp == history_data[0])
          try:
             industry_quantile = 1 - n[0][0] / len(temp)
          except:
             industry_quantile = 1
          return [industry_number,industry_quantile]

if __name__ == '__main__':
   compute_turnover = ComputeTurnover()
   rst = compute_turnover.compute_factor('E02319', '20190418', 12)
   print(rst)
   #data_peer = compute_turnover.compute_factor_in_peer('E00700', '20190403', 3)
   #print(data_peer)
   #print('industry_number =', data_peer[0])
   #print('industry_quantile =', data_peer[1])
   
   
   
   
   
   
   
   
   
   
   
   
   