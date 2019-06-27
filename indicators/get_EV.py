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
class ComputeEV_EBITDA(BaseComputation):
      computation_model_name = '企业价值倍数'
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
      def compute_factor(self,stock_code, end_date):
          con = MySQL(host='192.168.9.197',user='wanwei',pwd='wanwei',db='Wanvi_HK_F10Product')          
          result1 = con.ExeQuery('SELECT * from HK_STK_FIN_Income')  
          result2 = con.ExeQuery('SELECT * from HK_STK_FIN_BankOfIncome') 
          result1['Symbol'] = result1['Symbol'].apply(lambda x: 'E'+x)
          result2['Symbol'] = result2['Symbol'].apply(lambda x: 'E'+x)
          result1 = result1[['ReportType','Symbol','Currency','ProfitBeforeTaxation','ModifyTime']]
          result2 = result2[['ReportType','Symbol','Currency','ProfitBeforeTaxation','ModifyTime']]
          result1 = result1[result1['Symbol'] == stock_code][['ReportType','Currency','ProfitBeforeTaxation','ModifyTime']]
          result2 = result2[result2['Symbol'] == stock_code][['ReportType','Currency','ProfitBeforeTaxation','ModifyTime']]
          result1 = pd.concat([result1,result2],axis = 0)
          result1.loc[result1.ReportType == 2,'ProfitBeforeTaxation'] = result1.loc[result1.ReportType == 2,'ProfitBeforeTaxation'] *2
          result1['ProfitBeforeTaxation'] = result1['ProfitBeforeTaxation'] * 1000
          result1.loc[result1.Currency == 'CNY','ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'CNY','ProfitBeforeTaxation'] / 0.85        
          result1.loc[result1.Currency == 'JPY','ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'JPY','ProfitBeforeTaxation'] / 5.99
          result1.loc[result1.Currency == 'MYR','ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'MYR','ProfitBeforeTaxation'] / 1.62
          result1.loc[result1.Currency == 'SGD','ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'SGD','ProfitBeforeTaxation'] * 4.95
          result1.loc[result1.Currency == 'USD','ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'USD','ProfitBeforeTaxation'] * 7.6
          result1['ModifyTime'] = result1['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          result2 = con.ExeQuery('SELECT * from HK_STK_FIN_CashFlow')   
          result2['Symbol'] = result2['Symbol'].apply(lambda x: 'E'+x)
          result2 = result2[['ReportType','Symbol','Currency','NCFEY','ModifyTime']]
          result2 = result2[result2['Symbol'] == stock_code][['ReportType','Currency','NCFEY','ModifyTime']]
          result2.loc[result2.ReportType == 2,'NCFEY'] = result2.loc[result2.ReportType == 2,'NCFEY'] * 2
          result2['NCFEY'] = result2['NCFEY'] * 1000
          result2.loc[result2.Currency == 'CNY','NCFEY'] = result2.loc[result2.Currency == 'CNY','NCFEY'] / 0.85        
          result2.loc[result2.Currency == 'JPY','NCFEY'] = result2.loc[result2.Currency == 'JPY','NCFEY'] / 5.99
          result2.loc[result2.Currency == 'MYR','NCFEY'] = result2.loc[result2.Currency == 'MYR','NCFEY'] / 1.62
          result2.loc[result2.Currency == 'SGD','NCFEY'] = result2.loc[result2.Currency == 'SGD','NCFEY'] * 4.95
          result2.loc[result2.Currency == 'USD','NCFEY'] = result2.loc[result2.Currency == 'USD','NCFEY'] * 7.6
          result2['ModifyTime'] = result2['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          self_data = pd.merge(result1,result2,on = 'ModifyTime',how = 'outer').fillna(method = 'ffill')
          result3 = con.ExeQuery('SELECT * from HK_STK_ChangeOfShares')   
          result3['Symbol'] = result3['Symbol'].apply(lambda x: 'E'+x)
          result3 = result3[result3['Symbol'] == stock_code][['IssuedCapital','ModifyTime']]
          result3['ModifyTime'] = result3['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          self_price = pd.DataFrame(self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))['close'])
          self_price['ModifyTime'] = self_price.index.values
          self_price['ModifyTime'] = self_price['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          cap_data = pd.merge(self_price,result3,on = 'ModifyTime',how = 'outer').fillna(method = 'ffill')
          self_data = pd.merge(self_data,cap_data,on = 'ModifyTime',how = 'outer').fillna(method = 'ffill')
          result1 = con.ExeQuery('SELECT * from HK_STK_FIN_Balance')  
          result2 = con.ExeQuery('SELECT * from HK_STK_FIN_BankOfBalance')          
          result1['Symbol'] = result1['Symbol'].apply(lambda x: 'E'+x)
          result2['Symbol'] = result2['Symbol'].apply(lambda x: 'E'+x)
          result1 = result1[['Symbol','ReportType','Currency','TotalLiabilities','ModifyTime']]
          result2 = result2[['Symbol','ReportType','Currency','TotalLiabilities','ModifyTime']]
          result = pd.concat([result1,result2],axis = 0)
          result = result[result['Symbol'] == stock_code][['ReportType','Currency','TotalLiabilities','ModifyTime']]
          result['ModifyTime'] = result['ModifyTime'].apply(lambda x: int(str(x)[:4] +str(x)[5:7] + str(x)[8:10]))
          result.loc[result.ReportType == 2,'TotalLiabilities'] = result.loc[result.ReportType == 2,'TotalLiabilities'] * 2
          result['TotalLiabilities'] = result['TotalLiabilities'] * 1000
          result.loc[result.Currency == 'CNY','TotalLiabilities'] = result.loc[result.Currency == 'CNY','TotalLiabilities'] / 0.85        
          result.loc[result.Currency == 'JPY','TotalLiabilities'] = result.loc[result.Currency == 'JPY','TotalLiabilities'] / 5.99
          result.loc[result.Currency == 'MYR','TotalLiabilities'] = result.loc[result.Currency == 'MYR','TotalLiabilities'] / 1.62
          result.loc[result.Currency == 'SGD','TotalLiabilities'] = result.loc[result.Currency == 'SGD','TotalLiabilities'] * 4.95
          result.loc[result.Currency == 'USD','TotalLiabilities'] = result.loc[result.Currency == 'USD','TotalLiabilities'] * 7.6
          self_data = pd.merge(self_data,result,on = 'ModifyTime',how = 'outer').fillna(method = 'ffill')
          self_data['result'] = ((self_data['close'] * self_data['IssuedCapital'] + self_data['TotalLiabilities'] - self_data['NCFEY']) / self_data['ProfitBeforeTaxation'])
          self_data = self_data.sort_values('ModifyTime')
          result = pd.DataFrame(self_data['result'])        
          result = result.dropna(axis = 0)
          try:
              history_number = result.values[-1][0]
              result = result.sort_values(['result'],ascending = True)
              n = np.where(result.values == history_number)
              history_quantile = 1 - n[0][0]/len(result)
          except:
              history_number = np.nan
              history_quantile = 1
          return [history_number, history_quantile]            
      def compute_factor_in_peer(self, stock_code, end_date):
          peers = self.industry_stocks_map.get(self.stock_industry_map.get(stock_code))
          industry_vector = np.zeros(len(peers))    
          for i in range(0,len(peers)):             
              current_code = peers[i]
              print('computing industry peers =', current_code)
              computed_result = self.compute_factor(current_code, end_date)
              industry_vector[i] = computed_result[0]
          industry_vector = industry_vector[~np.isnan(industry_vector)]
          industry_number = np.median(industry_vector)
          history_data = self.compute_factor(stock_code, end_date)
          temp = np.sort(industry_vector)
          n = np.where(temp == history_data[0])
          try:
              industry_quantile = 1 - n[0][0] / len(temp)
          except:
              industry_quantile = 1
          return [industry_number, industry_quantile]

if __name__ == '__main__':
   compute_ev = ComputeEV_EBITDA()
   rst = compute_ev.compute_factor('E02318', '20190403')
   print(rst)
   #data_peer = compute_ev.compute_factor_in_peer('E0005', '20190403')
   #print(data_peer)
   #print('industry_number =', data_peer[0])
   #print('industry_quantile =', data_peer[1])