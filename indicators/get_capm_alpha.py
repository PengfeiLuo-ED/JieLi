# coding=utf-8

import requests
import numpy as np
import datetime as dt
import pandas as pd
from sklearn.linear_model import LinearRegression

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
class ComputationCapmAlpha(BaseComputation):
      computation_model_name = 'CAPM阿尔法'
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
      def compute_factor(self, code, benchmark, end_date, rolling_window):
          # get source data
          stock_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(code, end_date))
          benchmark_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))
          # prepare for computation
          table = pd.concat([stock_data.close.pct_change(), benchmark_data.close.pct_change()], axis=1).dropna()
          table.columns = [code, benchmark]
          table.loc[table[code] == -np.inf , code] = 0
          # compute with model
          if len(table) >= rolling_window:
             matrix = np.zeros((len(table) - rolling_window + 1, 3))
             for i in range(0, len(matrix)):
                 model = LinearRegression()
                 x = table.iloc[i:i + rolling_window, 1].values
                 y = table.iloc[i:i + rolling_window, 0].values
                 result = model.fit(x.reshape(-1, 1), y.reshape(-1, 1))
                 alpha = result.intercept_[0] * 250
                 beta = result.coef_[0][0]
                 vol = np.std((y.reshape(-1, 1) - result.predict(x.reshape(-1, 1)))) * np.sqrt(250)
                 matrix[i, 0] = alpha
                 matrix[i, 1] = beta
                 matrix[i, 2] = vol
             temp = np.sort(matrix[-2500:, 0])
             n = np.where(temp == matrix[-1, 0])
             history_number = matrix[-1, 0]
             history_quantile = n[0][0] / len(temp)
             return [history_number, history_quantile]
          else:
             print('Price less than 1 year!')
             matrix = np.zeros((1, 3))
             model = LinearRegression()
             x = table.iloc[:, 0].values
             y = table.iloc[:, 1].values
             result = model.fit(x.reshape(-1, 1), y.reshape(-1, 1))
             alpha = result.intercept_[0] * 250
             beta = result.coef_[0][0]
             vol = np.std((y.reshape(-1, 1) - result.predict(x.reshape(-1, 1)))) * np.sqrt(250)
             matrix[0, 0] = alpha
             matrix[0, 1] = beta
             matrix[0, 2] = vol
             history_number = matrix[-1, 0]
             return [history_number, 1]
      # endregion
      # region Industry Comparision Computation
      def fetch_industry_data(self):
          data = pd.read_csv('ind_jieli.csv', encoding='utf-8')
          data['ind'] = data['GICS_ind'].apply(lambda item: item.split('_')[1])
          self.stock_industry_map = pd.Series(data['ind'].values, index=data['Code_jieli'].values).to_dict()
          self.industry_stocks_map = dict()
          for ind in set(data['ind']):
              x = data[data['ind'] == ind]['Code_jieli'].values
              self.industry_stocks_map[ind] = x
      def compute_factor_in_peer(self, stock_code, benchmark, end_date, rolling_window):
          peers = self.industry_stocks_map.get(self.stock_industry_map.get(stock_code))
          industry_vector = np.zeros(len(peers))
          for i in range(0, len(peers)):
              current_code = peers[i]
              print('computing industry peers =', current_code)
              computed_result = self.compute_factor(current_code, benchmark, end_date, rolling_window)
              industry_vector[i] = computed_result[0]
          industry_number = np.median(industry_vector)
          history_data = self.compute_factor(stock_code, benchmark, end_date, rolling_window)
          temp = np.sort(industry_vector)
          n = np.where(temp == history_data[0])
          industry_quantile = n[0][0] / len(temp)
          return [industry_number, industry_quantile, industry_vector]
      # endregion

if __name__ == '__main__':
   capm_alpha_model = ComputationCapmAlpha()
   rst = capm_alpha_model.compute_factor('E02319', 'EHSI', '20190418', 250)
   print(rst)
   #data_peer = capm_alpha_model.compute_factor_in_peer('E00700', 'EHSI', '20190403', 250)
   #print(data_peer)
   #print('industry_number =', data_peer[0])
   #print('industry_quantile =', data_peer[1])
   #print('industry_vector =', data_peer[2])











