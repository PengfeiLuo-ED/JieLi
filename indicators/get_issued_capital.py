# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 11:00:02 2019

@author: BOPU
"""

from get_sql import MySQL


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
class BaseComputation(BaseComputation):
      computation_model_name = '总发行股本'
      def __init__(self):
          self.fetch_industry_data()
          
      con = MySQL(host='192.168.9.197',user='wanwei',pwd='wanwei',db='Wanvi_HK_F10Product')   
      result = con.ExeQuery('SELECT * from HK_STK_ChangeOfShares')         
      
      