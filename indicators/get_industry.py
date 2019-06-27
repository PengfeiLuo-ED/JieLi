# -*- coding: utf-8 -*-
"""
Created on Mon Apr 01 16:53:09 2019

@author: BOPU
"""

import pandas as pd

class IndustryInfo():
    
      def __init__(self):
         data = pd.read_csv('ind_jieli.csv',encoding='utf-8')
         data['ind'] = data['GICS_ind'].apply(lambda x: x.split('_')[1])
         self.ind_mapping = pd.Series(data['ind'].values , index = data['Code_jieli'].values).to_dict()
         self.ind_mapping_reverse = dict()
         for ind in set(data['ind']):             
             x = data[data['ind']==ind]['Code_jieli'].values
             self.ind_mapping_reverse[ind] = x
             
      def industry_name(self,ticker):
          if ticker in self.ind_mapping.keys():
             return self.ind_mapping[ticker] 
          else:
              print('wrong ticker!')
              return None
    
      def industry_peers(self,ticker):
          if ticker in self.ind_mapping.keys():
             return self.ind_mapping_reverse[self.ind_mapping[ticker]]
          else:
              print('wrong ticker!')
              return None
  
    

#if __name__ == '__main__':
#    ticker = 'E00700'
#    ii = IndustryInfo()
#    print(ii.industry_name(ticker))
#    print(ii.industry_peers(ticker))
    
    
#data = pd.read_csv('ind.csv',encoding='utf-8')
#data['Code_jieli'] = data['Code'].apply(lambda x: 'E0' + x.split('.')[0])
#data.to_csv('ind_jieli'+'.csv' , index = False , encoding = 'utf-8')
  
      



   