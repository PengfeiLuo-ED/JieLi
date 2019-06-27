# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 11:00:47 2019

@author: BOPU
"""
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

if __name__ == '__main__':  
   con = MySQL(host='192.168.9.197',user='wanwei',pwd='wanwei',db='Wanvi_HK_F10Product')   
   result = con.ExeQuery('SELECT * from HK_STK_ChangeOfShares')   
   result['Symbol'] = result['Symbol'].apply(lambda x: 'E'+x) 
   print(result[result['Symbol'] == 'E00700'][['IssuedCapital','ModifyTime']])
   #conn = MySQL(host='192.168.9.197',user='wanwei',pwd='wanwei',db='Wanvi_HK_F10Product')
   #result = conn.ExeQuery('SELECT * from HK_STK_StockInfo')
   #s = result['Symbol'].apply(lambda x: 'E'+x)      
   #data = pd.read_csv('ind_jieli.csv', encoding='utf-8')
   #name = data['Code_jieli']       
   #alla = len(set(s) & set(name))   
   #print(set(name) - alla)
   
   
   
   
   
   
   
   
   
   
    