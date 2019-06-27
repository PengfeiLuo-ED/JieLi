# -*- coding: utf-8 -*-
"""
Created on Thu May 16 15:14:09 2019

@author: BOPU
"""


import requests
import numpy as np
import datetime as dt
import pandas as pd
from sklearn.linear_model import LinearRegression
#from stock import models
#from django.db import connections
import pymssql
from datetime import datetime
     

raw_file = open('ind_jieli.csv', encoding='utf-8')
raw_data = raw_file.readlines()
max_len = 0
industry_stocks_map = dict()
stock_industry_map = dict()
temp = []
for i, line in enumerate(raw_data):
    if i > 0 and line[0].isdigit():
       row = line.split(',')
       code = "0" + row[0]
       name = row[1]
       industry_category = row[5]
       Key = (code,name)       
       stock_industry_map[Key] = industry_category
       
for row in stock_industry_map:
    industry = stock_industry_map[row] 
    if industry_stocks_map.get(industry) is None:
       industry_stocks_map[industry] = list()                                  
    industry_stocks_map[industry].append(row)
    
               
table = pd.read_csv('ind_jieli.csv',encoding='utf-8')  
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '气油生产商(HS)'] = '能源业(HS)'          
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '气油设备与服务(HS)'] = '能源业(HS)'      
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '石油及天然气(HS)'] = '能源业(HS)'           
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '煤炭(HS)'] = '能源业(HS)'         
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '煤炭Ⅲ(HS)'] = '能源业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '黄金及贵金属Ⅲ(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '铜(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '铝(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '其他金属及矿物(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '钢铁(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '纸及纸制品(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '特殊化工用品(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '林业及木材(HS)'] = '原材料业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '化肥及农用化合物(HS)'] = '原材料业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '商业用车及货车(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '工业零件及器材(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '环保工程(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '重机械(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '新能源物料(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '航空航天与国防(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '采购及供应链管理(HS)'] = '工业工程(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '印刷及包装(HS)'] = '工业工程(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '航空货运及物流(HS)'] = '交通运输(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '航运及港口(HS)'] = '交通运输(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '铁路及公路(HS)'] = '交通运输(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '支援服务Ⅲ(HS)'] = '支援服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '支援服务(HS)'] = '支援服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '物业服务及管理(HS)'] = '支援服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '教育'] = '支援服务(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '汽车Ⅲ(HS)'] = '汽车业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '汽车零件(HS)'] = '汽车业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '家庭电器(HS)'] = '家用电器(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '家具(HS)'] = '家用电器(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '玩具及消闲用品(HS)'] = '家用电器(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '消费电子产品(HS)'] = '家用电器(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '纺织品及布料(HS)'] = '纺织业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '服装(HS)'] = '纺织业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '鞋类(HS)'] = '纺织业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '珠宝钟表(HS)'] = '纺织业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '美容及个人护理(HS)'] = '纺织业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '其他服饰配件(HS)'] = '纺织业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '食品添加剂(HS)'] = '食品饮料(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '非酒精饮料(HS)'] = '食品饮料(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '酒精饮料(HS)'] = '食品饮料(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '包装食品(HS)'] = '食品饮料(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '乳制品(HS)'] = '食品饮料(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '医疗保健设备(HS)'] = '医疗保健(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '药品(HS)'] = '医疗保健(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '生物科技(HS)'] = '医疗保健(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '医疗及医学美容服务(HS)'] = '医疗保健(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '农产品(HS)'] = '农业产品(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '禽畜肉类(HS)'] = '农业产品(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '禽畜饲料(HS)'] = '农业产品(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '超市及便利店(HS)'] = '零售业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '百货商城(HS)'] = '零售业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '公共运输(HS)'] = '交通运输(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '航空服务(HS)'] = '交通运输(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '酒店、赌场及消闲设施(HS)'] = '休闲服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '酒店及度假村(HS)'] = '休闲服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '消闲及文娱设施(HS)'] = '休闲服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '餐饮(HS)'] = '休闲服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '旅游及观光(HS)'] = '休闲服务(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '赌场及博彩(HS)'] = '休闲服务(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '广告及宣传(HS)'] = '媒体娱乐(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '广播(HS)'] = '媒体娱乐(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '出版(HS)'] = '媒体娱乐(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '影视娱乐(HS)'] = '媒体娱乐(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '电讯服务(HS)'] = '电讯业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '卫星及无线通讯(HS)'] = '电讯业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '电力(HS)'] = '公用事业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '燃气供应(HS)'] = '公用事业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '水务(HS)'] = '公用事业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '非传统/可再生能源(HS)'] = '公用事业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '综合企业Ⅲ(HS)'] = '综合业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '系统开发及资讯科技顾问(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '软件开发(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '软件服务(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '电脑及周边器材(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '电子商贸及互联网服务(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '电讯设备(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '半导体(HS)'] = '资讯科技业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '半导体Ⅲ(HS)'] = '资讯科技业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '地产发展商(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '地产代理(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '地产投资(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '重型基建(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '楼宇建造(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '建筑材料(HS)'] = '地产基建(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '建筑材料(HS)'] = '地产基建(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '银行(HS)'] = '银行业(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '银行Ⅲ(HS)'] = '银行业(HS)'

table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '保险(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '保险Ⅲ(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '其他金融(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '其他金融Ⅲ(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '证券及经纪(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '投资及资产管理(HS)'] = '非银金融(HS)'
table.loc[:,'HS_ind'][table.loc[:,'HS_ind'] == '信贷(HS)'] = '非银金融(HS)'



table[table.loc[:,'Code_jieli'] == 'E01025']


list(enumerate(set(table.iloc[:,5])))

for i in range(0,len(list(set(table.iloc[:,5])))):  
    name = list(set(table.iloc[:,5]))[i]
    num = len(table[table.loc[:,'HS_ind'] == name])
    print(name,num)







temp = pd.read_csv('ind_jeli2622'+'.csv')

temp['name'] = temp['InstitutionID'].apply(lambda x: 'E'+str(x)[-5:])







table = pd.read_csv('ind_jieli_.csv',encoding='utf-8')  

name = table[table.loc[:,'HS_ind']=='食品饮料(HS)']

table[table.loc[:,'Code_jieli'] == 'E00422']

small = table.loc[:,'Code_jieli']
big = temp['name']


diff_set = list(set(big) - set(small))

diff_set_ = [item[1:] +'.HK' for item in diff_set]



table = pd.read_csv('ind_jieli_.csv',encoding='utf-8')  

table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E00823'] = '领展房产基金'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E00405'] = '越秀房产信托基金'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E02778'] = '冠君产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E01275'] = '开元产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E01426'] = '春泉产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E87001'] = '汇贤产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E01881'] = '富豪产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E00808'] = '泓富产业信托'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E00435'] = '阳光房地产基金'
table.loc[:,'Name'][table.loc[:,'Code_jieli'] == 'E00778'] = '置富产业信托'

table.to_csv('ind_jieli_'+'.csv',encoding='utf-8')



table[table.loc[:,'HS_ind'] == '地产基建(HS)']













