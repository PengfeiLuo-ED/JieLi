# coding=utf-8

#import os
import requests
import numpy as np
import datetime as dt
import pandas as pd
import time
#import math
#import csv
#import json
import pymssql
#from stock import models
#from django.db import connections
from datetime import datetime
#from jieli.logger import risk_computation_logger as logger


class BaseComputation(object):

    def __init__(self):
        self.computation_model_name: str = None
        self.stock_industry_map: dict = None
        self.industry_stocks_map: dict = None
        #self.db_con = connections['wanvi']
        self.db_con = pymssql.connect(host='192.168.9.197',user='wanwei',password='wanwei',database='Wanvi_HK_F10Product',charset='utf8')

    def fetch_stock_data(self, code, end_date, stock_data_length=350, is_dev_mode=False):
        #if is_dev_mode:
        #    stock_prices_db_con = connections['jieli_db_2']
        #    stock_prices_db_con = pymssql.connect(host='192.168.9.176',user='readonly',password='wanwei')
        #    cursor = stock_prices_db_con.cursor()
        #    cursor.execute(
        #        """select t.*
        #            from (SELECT *
        #                  FROM stock_prices
        #                  WHERE code = %s
        #                    and date <= %s
        #                  order by date desc
        #                  LIMIT %s) t
        #            order by t.date;""",
        #        [code, end_date, stock_data_length])
        #    db_data = cursor.fetchall()
        #    result = []
        #    for row in db_data:
        #        result.append([int(row[2].strftime('%Y%m%d')), row[3], row[4], row[5], row[6], row[7], row[8]])
        #    return result
        if code in ['EHSI']:
            stock_code = code
        else:
            stock_code = 'E' + code
        session = requests.session()
        raw_data = []
        current_count = 0
        while current_count < stock_data_length:
            if len(raw_data) == 0:
                this_end_date = end_date
            else:
                # reset date key point for next fetch
                this_end_date = raw_data[-1][0] - 1
            url = 'https://jybdata.iqdii.com/jybapp/price/stock/kline?code=' + \
                  stock_code + '&count=200&fq=1&end=' + str(this_end_date)
            # retry logic
            response = None
            retry_times = 5
            while retry_times > 0:
                retry_times -= 1
                # fetch
                try:
                    response = session.get(url=url).json()
                except requests.exceptions.RequestException as e:
                    #logger.warning(
                    #    "fetching stock_code:[" + stock_code + "] at end_date:[" +
                    #    str(end_date) + "] retry times remains " + str(retry_times) + " exception:" + str(e))
                    time.sleep(0.1)  # retry pause
                    session = requests.session()
                    continue
                # end_date earlier than listing date will never have data. break fetch
                if response is not None and response.get('data') is not None and response.get('data').get(
                        'day') is not None and len(response['data']['day']) == 0 and response.get('data').get(
                    'listingdate') is not None:
                    try:
                        lst_d = str(response['data']['listingdate'])
                        dt_listing_date = datetime.strptime(lst_d, '%Y%m%d')
                        dt_end_date = datetime.strptime(end_date, '%Y%m%d')
                        if dt_end_date < dt_listing_date:
                            #logger.warning(
                            #    "fetching stock_code:[" + stock_code + "] at end_date:[" +
                            #    str(end_date) + "] retry times remains " + str(
                            #        retry_times) + " exception: end_date < listing_date")
                            break
                    except Exception as e:
                        continue
                # retry when although has response but NO prices data
                if response is not None and response.get('data') is not None and response.get('data').get(
                        'day') is not None and len(response['data']['day']) == 0:
                    # the prevention mechanism might block us from accessing, we do max 5 times retry
                    #logger.warning(
                    #    "fetching stock_code:[" + stock_code + "] at end_date:[" +
                    #    str(end_date) + "] although has response but NO prices data. retry times remain :" + str(
                    #        retry_times))
                    time.sleep(0.1)  # retry pause
                    session = requests.session()
                    continue            
                break          
            # concat data
            if response is not None and response.get('data') is not None and response.get('data').get(
                    'day') is not None:
                fetched_data: list = response['data']['day']
                if len(fetched_data) > 0:
                    # for data concatenation
                    current_count += len(fetched_data)
                    fetched_data.sort(key=lambda entry: entry[0], reverse=True)
                    raw_data.extend(fetched_data)
                else:
                    break
            else:
                break
        # no matter how we try, still got nothing
        if len(raw_data) == 0:
            #logger.error(
            #    "Fail to fetch stock prices via jieli API with stock_code:[" +
            #    stock_code + "] end_date [" + end_date + "] our request responds NO DATA")
            return raw_data

        # organize previous concat data for output
        raw_data.sort(key=lambda entry: entry[0])
        if stock_data_length < len(raw_data):
            return raw_data[-stock_data_length::]
        else:
            return raw_data

    def fetch_stock_data_raw_response(self, code, end_date):
        if code in ['EHSI']:
            stock_code = code
        else:
            stock_code = 'E' + code
        session = requests.session()
        url = 'https://jybdata.iqdii.com/jybapp/price/stock/kline?code=' + \
              stock_code + '&count=200&fq=1&end=' + str(end_date)
        # retry logic
        response = None
        retry_times = 15
        while retry_times > 0:
            retry_times -= 1
            try:
                response = session.get(url=url).json()
            except requests.exceptions.RequestException as e:
                # print('*api access error, retry times remains ' + str(retry_times) + " Exception:" + str(e))
                time.sleep(0.5)  # retry pause
                continue
            # successfully fetched, but not ensuring it has prices data
            if len(response['data']['day']) == 0:
                # print("??? although has response but NO prices data. retry fetch remain times :" + str(retry_times))
                time.sleep(0.5)  # retry pause
                session.cookies.clear()
                session = requests.session()
                continue
            break
        if response is None or response.get('data') is None:
            #logger.error('Failed to fetch data from jieli api for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + "]")
            return []
        return response

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

    def compute_factor(self, *args):
        pass

    def dictfetchall(self, cursor):
        # Return all rows from a cursor as a dict
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    #def fetch_industry_data(self):
    #    self.stock_industry_map = dict()
    #    self.industry_stocks_map = dict()
    #    db_data = models.Stock.objects.all()
    #    for row in db_data:
    #        # pre process code and category
    #        current_category = row.industry_category  # general industry category
    #        stock_code = row.code
    #        self.stock_industry_map[stock_code] = current_category
    #        if self.industry_stocks_map.get(current_category) is None:
    #            self.industry_stocks_map[current_category] = list()
    #        self.industry_stocks_map[current_category].append(stock_code)

    def fetch_industry_data(self, *args):
        data = pd.read_csv('ind_jieli_.csv', encoding='utf-8')
        data['ind'] = data['HS_ind']
        self.stock_industry_map = pd.Series(data['ind'].values, index = data['Code_jieli'].values).to_dict()
        self.industry_stocks_map = dict()
        for ind in set(data['ind']):
            x = data[data['ind'] == ind]['Code_jieli'].values
            self.industry_stocks_map[ind] = x

    def compute_factor_in_peer(self, stock_code, end_date, cache=None):
        peers = self.industry_stocks_map.get(self.stock_industry_map.get(stock_code))
        industry_vector = np.zeros(len(peers))
        for i in range(0, len(peers)):
            current_code = peers[i]
            # print('computing ' + self.computation_model_name + " for " + stock_code + "; current peer @"
            #       + current_code + " progress @ " + str(round((i / len(peers)) * 100, 1)) + " %;")
            computed_result = self.compute_factor(current_code, end_date, cache)
            industry_vector[i] = computed_result[0]
        industry_vector = industry_vector[~np.isnan(industry_vector)]
        industry_number = np.median(industry_vector)
        history_data = self.compute_factor(stock_code, end_date)
        temp = np.sort(industry_vector)
        n = np.where(temp == history_data[0])
        try:
            industry_quantile = n[0][0] / len(temp)
        except IndexError:
            industry_quantile = 1
        return [industry_number, industry_quantile]
    
    def save_to_db(self, *args):
        pass

# 1
class ComputationNetProfitMargin(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '销售净利率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,NetProfitMargin,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Deposits_Equity,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'NetProfitMargin', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1 = pd.DataFrame(result1['NetProfitMargin'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['NetProfitMargin'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 2
class ComputationROA(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'ROA'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,ReturnOnTotalAssets,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,ReturnOnTotalAssets,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.ReportType == '2',
                        'ReturnOnTotalAssets'] = result1.loc[result1.ReportType == '2', 'ReturnOnTotalAssets'] * 2
            result1 = pd.DataFrame(result1['ReturnOnTotalAssets'])
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['ReturnOnTotalAssets'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
        #models.RiskIndexHistory.objects.create(
        #    risk_index=models.RiskIndex.objects.get(
        #        category=models.RiskCategoryEnum.BASIC,
        #        detail_category=self.computation_model_name
        #    ),
        #    value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
        #    history_quantile=round(history_quantile, 4),
        #    industry_quantile=round(industry_quantile, 4),
        #    stock_id=stock_code,
        #    date=date,
        #    is_warning=is_warning
        #)

# 3
class ComputationROE(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'ROE'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,ReturnOnEquity,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,ReturnOnEquity,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.ReportType == '2',
                        'ReturnOnEquity'] = result1.loc[result1.ReportType == '2', 'ReturnOnEquity'] * 2
            result1 = pd.DataFrame(result1['ReturnOnEquity'])
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['ReturnOnEquity'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 4
class ComputationROIC(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'ROIC'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,ReturnOnCapitalEmploy,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,ReturnOnLoans,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'ReturnOnCapitalEmploy', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.ReportType == '2',
                        'ReturnOnCaitalEmploy'] = result1.loc[result1.ReportType == '2', 'ReturnOnCapitalEmploy'] * 2
            result1 = pd.DataFrame(result1['ReturnOnCapitalEmploy'])
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['ReturnOnCapitalEmploy'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 5
class ComputationGrossProfitMargin(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '销售毛利率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Pre_taxProfitMargin,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.DataFrame(result1['Pre_taxProfitMargin'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['Pre_taxProfitMargin'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 6
class ComputationOperatingProfitMargin(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '主营业务利润率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,OperatingProfitMargin,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Loans_TotalAssets,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'OperatingProfitMargin', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1 = pd.DataFrame(result1['OperatingProfitMargin'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['OperatingProfitMargin'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 7
class ComputationInventoryTurnoverRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '存货周转率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,InventoryTurnover,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1.loc[result1.ReportType == '2',
                        'InventoryTurnover'] = result1.loc[result1.ReportType == '2', 'InventoryTurnover'] * 2
            result1 = pd.DataFrame(result1['InventoryTurnover'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['InventoryTurnover'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 8
class ComputationAssetTurnoverRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '总资产周转率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql("SELECT ReportType,TotalAssets,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,TotalAssets,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result3 = pd.read_sql("SELECT ReportType,Turnover,YearEnd from HK_STK_FIN_Income where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4 = pd.read_sql(
                "SELECT ReportType,NetInterestIncome,YearEnd from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4.columns = ['ReportType', 'Turnover', 'YearEnd']
            result3 = pd.concat([result3, result4], axis=0)
            result3.loc[result3.ReportType == '2', 'Turnover'] = result3.loc[result3.ReportType == '2', 'Turnover'] * 2
            result = pd.merge(result1, result3, on='YearEnd', how='outer').fillna(method='ffill').fillna(method='bfill')
            result['result'] = result['Turnover'] / result['TotalAssets']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 9
class ComputationNonCurrentAssetTurnoverRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '固定资产周转率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Non_currentAssets,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql("SELECT ReportType,Loans,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'Non_currentAssets', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result3 = pd.read_sql("SELECT ReportType,Turnover,YearEnd from HK_STK_FIN_Income where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4 = pd.read_sql(
                "SELECT ReportType,NetInterestIncome,YearEnd from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4.columns = ['ReportType', 'Turnover', 'YearEnd']
            result3 = pd.concat([result3, result4], axis=0)
            result3.loc[result3.ReportType == '2', 'Turnover'] = result3.loc[result3.ReportType == '2', 'Turnover'] * 2
            result = pd.merge(result1, result3, on='YearEnd', how='outer').fillna(method='ffill').fillna(method='bfill')
            result['result'] = result['Turnover'] / result['Non_currentAssets']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 10
class ComputationCurrentAssetTurnoverRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '流动资产周转率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,CurrentAssets,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,LiquidFunds,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'CurrentAssets', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result3 = pd.read_sql("SELECT ReportType,Turnover,YearEnd from HK_STK_FIN_Income where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4 = pd.read_sql(
                "SELECT ReportType,NetInterestIncome,YearEnd  from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result4.columns = ['ReportType', 'Turnover', 'YearEnd']
            result3 = pd.concat([result3, result4], axis=0)
            result3.loc[result3.ReportType == '2', 'Turnover'] = result3.loc[result3.ReportType == '2', 'Turnover'] * 2
            result = pd.merge(result1, result3, on='YearEnd', how='outer').fillna(method='ffill').fillna(method='bfill')
            result['result'] = result['Turnover'] / result['CurrentAssets']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 11
class ComputationCurrentRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '流动比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,CurrentRatio,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,CoreAdequacyRatio,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2['CoreAdequacyRatio'] = result2['CoreAdequacyRatio'] / 10
            result2.columns = ['ReportType', 'CurrentRatio', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1 = pd.DataFrame(result1['CurrentRatio'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['CurrentRatio'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 12
class ComputationQuickRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '速动比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,QuickRatio,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,CoreCapitalAdequacyRatio,YearEnd from view_STKFINBankOfFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2['CoreCapitalAdequacyRatio'] = result2['CoreCapitalAdequacyRatio'] / 10
            result2.columns = ['ReportType', 'QuickRatio', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1 = pd.DataFrame(result1['QuickRatio'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['QuickRatio'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 13
class ComputationCashRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '现金比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,CurrentLiabilities,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Deposits,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result3 = pd.read_sql("SELECT ReportType,NCFEY,YearEnd from HK_STK_FIN_CashFlow where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'CurrentLiabilities', 'YearEnd']
            result3.loc[result3.ReportType == 2, ['NCFEY']] = result3.loc[result3.ReportType == 2, ['NCFEY']] * 2
            result1 = pd.concat([result1, result2], axis=0)
            result = pd.merge(result1, result3, on='YearEnd', how='outer').fillna(method='ffill').fillna(method='bfill')
            result['result'] = result['NCFEY'] / result['CurrentLiabilities']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 14
class ComputationDebtAssetRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '资产负债率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,DebtAssetRatio,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,TotalAssets,TotalLiabilities,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2['DebtAssetRatio'] = result2['TotalLiabilities'] * 100 / result2['TotalAssets']
            result2 = result2.loc[:, ['ReportType', 'DebtAssetRatio', 'YearEnd']]
            result1 = pd.concat([result1, result2], axis=0)
            result1 = pd.DataFrame(result1['DebtAssetRatio'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['DebtAssetRatio'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 15
class ComputationMarketLeverageRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '市场杠杆比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result3 = pd.read_sql("SELECT IssuedCapital,DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                stock_code + ' order by ModifyTime'), con=self.db_con)
            result3['DateofChange'] = pd.to_datetime(result3['DateofChange'])
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_price = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_price = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_price['DateofChange'] = self_price.index.values
            cap_data = pd.merge(self_price, result3, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            result1 = pd.read_sql(
                'SELECT TotalLiabilities,Equity,ModifyTime from HK_STK_FIN_Balance where Symbol = ' + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                'SELECT TotalLiabilities,Equity,ModifyTime from HK_STK_FIN_BankOfBalance where Symbol = ' + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1['DateofChange'] = pd.to_datetime(result1['ModifyTime'])
            self_data = pd.merge(result1, cap_data, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['result'] = self_data['TotalLiabilities'] * 1000 / (
                    self_data['close'] * self_data['IssuedCapital'])
            self_data = self_data.dropna(axis=0)
            result1 = self_data['result'].values
            history_number = result1[-1]
            result1 = np.sort(result1, kind='quicksort')
            n = np.where(result1 == history_number)
            history_quantile = 1 - n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 16
class ComputationBookLeverageRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '账面杠杆比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,TotalLiabilities,Equity,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,TotalLiabilities,Equity,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1['result'] = result1['TotalLiabilities'] / result1['Equity']
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = 1 - n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 17
class ComputationDCR(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '债务保障比率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,TotalLiabilities,ModifyTime from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,TotalLiabilities,ModifyTime from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1['ModifyTime'] = pd.to_datetime(result1['ModifyTime'])
            result1.loc[result1.ReportType == 2,
                        'TotalLiabilities'] = result1.loc[result1.ReportType == 2, 'TotalLiabilities'] * 2
            result1.loc[result1.Currency == 'CNY',
                        'TotalLiabilities'] = result1.loc[result1.Currency == 'CNY', 'TotalLiabilities'] / 0.85
            result1.loc[result1.Currency == 'JPY',
                        'TotalLiabilities'] = result1.loc[result1.Currency == 'JPY', 'TotalLiabilities'] / 5.99
            result1.loc[result1.Currency == 'MYR',
                        'TotalLiabilities'] = result1.loc[result1.Currency == 'MYR', 'TotalLiabilities'] * 1.62
            result1.loc[result1.Currency == 'SGD',
                        'TotalLiabilities'] = result1.loc[result1.Currency == 'SGD', 'TotalLiabilities'] * 4.95
            result1.loc[result1.Currency == 'USD',
                        'TotalLiabilities'] = result1.loc[result1.Currency == 'USD', 'TotalLiabilities'] * 7.6
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,CEBY,NCFEY,ModifyTime from HK_STK_FIN_CashFlow where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2['ModifyTime'] = pd.to_datetime(result2['ModifyTime'])
            result2.loc[result2.ReportType == 2,
                        ['CEBY', 'NCFEY']] = result2.loc[result2.ReportType == 2, ['CEBY', 'NCFEY']] * 2
            result2.loc[result2.Currency == 'CNY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'CNY', ['CEBY', 'NCFEY']] / 0.85
            result2.loc[result2.Currency == 'JPY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'JPY', ['CEBY', 'NCFEY']] / 5.99
            result2.loc[result2.Currency == 'MYR',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'MYR', ['CEBY', 'NCFEY']] * 1.62
            result2.loc[result2.Currency == 'SGD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'SGD', ['CEBY', 'NCFEY']] * 4.95
            result2.loc[result2.Currency == 'USD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'USD', ['CEBY', 'NCFEY']] * 7.6
            result = pd.merge(result1, result2, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            result['result'] = (result['NCFEY'] - result['CEBY']) / result['TotalLiabilities']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 18
class ComputationSalesIncomePctchg(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '销售收入同比增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,Turnover,YearEnd from HK_STK_FIN_Income where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,NetInterestIncome,YearEnd from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'Currency', 'Turnover', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.Currency == 'CNY', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'CNY', 'Turnover'] / 0.85
            result1.loc[result1.Currency == 'JPY', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'JPY', 'Turnover'] / 5.99
            result1.loc[result1.Currency == 'MYR', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'MYR', 'Turnover'] * 1.62
            result1.loc[result1.Currency == 'SGD', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'SGD', 'Turnover'] * 4.95
            result1.loc[result1.Currency == 'USD', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'USD', 'Turnover'] * 7.6
            result1['year'] = result1['YearEnd'].apply(lambda x: x[:4])
            result1['month'] = result1['YearEnd'].apply(lambda x: x[4:6])
            re6 = result1[result1.month == '06'].sort_values(by='year')
            re12 = result1[result1.month == '12'].sort_values(by='year')
            re6['result'] = re6['Turnover'].pct_change()
            re12['result'] = re12['Turnover'].pct_change()
            result1 = pd.concat([re6, re12], axis=0).sort_values(by='year')
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 19
class ComputationNetProfitPctchg(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '净利润同比增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,NetProfit,YearEnd from HK_STK_FIN_Income where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,NetProfit,YearEnd from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.ReportType == 2, 'NetProfit'] = result1.loc[result1.ReportType == 2, 'NetProfit'] * 2
            result1.loc[result1.Currency == 'CNY', 'NetProfit'] = result1.loc[
                                                                      result1.Currency == 'CNY', 'NetProfit'] / 0.85
            result1.loc[result1.Currency == 'JPY', 'NetProfit'] = result1.loc[
                                                                      result1.Currency == 'JPY', 'NetProfit'] / 5.99
            result1.loc[result1.Currency == 'MYR', 'NetProfit'] = result1.loc[
                                                                      result1.Currency == 'MYR', 'NetProfit'] * 1.62
            result1.loc[result1.Currency == 'SGD', 'NetProfit'] = result1.loc[
                                                                      result1.Currency == 'SGD', 'NetProfit'] * 4.95
            result1.loc[result1.Currency == 'USD', 'NetProfit'] = result1.loc[
                                                                      result1.Currency == 'USD', 'NetProfit'] * 7.6
            result1['year'] = result1['YearEnd'].apply(lambda x: x[:4])
            result1['month'] = result1['YearEnd'].apply(lambda x: x[4:6])
            re6 = result1[result1.month == '06'].sort_values(by='year')
            re12 = result1[result1.month == '12'].sort_values(by='year')
            re6['result'] = re6['NetProfit'].pct_change()
            re12['result'] = re12['NetProfit'].pct_change()
            result1 = pd.concat([re6, re12], axis=0).sort_values(by='year')
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 20
class ComputationTotalAssetPctchg(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '总资产同比增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,TotalAssets,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,TotalAssets,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.ReportType == 2,
                        'TotalAssets'] = result1.loc[result1.ReportType == 2, 'TotalAssets'] * 2
            result1.loc[result1.Currency == 'CNY',
                        'TotalAssets'] = result1.loc[result1.Currency == 'CNY', 'TotalAssets'] / 0.85
            result1.loc[result1.Currency == 'JPY',
                        'TotalAssets'] = result1.loc[result1.Currency == 'JPY', 'TotalAssets'] / 5.99
            result1.loc[result1.Currency == 'MYR',
                        'TotalAssets'] = result1.loc[result1.Currency == 'MYR', 'TotalAssets'] * 1.62
            result1.loc[result1.Currency == 'SGD',
                        'TotalAssets'] = result1.loc[result1.Currency == 'SGD', 'TotalAssets'] * 4.95
            result1.loc[result1.Currency == 'USD',
                        'TotalAssets'] = result1.loc[result1.Currency == 'USD', 'TotalAssets'] * 7.6
            result1['year'] = result1['YearEnd'].apply(lambda x: x[:4])
            result1['month'] = result1['YearEnd'].apply(lambda x: x[4:6])
            re6 = result1[result1.month == '06'].sort_values(by='year')
            re12 = result1[result1.month == '12'].sort_values(by='year')
            re6['result'] = re6['TotalAssets'].pct_change()
            re12['result'] = re12['TotalAssets'].pct_change()
            result1 = pd.concat([re6, re12], axis=0).sort_values(by='YearEnd')
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 21
class ComputationNonCurrentAssetRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '固定资产占比'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Non_currentAssets,TotalAssets,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Loans,TotalAssets,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.columns = ['ReportType', 'Non_currentAssets', 'TotalAssets', 'YearEnd']
            result1 = pd.concat([result1, result2], axis=0)
            result1['result'] = result1['Non_currentAssets'] / result1['TotalAssets']
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)

            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 22
class ComputationGrossProfitMarginChange(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '毛利润增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Pre_taxProfitMargin,YearEnd from view_STKFINFinancialRatios where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1['Pre_taxProfitMargin'] = result1['Pre_taxProfitMargin'].astype(float)
            result1['year'] = result1['YearEnd'].apply(lambda x: x[:4])
            result1['month'] = result1['YearEnd'].apply(lambda x: x[4:6])
            re6 = result1[result1.month == '06'].sort_values(by='year')
            re12 = result1[result1.month == '12'].sort_values(by='year')
            re6['result'] = re6['Pre_taxProfitMargin'].diff()
            re12['result'] = re12['Pre_taxProfitMargin'].diff()
            result1 = pd.concat([re6, re12], axis=0).sort_values(by='YearEnd').dropna()
            result1 = pd.DataFrame(result1['result'])
            result1 = result1.dropna(axis=0)
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 23
class ComputationNetCashFlowPctchg(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '净现金流同比增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result2 = pd.read_sql("SELECT Currency,CEBY,NCFEY,YearEnd from HK_STK_FIN_CashFlow where Symbol = " + str(
                stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2.loc[result2.Currency == 'CNY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'CNY', ['CEBY', 'NCFEY']] / 0.85
            result2.loc[result2.Currency == 'JPY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'JPY', ['CEBY', 'NCFEY']] / 5.99
            result2.loc[result2.Currency == 'MYR',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'MYR', ['CEBY', 'NCFEY']] * 1.62
            result2.loc[result2.Currency == 'SGD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'SGD', ['CEBY', 'NCFEY']] * 4.95
            result2.loc[result2.Currency == 'USD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'USD', ['CEBY', 'NCFEY']] * 7.6
            result2['result'] = (result2['NCFEY'] - result2['CEBY'])
            result2['year'] = result2['YearEnd'].apply(lambda x: x[:4])
            result2['month'] = result2['YearEnd'].apply(lambda x: x[4:6])
            re6 = result2[result2.month == '06'].sort_values(by='year')
            re12 = result2[result2.month == '12'].sort_values(by='year')
            re6['result'] = re6['result'].pct_change()
            re12['result'] = re12['result'].pct_change()
            result2 = pd.concat([re6, re12], axis=0).sort_values(by='YearEnd')
            result2 = pd.DataFrame(result2['result'])
            history_number = result2.values[-1][0]
            result2 = result2.sort_values(['result'], ascending=True)
            n = np.where(result2.values == history_number)
            history_quantile = n[0][0] / len(result2)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 24
class ComputationEquityPctchg(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '资本累积同比增长率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,Equity,YearEnd from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,Equity,YearEnd from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by YearEnd'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1.loc[result1.Currency == 'CNY', 'Equity'] = result1.loc[result1.Currency == 'CNY', 'Equity'] / 0.85
            result1.loc[result1.Currency == 'JPY', 'Equity'] = result1.loc[result1.Currency == 'JPY', 'Equity'] / 5.99
            result1.loc[result1.Currency == 'MYR', 'Equity'] = result1.loc[result1.Currency == 'MYR', 'Equity'] * 1.62
            result1.loc[result1.Currency == 'SGD', 'Equity'] = result1.loc[result1.Currency == 'SGD', 'Equity'] * 4.95
            result1.loc[result1.Currency == 'USD', 'Equity'] = result1.loc[result1.Currency == 'USD', 'Equity'] * 7.6
            result1['year'] = result1['YearEnd'].apply(lambda x: x[:4])
            result1['month'] = result1['YearEnd'].apply(lambda x: x[4:6])
            re6 = result1[result1.month == '06'].sort_values(by='year')
            re12 = result1[result1.month == '12'].sort_values(by='year')
            re6['result'] = re6['Equity'].pct_change()
            re12['result'] = re12['Equity'].pct_change()
            result1 = pd.concat([re6, re12], axis=0).sort_values(by='YearEnd')
            result1 = pd.DataFrame(result1['result'])
            history_number = result1.values[-1][0]
            result1 = result1.sort_values(['result'], ascending=True)
            n = np.where(result1.values == history_number)
            history_quantile = n[0][0] / len(result1)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 25
class ComputationPE(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '市盈率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            share_data = pd.read_sql(
                "SELECT EPS_HKD,ReportType,ModifyTime from HK_STK_EarningsSummary where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            share_data['ModifyTime'] = pd.to_datetime(share_data['ModifyTime'])
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_price = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_price = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_price['ModifyTime'] = self_price.index.values
            share_data.loc[share_data.ReportType == '2', 'EPS_HKD'] = share_data.loc[
                                                                          share_data.ReportType == '2', 'EPS_HKD'] * 1.8
            self_data = pd.merge(self_price, share_data, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['result'] = (self_data['close'] / self_data['EPS_HKD'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = 1 - n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 26
class ComputationPB(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '市净率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT Currency,Equity,ModifyTime from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT Currency,Equity,ModifyTime from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_price = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_price = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_price['DateofChange'] = self_price.index.values
            result = pd.concat([result1, result2], axis=0)
            result['ModifyTime'] = pd.to_datetime(result['ModifyTime'])
            share_data = result[['Currency', 'Equity', 'ModifyTime']]
            share_data.loc[share_data.Currency == 'CNY', 'Equity'] = share_data.loc[
                                                                         share_data.Currency == 'CNY', 'Equity'] / 0.85
            share_data.loc[share_data.Currency == 'JPY', 'Equity'] = share_data.loc[
                                                                         share_data.Currency == 'JPY', 'Equity'] / 5.99
            share_data.loc[share_data.Currency == 'MYR', 'Equity'] = share_data.loc[
                                                                         share_data.Currency == 'MYR', 'Equity'] / 1.62
            share_data.loc[share_data.Currency == 'SGD', 'Equity'] = share_data.loc[
                                                                         share_data.Currency == 'SGD', 'Equity'] * 4.95
            share_data.loc[share_data.Currency == 'USD', 'Equity'] = share_data.loc[
                                                                         share_data.Currency == 'USD', 'Equity'] * 7.6
            share_data['DateofChange'] = pd.to_datetime(share_data['ModifyTime'])
            self_data = pd.merge(self_price, share_data, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            share_data = pd.read_sql(
                "SELECT IssuedCapital,DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            share_data['DateofChange'] = pd.to_datetime(share_data['DateofChange'])
            self_data = pd.merge(self_data, share_data, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['result'] = (self_data['IssuedCapital'] * self_data['close'] / self_data['Equity'] / 1000)
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = 1 - n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 27
class ComputationPS(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '市销率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result = pd.read_sql(
                "SELECT IssuedCapital,DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            result['DateofChange'] = pd.to_datetime(result['DateofChange'])
            shares = result[['IssuedCapital', 'DateofChange']]
            share_data = pd.read_sql(
                "SELECT Currency,ReportType,Turnover,ModifyTime from HK_STK_FIN_Income where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            share_data['DateofChange'] = pd.to_datetime(share_data['ModifyTime'])
            share_data.loc[share_data.Currency == 'CNY', 'Turnover'] = share_data.loc[
                                                                           share_data.Currency == 'CNY', 'Turnover'] / 0.85
            share_data.loc[share_data.Currency == 'JPY', 'Turnover'] = share_data.loc[
                                                                           share_data.Currency == 'JPY', 'Turnover'] / 5.99
            share_data.loc[share_data.Currency == 'MYR', 'Turnover'] = share_data.loc[
                                                                           share_data.Currency == 'MYR', 'Turnover'] / 1.62
            share_data.loc[share_data.Currency == 'SGD', 'Turnover'] = share_data.loc[
                                                                           share_data.Currency == 'SGD', 'Turnover'] * 4.95
            share_data.loc[share_data.Currency == 'USD', 'Turnover'] = share_data.loc[
                                                                           share_data.Currency == 'USD', 'Turnover'] * 7.6
            share_data.loc[share_data.ReportType == '2', 'Turnover'] = share_data.loc[
                                                                           share_data.ReportType == '2', 'Turnover'] * 2
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_price = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_price = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_price['DateofChange'] = self_price.index.values
            self_data = pd.merge(self_price, share_data, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data = pd.merge(self_data, shares, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['result'] = (self_data['IssuedCapital'] * self_data['close'] / self_data['Turnover'] / 1000)
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = 1 - n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 28
class ComputationEvEbitda(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '企业价值倍数'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,ProfitBeforeTaxation,ModifyTime from HK_STK_FIN_Income where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,ProfitBeforeTaxation,ModifyTime from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result1 = pd.concat([result1, result2], axis=0)
            result1['ModifyTime'] = pd.to_datetime(result1['ModifyTime'])
            result1.loc[result1.ReportType == 2,
                        'ProfitBeforeTaxation'] = result1.loc[result1.ReportType == 2, 'ProfitBeforeTaxation'] * 2
            result1['ProfitBeforeTaxation'] = result1['ProfitBeforeTaxation'] * 1000
            result1.loc[result1.Currency == 'CNY',
                        'ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'CNY', 'ProfitBeforeTaxation'] / 0.85
            result1.loc[result1.Currency == 'JPY',
                        'ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'JPY', 'ProfitBeforeTaxation'] / 5.99
            result1.loc[result1.Currency == 'MYR',
                        'ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'MYR', 'ProfitBeforeTaxation'] / 1.62
            result1.loc[result1.Currency == 'SGD',
                        'ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'SGD', 'ProfitBeforeTaxation'] * 4.95
            result1.loc[result1.Currency == 'USD',
                        'ProfitBeforeTaxation'] = result1.loc[result1.Currency == 'USD', 'ProfitBeforeTaxation'] * 7.6
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,NCFEY,ModifyTime from HK_STK_FIN_CashFlow where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2['ModifyTime'] = pd.to_datetime(result2['ModifyTime'])
            result2.loc[result2.ReportType == 2, 'NCFEY'] = result2.loc[result2.ReportType == 2, 'NCFEY'] * 2
            result2['NCFEY'] = result2['NCFEY'] * 1000
            result2.loc[result2.Currency == 'CNY', 'NCFEY'] = result2.loc[result2.Currency == 'CNY', 'NCFEY'] / 0.85
            result2.loc[result2.Currency == 'JPY', 'NCFEY'] = result2.loc[result2.Currency == 'JPY', 'NCFEY'] / 5.99
            result2.loc[result2.Currency == 'MYR', 'NCFEY'] = result2.loc[result2.Currency == 'MYR', 'NCFEY'] / 1.62
            result2.loc[result2.Currency == 'SGD', 'NCFEY'] = result2.loc[result2.Currency == 'SGD', 'NCFEY'] * 4.95
            result2.loc[result2.Currency == 'USD', 'NCFEY'] = result2.loc[result2.Currency == 'USD', 'NCFEY'] * 7.6
            self_data = pd.merge(result1, result2, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            result3 = pd.read_sql(
                "SELECT IssuedCapital,DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            result3['ModifyTime'] = pd.to_datetime(result3['DateofChange'])
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_price = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_price = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_price['ModifyTime'] = self_price.index.values
            cap_data = pd.merge(self_price, result3, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data = pd.merge(self_data, cap_data, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,TotalLiabilities,ModifyTime from HK_STK_FIN_Balance where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,TotalLiabilities,ModifyTime from HK_STK_FIN_BankOfBalance where Symbol = " + str(
                    stock_code) + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime', con=self.db_con)
            result = pd.concat([result1, result2], axis=0)
            result['ModifyTime'] = pd.to_datetime(result['ModifyTime'])
            result.loc[result.ReportType == 2,
                       'TotalLiabilities'] = result.loc[result.ReportType == 2, 'TotalLiabilities'] * 2
            result['TotalLiabilities'] = result['TotalLiabilities'] * 1000
            result.loc[result.Currency == 'CNY',
                       'TotalLiabilities'] = result.loc[result.Currency == 'CNY', 'TotalLiabilities'] / 0.85
            result.loc[result.Currency == 'JPY',
                       'TotalLiabilities'] = result.loc[result.Currency == 'JPY', 'TotalLiabilities'] / 5.99
            result.loc[result.Currency == 'MYR',
                       'TotalLiabilities'] = result.loc[result.Currency == 'MYR', 'TotalLiabilities'] / 1.62
            result.loc[result.Currency == 'SGD',
                       'TotalLiabilities'] = result.loc[result.Currency == 'SGD', 'TotalLiabilities'] * 4.95
            result.loc[result.Currency == 'USD',
                       'TotalLiabilities'] = result.loc[result.Currency == 'USD', 'TotalLiabilities'] * 7.6
            self_data = pd.merge(self_data, result, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['result'] = ((self_data['close'] * self_data['IssuedCapital'] + self_data['TotalLiabilities'] -
                                    self_data['NCFEY']) / self_data['ProfitBeforeTaxation'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = 1 - n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 29
class ComputationCashFlowRatio(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '自由现金流与经营活动现金流比值'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,NCFOA,CEBY,NCFEY,ModifyTime from HK_STK_FIN_CashFlow where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2.loc[result2.ReportType == 2,
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.ReportType == 2,
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] * 2
            result2.loc[result2.Currency == 'CNY',
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'CNY',
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] / 0.85
            result2.loc[result2.Currency == 'JPY',
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'JPY',
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] / 5.99
            result2.loc[result2.Currency == 'MYR',
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'MYR',
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] * 1.62
            result2.loc[result2.Currency == 'SGD',
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'SGD',
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] * 4.95
            result2.loc[result2.Currency == 'USD',
                        ['NCFOA', 'CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'USD',
                                                                  ['NCFOA', 'CEBY', 'NCFEY']] * 7.6
            result2['ModifyTime'] = pd.to_datetime(result2['ModifyTime'])
            result2['result'] = (result2['NCFEY'] - result2['CEBY']) / result2['NCFOA']
            result = pd.DataFrame(result2['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 30
class ComputationSaleCash(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '单位销售现金净流入'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result1 = pd.read_sql(
                "SELECT ReportType,Currency,Turnover,ModifyTime from HK_STK_FIN_Income where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,NetInterestIncome,ModifyTime from HK_STK_FIN_BankOfIncome where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2.columns = ['ReportType', 'Currency', 'Turnover', 'ModifyTime']
            result1 = pd.concat([result1, result2], axis=0)
            result1['ModifyTime'] = pd.to_datetime(result1['ModifyTime'])
            result1.loc[result1.ReportType == 2, 'Turnover'] = result1.loc[result1.ReportType == 2, 'Turnover'] * 2
            result1.loc[result1.Currency == 'CNY', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'CNY', 'Turnover'] / 0.85
            result1.loc[result1.Currency == 'JPY', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'JPY', 'Turnover'] / 5.99
            result1.loc[result1.Currency == 'MYR', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'MYR', 'Turnover'] * 1.62
            result1.loc[result1.Currency == 'SGD', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'SGD', 'Turnover'] * 4.95
            result1.loc[result1.Currency == 'USD', 'Turnover'] = result1.loc[
                                                                     result1.Currency == 'USD', 'Turnover'] * 7.6
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,CEBY,NCFEY,ModifyTime from HK_STK_FIN_CashFlow where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2['ModifyTime'] = pd.to_datetime(result2['ModifyTime'])
            result2.loc[result2.ReportType == 2,
                        ['CEBY', 'NCFEY']] = result2.loc[result2.ReportType == 2, ['CEBY', 'NCFEY']] * 2
            result2.loc[result2.Currency == 'CNY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'CNY', ['CEBY', 'NCFEY']] / 0.85
            result2.loc[result2.Currency == 'JPY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'JPY', ['CEBY', 'NCFEY']] / 5.99
            result2.loc[result2.Currency == 'MYR',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'MYR', ['CEBY', 'NCFEY']] * 1.62
            result2.loc[result2.Currency == 'SGD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'SGD', ['CEBY', 'NCFEY']] * 4.95
            result2.loc[result2.Currency == 'USD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'USD', ['CEBY', 'NCFEY']] * 7.6
            result = pd.merge(result1, result2, on='ModifyTime', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            result['result'] = (result['NCFEY'] - result['CEBY']) / result['Turnover']
            result = pd.DataFrame(result['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 31
class ComputationOperationCashFlow(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '经营活动现金流'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            result2 = pd.read_sql(
                "SELECT ReportType,Currency,CEBY,NCFEY,ModifyTime from HK_STK_FIN_CashFlow where Symbol = " + str(
                    stock_code + " and YearEnd <= " + end_date[0:6] + ' order by ModifyTime'), con=self.db_con)
            result2['ModifyTime'] = pd.to_datetime(result2['ModifyTime'])
            result2.loc[result2.ReportType == 2,
                        ['CEBY', 'NCFEY']] = result2.loc[result2.ReportType == 2, ['CEBY', 'NCFEY']] * 2
            result2.loc[result2.Currency == 'CNY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'CNY', ['CEBY', 'NCFEY']] / 0.85
            result2.loc[result2.Currency == 'JPY',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'JPY', ['CEBY', 'NCFEY']] / 5.99
            result2.loc[result2.Currency == 'MYR',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'MYR', ['CEBY', 'NCFEY']] * 1.62
            result2.loc[result2.Currency == 'SGD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'SGD', ['CEBY', 'NCFEY']] * 4.95
            result2.loc[result2.Currency == 'USD',
                        ['CEBY', 'NCFEY']] = result2.loc[result2.Currency == 'USD', ['CEBY', 'NCFEY']] * 7.6
            result2['result'] = (result2['NCFEY'] - result2['CEBY'])
            result = pd.DataFrame(result2['result'])
            result = result.dropna(axis=0)
            history_number = result.values[-1][0]
            result = result.sort_values(['result'], ascending=True)
            n = np.where(result.values == history_number)
            history_quantile = n[0][0] / len(result)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.BASIC,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 32
class ComputationPriceTrend(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '价格趋势'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        short_window = 12
        long_window = 26
        diff_window = 9
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_close = pd.DataFrame(cache[stock_code]['close'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_close = pd.DataFrame(fetched_data['close'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_close['EMA_long'] = self_close['close'].ewm(adjust=False, alpha=2 / (long_window + 1),
                                                             ignore_na=True).mean()
            self_close['EMA_short'] = self_close['close'].ewm(adjust=False, alpha=2 / (short_window + 1),
                                                              ignore_na=True).mean()
            self_close['diff'] = self_close['EMA_short'] - self_close['EMA_long']
            self_close['dea'] = self_close['diff'].ewm(adjust=False, alpha=2 / (diff_window + 1), ignore_na=True).mean()
            self_close['macd'] = 2 * (self_close['diff'] - self_close['dea'])
            self_close = self_close.dropna()
            history_number = self_close['diff'].values[-1]
            temp_self = pd.DataFrame(self_close['diff']).sort_values('diff', ascending=True).values
            n = np.where(temp_self == history_number)
            history_quantile = n[0][0] / len(temp_self)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 33
class ComputationExcessRet(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '超额收益'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        rolling_window = 250
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_ret = pd.DataFrame(cache[stock_code]['close'].pct_change().dropna())
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_ret = pd.DataFrame(fetched_data['close'].pct_change().dropna())
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_ret[1 + self_ret < 0] = -0.999
            self_ret = np.log(1 + self_ret) - np.log(1 + 0.02 / 250)
            lag = 21
            rstr_all = []
            if len(self_ret) > lag + rolling_window:
                for i in np.arange(lag + rolling_window, len(self_ret)):
                    rstr = self_ret.iloc[i - lag - rolling_window:i - lag + 1].ewm(halflife=126).mean().values[-1][0]
                    rstr_all.append(rstr)
                history_number = rstr_all[-1]
                temp_self = np.sort(rstr_all)
                n = np.where(temp_self == history_number)
                history_quantile = n[0][0] / len(temp_self)
            else:
                try:
                    rstr = self_ret.ewm(halflife=126).mean().values[-1][0]
                    history_number = rstr
                    history_quantile = 1
                except IndexError:
                    history_number = np.nan
                    history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 34
class ComputationCapmAlpha(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'CAPM回归截距项'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(code) is not None:
            return self.cache_result.get(code)
        # private vars
        # using cache for faster computation. This requires these private vars exactly the same as other CAPM models
        benchmark = 'EHSI'
        rolling_window = 250
        try:
            if cache is not None and cache.get(code) is not None and len(cache[code]) > 0:
                stock_data = pd.DataFrame(cache[code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[code] = fetched_data
            if cache is not None and cache.get(benchmark) is not None and len(cache[benchmark]) > 0:
                benchmark_data = pd.DataFrame(cache[benchmark])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))
                benchmark_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[benchmark] = fetched_data
            table = pd.concat([stock_data.close.pct_change(), benchmark_data.close.pct_change()], axis=1).dropna()
            table.columns = [code, benchmark]
            if len(table) == 0:
                #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
                #             code + '] at end_date:[' + end_date + '] with Exception message:' + "table len is 0")
                self.cache_result[code] = [np.nan, 1]
                return [np.nan, 1]
            if len(table) >= rolling_window:
                # try cache before computation
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((len(table) - rolling_window + 1, 3))
                    for i in range(0, len(matrix)):
                        x = table.iloc[i:i + rolling_window, 1].values
                        y = table.iloc[i:i + rolling_window, 0].values
                        A = np.vstack([x, np.ones(len(x))]).T
                        beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                        vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                        matrix[i, 0] = alpha * 250
                        matrix[i, 1] = beta
                        matrix[i, 2] = vol
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                # after computation process
                temp = np.sort(matrix[-2500:, 0])
                n = np.where(temp == matrix[-1, 0])
                history_result = matrix[-1, 0]
                history_quantile = n[0][0] / len(temp)
                self.cache_result[code] = [history_result, history_quantile]
                return [history_result, history_quantile]
            else:
                # try cache before computation
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((1, 3))
                    x = table.iloc[:, 0].values
                    y = table.iloc[:, 1].values
                    A = np.vstack([x, np.ones(len(x))]).T
                    beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                    vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                    matrix[0, 0] = alpha * 250
                    matrix[0, 1] = beta
                    matrix[0, 2] = vol
                    # caching matrix
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                history_result = matrix[-1, 0]
                self.cache_result[code] = [history_result, 1]
                return [history_result, 1]
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[code] = [np.nan, 1]
            return [np.nan, 1]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 35
class ComputationPriceReversal(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '价格反转'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        rolling_window = 14
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_ret = pd.DataFrame(cache[stock_code]['close'].pct_change().dropna())
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_ret = pd.DataFrame(fetched_data['close'].pct_change().dropna())
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_up = self_ret.copy()
            self_down = self_ret.copy()
            self_up[self_up < 0] = 0
            self_down[self_down > 0] = 0
            self_up = self_up.rolling(min(rolling_window, len(self_ret))).sum().dropna()
            self_down = self_down.rolling(min(rolling_window, len(self_ret))).sum().abs().dropna()
            rsi = self_up / (self_up + self_down)
            history_number = rsi.values[-1][0]
            temp_self = np.sort(rsi.values.reshape(len(rsi), ))
            n = np.where(temp_self == history_number)
            history_quantile = 1 - n[0][0] / len(temp_self)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 36
class ComputationCapmVol(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'CAPM残差波动率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(code) is not None:
            return self.cache_result.get(code)
        # private vars
        # using cache for faster computation. This requires these private vars exactly the same as other CAPM models
        benchmark = 'EHSI'
        rolling_window = 250
        try:
            if cache is not None and cache.get(code) is not None and len(cache[code]) > 0:
                stock_data = pd.DataFrame(cache[code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[code] = fetched_data
            if cache is not None and cache.get(benchmark) is not None and len(cache[benchmark]) > 0:
                benchmark_data = pd.DataFrame(cache[benchmark])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))
                benchmark_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[benchmark] = fetched_data
            table = pd.concat([stock_data.close.pct_change(), benchmark_data.close.pct_change()], axis=1).dropna()
            table.columns = [code, benchmark]
            if len(table) == 0:
                #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
                #             code + '] at end_date:[' + end_date + '] with Exception message:' + "table len is 0")
                self.cache_result[code] = [np.nan, 1]
                return [np.nan, 1]
            # compute with model
            if len(table) >= rolling_window:
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((len(table) - rolling_window + 1, 3))
                    for i in range(0, len(matrix)):
                        x = table.iloc[i:i + rolling_window, 1].values
                        y = table.iloc[i:i + rolling_window, 0].values
                        A = np.vstack([x, np.ones(len(x))]).T
                        beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                        vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                        matrix[i, 0] = alpha * 250
                        matrix[i, 1] = beta
                        matrix[i, 2] = vol
                    # caching matrix
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                temp = np.sort(matrix[-2500:, 2])
                n = np.where(temp == matrix[-1, 2])
                history_result = matrix[-1, 2]
                history_quantile = 1 - n[0][0] / len(temp)
                self.cache_result[code] = [history_result, history_quantile]
                return [history_result, history_quantile]
            else:
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((1, 3))
                    x = table.iloc[:, 0].values
                    y = table.iloc[:, 1].values
                    A = np.vstack([x, np.ones(len(x))]).T
                    beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                    vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                    matrix[0, 0] = alpha * 250
                    matrix[0, 1] = beta
                    matrix[0, 2] = vol
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                history_result = matrix[-1, 2]
                self.cache_result[code] = [history_result, 1]
                return [history_result, 1]
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[code] = [np.nan, 1]
            return [np.nan, 1]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 37
class ComputationExcessRetVol(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '日超额收益波动率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        benchmark = 'EHSI'
        rolling_window = 250
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_ret = pd.DataFrame(cache[stock_code]['close'].pct_change()).dropna()
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_ret = pd.DataFrame(fetched_data['close'].pct_change()).dropna()
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data

            if cache is not None and cache.get(benchmark) is not None and len(cache[benchmark]) > 0:
                rf = pd.DataFrame(cache[benchmark]['close'].pct_change()).dropna()
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))
                rf = pd.DataFrame(fetched_data['close'].pct_change()).dropna()
                # cache again
                if cache is not None:
                    cache[benchmark] = fetched_data
            temp = pd.concat([self_ret, rf], axis=1)
            temp = temp.dropna()
            temp.columns = ['close', 'rm']
            self_ret = temp['close'] - temp['rm']
            self_ret = self_ret.rolling(min(rolling_window, len(self_ret))).std().dropna() * np.sqrt(245)
            history_number = self_ret.values[-1]
            temp_self = np.sort(self_ret.values)
            n = np.where(temp_self == history_number)
            history_quantile = 1 - n[0][0] / len(temp_self)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 38
class ComputationPriceAmplitude(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '价格振幅'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_ret = pd.DataFrame(cache[stock_code]['close'].pct_change().dropna())
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_ret = pd.DataFrame(fetched_data['close'].pct_change().dropna())
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_ret = np.log(1 + self_ret)
            zt = pd.DataFrame(columns=np.arange(12), index=self_ret.index)
            for i in zt.columns:
                zt[i] = self_ret.rolling((i + 1) * 21).sum() - np.log(1 + (i + 1) * 0.02 / 12)
            zt = zt.dropna()
            zt_max = zt.max(axis=1)
            zt_min = zt.min(axis=1)
            zt_max[1 + zt_max < 0] = -0.999
            zt_min[1 + zt_min < 0] = -0.999
            self_ret_ = np.log(1 + zt_max) - np.log(1 + zt_min)
            history_number = self_ret_.values[-1]
            temp_self = np.sort(self_ret_.values)
            n = np.where(temp_self == history_number)
            history_quantile = 1 - n[0][0] / len(temp_self)
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_number, history_quantile]
        return [history_number, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 39
class ComputationCapmBeta(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = 'CAPM贝塔'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(code) is not None:
            return self.cache_result.get(code)
        # private vars
        # using cache for faster computation. This requires these private vars exactly the same as other CAPM models
        benchmark = 'EHSI'
        rolling_window = 250
        try:
            if cache is not None and cache.get(code) is not None and len(cache[code]) > 0:
                stock_data = pd.DataFrame(cache[code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[code] = fetched_data
            if cache is not None and cache.get(benchmark) is not None and len(cache[benchmark]) > 0:
                benchmark_data = pd.DataFrame(cache[benchmark])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(benchmark, end_date))
                benchmark_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[benchmark] = fetched_data
            table = pd.concat([stock_data.close.pct_change(), benchmark_data.close.pct_change()], axis=1).dropna()
            table.columns = [code, benchmark]
            if len(table) == 0:
                #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
                #             code + '] at end_date:[' + end_date + '] with Exception message:' + 'table len is 0')
                self.cache_result[code] = [np.nan, 1]
                return [np.nan, 1]
            if len(table) >= rolling_window:
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((len(table) - rolling_window + 1, 3))
                    for i in range(0, len(matrix)):
                        x = table.iloc[i:i + rolling_window, 1].values
                        y = table.iloc[i:i + rolling_window, 0].values
                        A = np.vstack([x, np.ones(len(x))]).T
                        beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                        vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                        matrix[i, 0] = alpha * 250
                        matrix[i, 1] = beta
                        matrix[i, 2] = vol
                    # caching matrix
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                temp = np.sort(matrix[-2500:, 1])
                n = np.where(temp == matrix[-1, 1])
                history_result = matrix[-1, 1]
                history_quantile = 1 - n[0][0] / len(temp)
                self.cache_result[code] = [history_result, history_quantile]
                return [history_result, history_quantile]
            else:
                if cache is not None and cache.get(code + "capm_compute_cache") is not None:
                    matrix = cache.get(code + "capm_compute_cache")
                else:
                    matrix = np.zeros((1, 3))
                    x = table.iloc[:, 0].values
                    y = table.iloc[:, 1].values
                    A = np.vstack([x, np.ones(len(x))]).T
                    beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
                    vol = np.std((y - (alpha + beta * x))) * np.sqrt(250)
                    matrix[0, 0] = alpha * 250
                    matrix[0, 1] = beta
                    matrix[0, 2] = vol
                    # caching matrix
                    if cache is not None:
                        cache[code + "capm_compute_cache"] = matrix
                history_result = matrix[-1, 1]
                self.cache_result[code] = [history_result, 1]
                return [history_result, 1]
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[code] = [np.nan, 1]
            return [np.nan, 1]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 40
class ComputationTurnover1Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '1月度平均换手率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        rolling_window = 1
        try:
            result = pd.read_sql(
                "SELECT IssuedCapital, DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            result['DateofChange'] = pd.to_datetime(result['DateofChange'])

            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_vol = pd.DataFrame(cache[stock_code]['volume'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_vol = pd.DataFrame(fetched_data['volume'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_vol['DateofChange'] = self_vol.index.values
            self_data = pd.merge(self_vol, result, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['IssuedCapital'] = self_data['IssuedCapital'].apply(lambda x: float(x))
            self_data['result'] = (self_data['volume'] / self_data['IssuedCapital'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            if len(result) >= (rolling_window + 1) * 21:
                result = np.log(result.rolling(21).sum()).dropna()
                length = int(np.floor(len(result) / 21))
                result = np.array(result[-length * 21:])
                result = np.reshape(result, [length, 21])
                result = pd.DataFrame(
                    np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[
                         :, 20].values
                history_result = result[-1]
                temp_self = np.sort(result)
                n = np.where(temp_self == history_result)
                history_quantile = 1 - n[0][0] / len(temp_self)
            else:
                history_result = np.nan
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 41
class ComputationVolume1Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '1月度平均成交量'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        window = 1
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                stock_data = pd.DataFrame(cache[stock_code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            if len(stock_data) >= window * 21:
                tmp_rst = stock_data['volume'].rolling(window * 21).mean().tail(len(stock_data) - window * 21 + 1)
                history_result = tmp_rst.values[-1]
                single_stock_sort = np.sort(tmp_rst.values)
                n = np.where(single_stock_sort == history_result)
                history_quantile = 1 - n[0][0] / len(single_stock_sort)
            else:
                tmp_rst = stock_data['volume'].mean()
                history_result = tmp_rst
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 42
class ComputationTurnover3Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '3月度平均换手率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        rolling_window = 3
        try:
            result = pd.read_sql(
                "SELECT IssuedCapital, DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            result['DateofChange'] = pd.to_datetime(result['DateofChange'])

            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_vol = pd.DataFrame(cache[stock_code]['volume'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_vol = pd.DataFrame(fetched_data['volume'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_vol['DateofChange'] = self_vol.index.values
            self_data = pd.merge(self_vol, result, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['IssuedCapital'] = self_data['IssuedCapital'].apply(lambda x: float(x))
            self_data['result'] = (self_data['volume'] / self_data['IssuedCapital'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            if len(result) >= (rolling_window + 1) * 21:
                result = np.log(result.rolling(21).sum()).dropna()
                length = int(np.floor(len(result) / 21))
                result = np.array(result[-length * 21:])
                result = np.reshape(result, [length, 21])
                result = pd.DataFrame(
                    np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[
                         :, 20].values
                history_result = result[-1]
                temp_self = np.sort(result)
                n = np.where(temp_self == history_result)
                history_quantile = 1 - n[0][0] / len(temp_self)
            else:
                history_result = np.nan
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 43
class ComputationVolume3Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '3月度平均成交量'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        window = 3
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                stock_data = pd.DataFrame(cache[stock_code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            if len(stock_data) >= window * 21:
                tmp_rst = stock_data['volume'].rolling(window * 21).mean().tail(len(stock_data) - window * 21 + 1)
                history_result = tmp_rst.values[-1]
                single_stock_sort = np.sort(tmp_rst.values)
                n = np.where(single_stock_sort == history_result)
                history_quantile = 1 - n[0][0] / len(single_stock_sort)
            else:
                tmp_rst = stock_data['volume'].mean()
                history_result = tmp_rst
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 44
class ComputationTurnover12Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '12月度平均换手率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        rolling_window = 12
        try:
            result = pd.read_sql(
                "SELECT IssuedCapital, DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                    stock_code + ' order by DateofChange'), con=self.db_con)
            result['DateofChange'] = pd.to_datetime(result['DateofChange'])
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                self_vol = pd.DataFrame(cache[stock_code]['volume'])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                self_vol = pd.DataFrame(fetched_data['volume'])
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            self_vol['DateofChange'] = self_vol.index.values
            self_data = pd.merge(self_vol, result, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['IssuedCapital'] = self_data['IssuedCapital'].apply(lambda x: float(x))
            self_data['result'] = (self_data['volume'] / self_data['IssuedCapital'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
            if len(result) >= (rolling_window + 1) * 21:
                result = np.log(result.rolling(21).sum()).dropna()
                length = int(np.floor(len(result) / 21))
                result = np.array(result[-length * 21:])
                result = np.reshape(result, [length, 21])
                result = pd.DataFrame(
                    np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[
                         :, 20].values
                history_result = result[-1]
                temp_self = np.sort(result)
                n = np.where(temp_self == history_result)
                history_quantile = 1 - n[0][0] / len(temp_self)
            else:
                history_result = np.nan
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

# 45
class ComputationVolume12Month(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '12月度平均成交量'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, cache=None):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        # private vars
        window = 12
        try:
            if cache is not None and cache.get(stock_code) is not None and len(cache[stock_code]) > 0:
                stock_data = pd.DataFrame(cache[stock_code])
            else:
                # prevent case with fail cache
                fetched_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
                stock_data = pd.DataFrame(fetched_data)
                # cache again
                if cache is not None:
                    cache[stock_code] = fetched_data
            if len(stock_data) >= window * 21:
                tmp_rst = stock_data['volume'].rolling(window * 21).mean().tail(len(stock_data) - window * 21 + 1)
                history_result = tmp_rst.values[-1]
                single_stock_sort = np.sort(tmp_rst.values)
                n = np.where(single_stock_sort == history_result)
                history_quantile = 1 - n[0][0] / len(single_stock_sort)
            else:
                tmp_rst = stock_data['volume'].mean()
                history_result = tmp_rst
                history_quantile = 1
        except Exception as e:
            #logger.error('fail to compute ' + self.computation_model_name + ' for stock_code:[' +
            #             stock_code + '] at end_date:[' + end_date + '] with Exception message:' + str(e))
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

class ComputationVolume(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '平均成交量'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, window):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        stock_data = self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))
        if len(stock_data) >= window * 21:
            tmp_rst = stock_data['volume'].rolling(window * 21).mean().tail(len(stock_data) - window * 21 + 1)
            history_result = tmp_rst.values[-1]
            single_stock_sort = np.sort(tmp_rst.values)
            n = np.where(single_stock_sort == history_result)
            history_quantile = 1 - n[0][0] / len(single_stock_sort)
        else:
            try:
                tmp_rst = stock_data['volume'].mean()
                history_result = tmp_rst
                history_quantile = 1
            except IndexError:
                history_result = np.nan
                history_quantile = 1
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=str(rolling_window) + "月度" + self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

class ComputationTurnover(BaseComputation):

    def __init__(self):
        super().__init__()
        self.computation_model_name = '平均换手率'
        self.fetch_industry_data()
        self.cache_result = dict()

    def compute_factor(self, stock_code, end_date, rolling_window):
        # get cache result
        if self.cache_result.get(stock_code) is not None:
            return self.cache_result.get(stock_code)
        result = pd.read_sql(
            "SELECT IssuedCapital, DateofChange from HK_STK_ChangeOfShares where Symbol = " + str(
                stock_code + ' order by DateofChange'), con=self.db_con)
        result['DateofChange'] = pd.to_datetime(result['DateofChange'])
        self_vol = pd.DataFrame(
            self.pre_process_fetched_stock_data(self.fetch_stock_data(stock_code, end_date))['volume'])
        try:
            self_vol['DateofChange'] = self_vol.index.values
            self_data = pd.merge(self_vol, result, on='DateofChange', how='outer').fillna(method='ffill').fillna(
                method='bfill')
            self_data['IssuedCapital'] = self_data['IssuedCapital'].apply(lambda x: float(x))
            self_data['result'] = (self_data['volume'] / self_data['IssuedCapital'])
            result = pd.DataFrame(self_data['result'])
            result = result.dropna(axis=0)
        except (ValueError, IndexError, TypeError):
            self.cache_result[stock_code] = [np.nan, 1]
            return [np.nan, 1]
        if len(result) >= (rolling_window + 1) * 21:
            result = np.log(result.rolling(21).sum()).dropna()
            length = int(np.floor(len(result) / 21))
            result = np.array(result[-length * 21:])
            result = np.reshape(result, [length, 21])
            result = pd.DataFrame(np.log((np.exp(pd.DataFrame(result))).rolling(rolling_window).mean()).dropna()).iloc[
                     :, 20].values
            history_result = result[-1]
            temp_self = np.sort(result)
            n = np.where(temp_self == history_result)
            history_quantile = 1 - n[0][0] / len(temp_self)
        else:
            history_result = np.nan
            history_quantile = 1
        self.cache_result[stock_code] = [history_result, history_quantile]
        return [history_result, history_quantile]

    #def save_to_db(self, stock_code, date, is_warning, history_result, history_quantile, industry_quantile,
    #               rolling_window):
    #    models.RiskIndexHistory.objects.create(
    #        risk_index=models.RiskIndex.objects.get(
    #            category=models.RiskCategoryEnum.QUANTIZED,
    #            detail_category=str(rolling_window) + "月度" + self.computation_model_name
    #        ),
    #        value=None if history_result in [np.nan, np.inf, -np.inf] else history_result,
    #        history_quantile=round(history_quantile, 4),
    #        industry_quantile=round(industry_quantile, 4),
    #        stock_id=stock_code,
    #        date=date,
    #        is_warning=is_warning
    #    )

#class ComputationHelper(BaseComputation):
#    def __init__(self):
#        super().__init__()
#        self.cache_of_fetched_data = dict()
#        self.fetch_industry_data()
#    def inject_stock_data_from_raw_csv(self):
#        raw_file = open('ind_jieli.csv', encoding='utf-8')
#        raw_data = raw_file.readlines()
#        results = []
#        for i, line in enumerate(raw_data):
#            if i > 0 and line[0].isdigit():
#                row = line.split(',')
                # # pre process data
#                raw_code = row[0].split('.')[0]
#                if len(raw_code) == 4:
#                    code = "0" + raw_code
#                elif len(raw_code) == 5:
#                    code = raw_code
#                else:
#                    raise Exception("Invalid raw stock code: " + raw_code)
#                print("injecting stock code: " + code)
#                name = row[1]
#                alias_name = row[2]
#                detailed_industry_category = row[4]
#                industry_category = row[5]
#                is_sh = 0
#                is_sz = 0
#                if code not in ['00724', '08078', '08158', '08186']:  # failed request code, need manually modify
#                    cur_response = self.fetch_stock_data_raw_response(
#                        code, str(datetime.now()).split(' ')[0].replace("-", ""))
#                    is_sh = int(cur_response['data']['hgt'])
#                    is_sz = int(cur_response['data']['sgt'])
#                results.append(models.Stock(
#                    code=code,
#                    name=name,
#                    alias_name=alias_name,
#                    industry_category=industry_category,
#                    detailed_industry_category=detailed_industry_category,
#                    is_shanghai_hongkong_connected=is_sh,
#                    is_shenzhen_hongkong_connected=is_sz
#                ))
#        models.Stock.objects.bulk_create(results)
#    @staticmethod
#    def bulk_compute_by_category(cur_computation_model, cur_category, cur_all_stocks, cur_date,
#                                 cur_rsts_indx_htry: dict,
#                                 cur_rsts_bchmrk: dict, cur_cache=None):
#        compute_model = cur_computation_model
#        rst_benchmark = None
#        cur_risk_index = models.RiskIndex.objects.get(detail_category=compute_model.computation_model_name)
#        print("computing | " + cur_category + " | " + compute_model.computation_model_name + " | " + cur_date)
#        for i, cur_stock in enumerate(cur_all_stocks):
#            stock_code = cur_stock.code
#            result_single = compute_model.compute_factor(stock_code, cur_date, cache=cur_cache)
#            result_peers = compute_model.compute_factor_in_peer(stock_code, cur_date, cache=cur_cache)
#            rst_history_quantile = round(result_single[1], 4)
#            rst_industry_quantile = round(result_peers[1], 4)
#            rst_benchmark = result_peers[0]
            # print(result_single[0], result_single[1], result_peers[0], result_peers[1])
#            atomic_htry_rst = {
#                'val': None if np.isnan(result_single[0]) or np.isinf(result_single[0]) else np.str(
#                    round(result_single[0], 4)),
#                'is_wr': True if rst_history_quantile <= 0.2 or rst_industry_quantile <= 0.2 else False,
#                'hq': rst_history_quantile,
#                'iq': rst_industry_quantile
#            }
            # save to dict
#            if cur_rsts_indx_htry.get(cur_stock.id) is None:
#                cur_rsts_indx_htry[cur_stock.id] = dict()
#            cur_rsts_indx_htry[cur_stock.id][cur_risk_index.code] = json.dumps(atomic_htry_rst)
        # need to insert once, save to dict
#        if cur_rsts_bchmrk.get(cur_category) is None:
#            cur_rsts_bchmrk[cur_category] = dict()
#        cur_rsts_bchmrk[cur_category][cur_risk_index.code] = None if np.isnan(rst_benchmark) or np.isinf(
#            rst_benchmark) else np.str(round(rst_benchmark, 4))
#    def do_cache_fetched_data(self, stock_codes: [str], end_date: str):
#        if stock_codes is None or len(stock_codes) == 0:
#            return None
#        print("fetching prices for " + str(stock_codes) + " at " + end_date)
#        for i, code in enumerate(stock_codes):
#            self.cache_of_fetched_data[code] = self.pre_process_fetched_stock_data(
#                self.fetch_stock_data(code, end_date, is_dev_mode=True))


if __name__ == '__main__':
    # dev()
   pass
    # # ------------------------------------------------------------------------------------------------------------
   #compute_model = BaseComputation()
   #print("0 testing", compute_model.computation_model_name)
   #rst = compute_model.fetch_stock_data('00700', '201906015')
   #print(len(rst))
    #
   compute_model = ComputationCapmAlpha()
   print("1 testing", compute_model.computation_model_name)
   this_result = compute_model.compute_factor('E00700', '20190620')
   print(this_result)
   expected_result = [-0.05756438695470142, 0.024891774891774892]
   print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', '20190403')
    # expected_result = [-0.07410351279259102, 0.5154639175257731]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # #
    # compute_model = ComputationCapmBeta()
    # print("2 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', 'EHSI', '20190403', 250)
    # expected_result = [1.568246917479231, 0.15097402597402598]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', 'EHSI', '20190403', 250)
    # expected_result = [0.438132168196138, 0.030927835051546393]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationCapmVol()
    # print("3 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', 'EHSI', '20190403', 250)
    # expected_result = [0.19083534603416955, 0.6130952380952381]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', 'EHSI', '20190403', 250)
    # expected_result = [0.5967731497491346, 0.9072164948453608]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationExcessRet()
    # print("4 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', '20190403', 250)
    # expected_result = [-0.0001499084412990167, 0.14660831509846828]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', '20190403', 250)
    # expected_result = [-0.0013073520589786019, 0.6701030927835051]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationExcessRetVol()
    # print("5 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00591', 'EHSI', '20190403', 250)
    # expected_result = [0.21570793041058425, 0.5638528138528138]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00591', 'EHSI', '20190403', 250)
    # expected_result = [0.6620409699486418, 1.0]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPriceAmplitude()
    # print("6 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00100', '20190403')
    # expected_result = [0.4443595478894872, 0.5160382667416995]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00018', '20190403')
    # expected_result = [0.7997956236137023, 0.8977272727272727]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationTurnover()
    # print("7 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', '20190403', 3)
    # expected_result = [-3.2025064677422823, 0.71875]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', '20190403', 3)
    # expected_result = [-3.8498188136792155, 0.29824561403508776]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationVolume()
    # print("8 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E01917', '20190403', 1)
    # expected_result = [61902337.5, 1]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E01917', '20190403', 1)
    # expected_result = [1063810.0, 0.04123711340206182]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPriceReversal()
    # print("9 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', '20190403', 14)
    # expected_result = [0.6653318999466219, 0.27564717162032604]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', '20190403', 14)
    # expected_result = [0.5018137942170781, 0.14432989690721654]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPriceTrend()
    # print("10 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('00439', '20190403')
    # expected_result = [5.128042103072573, 0.8876190476190476]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('00439', '20190403')
    # expected_result = [-0.0009154814858250426, 0.9896907216494846]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPB()
    # print("11 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190403')
    # expected_result = [0.9558294058935681, 0.4638109305760709]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190403')
    # expected_result = [0.7988148324906911, 0.28]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPS()
    # print("12 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02319', '20190418')
    # expected_result = [1.3697163851726544, 0.1878378378378378]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02319', '20190418')
    # expected_result = [1.6573497910396386, 0.65]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationPE()
    # print("13 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02319', '20190418')
    # expected_result = [32.447466007416566, 0.3564356435643564]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('08510', '20140101')
    # expected_result = [8.164078056551176, 0.1558441558441559]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationEvEbitda()
    # print("14 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [np.nan, 1]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [37.82135999097042, 1]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationCashFlowRatio()
    # print("15 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190418')
    # expected_result = [-5.616014840006184, 0.16666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190418')
    # expected_result = [0.2502847727514471, 0.025]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationDCR()
    # print("16 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02319', '20190418')
    # expected_result = [0.04693771899816814, 0.6666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02319', '20190418')
    # expected_result = [-0.004853916750544704, 0.6710526315789473]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationOperationCashFlow()
    # print("17 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190403')
    # expected_result = [-36330000.0, 0.0]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190403')
    # expected_result = [-5002.5, 0.1]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationSaleCash()
    # print("18 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190403')
    # expected_result = [-0.15678648554871386, 0.16666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190403')
    # expected_result = [-0.4394179460018315, 0.575]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNetProfitPctchg()
    # print("19 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190423')
    # expected_result = [0.33776222165933634, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190423')
    # expected_result = [0.2038126100890717, 0.5833333333333334]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationSalesIncomePctchg()
    # print("20 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [0.16749115062715747, 0.25]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [0.12854901966363286, 0.6666666666666666]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationTotalAssetPctchg()
    # print("21 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [0.1459756722545147, 0.1111111111111111]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [0.1459756722545147, 0.46153846153846156]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNetCashFlowPctchg()
    # print("22 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [0.0, 0.3]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [0.0, 0.07142857142857142]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationEquityPctchg()
    # print("23 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [0.21201559490816857, 0.5555555555555556]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [0.08116341537907323, 0.6153846153846154]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputeNetCashFlowPctchg()
    # print("24 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [0.0, 0.3]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [0.0, 0.07142857142857142]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationROIC()
    # print("25 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [5.0449, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [2.3529, 0.9230769230769231]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationROA()
    # print("26 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [1.6958, 0.8333333333333334]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [1.5348, 0.6923076923076923]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationROE()
    # print("27 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190403')
    # expected_result = [22.5152, 0.8333333333333334]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190403')
    # expected_result = [11.5722, 0.9230769230769231]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationOperatingProfitMargin()
    # print("28 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [38.3756, 0.8333333333333334]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [46.0269, 0.3]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNetProfitMargin()
    # print("29 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190423')
    # expected_result = [9.8899, 0.6666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190423')
    # expected_result = [4.0885, 0.6923076923076923]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationCashRatio()
    # print("30 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [0.22095442459984016, 0.0]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [0.08230894270096129, 0.925]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationCurrentRatio()
    # print("31 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [2.0, 0.16666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [1.3944999999999999, 0.8]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationDebtAssetRatio()
    # print("32 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02319', '20190423')
    # expected_result = [54.559, 0.8333333333333334]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02319', '20190423')
    # expected_result = [45.8005, 0.6883116883116883]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationGrossProfitMargin()
    # print("33 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02319', '20190423')
    # expected_result = [5.8509, 0.6666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02319', '20190423')
    # expected_result = [5.5947, 0.5263157894736842]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNetProfitMargin()
    # print("34 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E02318', '20190423')
    # expected_result = [9.8899, 0.6666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E02318', '20190423')
    # expected_result = [4.0885, 0.6923076923076923]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationQuickRatio()
    # print("35 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [1.7, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [1.0765, 0.825]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)

    # compute_model = ComputationMarketLeverageRatio()
    # print("36 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E01889', '20190423')
    # expected_result = [1.7181069675667424, 0.7846371347785108]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E01889', '20190423')
    # expected_result = [11.73778923605394, 0.08]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)

    # compute_model = ComputationBookLeverageRatio()
    # print("37 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [12.691741877983173, 0.33333333333333337]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [12.199805392472017, 0.6]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNonCurrentAssetTurnoverRatio()
    # print("38 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [0.031057476041462938, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [0.03849779791759473, 0.2]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationAssetTurnoverRatio()
    # print("39 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [0.01191849965052515, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [0.01913453839641844, 0.1]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # #
    # compute_model = ComputationCurrentAssetTurnoverRatio()
    # print("40 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [0.18722941729150164, 0.5]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [0.17741024038852926, 0.6]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationInventoryTurnoverRatio()
    # print("41 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00700', '20190423')
    # expected_result = [962.1112, 0.6666666666666666]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00700', '20190423')
    # expected_result = [47.735, 0.9090909090909091]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationNonCurrentAssetRatio()
    # print("42 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E00005', '20190423')
    # expected_result = [0.38375622135596243, 0.8333333333333334]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E00005', '20190423')
    # expected_result = [0.4274854124773395, 0.375]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #
    # compute_model = ComputationGrossProfitMargin()
    # print("43 testing", compute_model.computation_model_name)
    # this_result = compute_model.compute_factor('E03335', '20190423')
    # expected_result = [0.0, 0.0]
    # print("test result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    # this_result = compute_model.compute_factor_in_peer('E03335', '20190423')
    # expected_result = [0.0, 0.0]
    # print("test peer result", expected_result == this_result)
    # if expected_result != this_result:
    #     print(this_result)
    #print("test ends!")
