import operator

from common.Common import *
import tushare as ts
import pandas as pd
from time import sleep


class StockDataEngine(object):

    def __init__(self, mongoDbEngine):
        self.tuSharePro = None
        self.mongoDbEngine = mongoDbEngine
        self._updatedCodeCount = 0  # 更新日线数据的计数器


    def startTuSharePro(self):
        if self.tuSharePro is None:
            ts.set_token(Config.tuShareProToken)
            self.tuSharePro = ts.pro_api()



    def getStockCodesFromTuSharePro(self):
        print("从TuSharePro获取股票代码表...")

        self.startTuSharePro()

        lastEx = None
        retry = 3
        for _ in range(retry):
            try:
                df = self.tuSharePro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
                data = df[['ts_code', 'name']].values.tolist()
                codes = {}
                for code, name in data:
                    codes[code] = name
                break
            except Exception as ex:
                lastEx = ex
                print("从TuSharePro获取股票代码表异常: {}, retrying...".format(ex))
                sleep(1)
        else:
            print("从TuSharePro获取股票代码表异常: {}, retried {} times".format(lastEx, retry))
            return None

        print("从TuSharePro获取股票代码表成功")
        return codes


    def updateOneCode(self, code, data):

        # get max date range
        startDate, endDate = None, None
        for _, dates in data.items():
            if startDate is None:
                startDate = dates[0]
                endDate = dates[-1]
            else:
                if operator.lt(dates[0], startDate):
                    startDate = dates[0]
                if operator.gt(dates[-1], endDate):
                    endDate = dates[-1]

        data = self.getIndexDaysFromTuSharePro(code, startDate, endDate, sorted(data))
        if not data: # None(errors) or no data
            if data is None: # indicate fetching data error from engine point of view
                print(" 获取{}日线数据[{}, {}]失败".format(code, startDate, endDate))

        # updat to DB
        if self.mongoDbEngine.updateDays(code, data):
            self._updatedCodeCount += 1 # 需要更新的股票（也就是在数据库里的数据不全），并且数据成功写入数据库

    def getIndexDaysFromTuSharePro(self, code, startDate, endDate, fields, name=None):
        """
            从TuSharePro获取指数日线数据
        """
        self.startTuSharePro()

        print("TuSharePro: {}, {} ~ {}".format(code, startDate, endDate))

        proStartDate = startDate.replace('-', '')
        proEndDate = endDate.replace('-', '')

        lastEx = None
        retry = 3
        for _ in range(retry):
            try:
                # ohlcv, amount
                dailyDf = self.tuSharePro.index_daily(ts_code=code, start_date=proStartDate, end_date=proEndDate)
                dailyDf = dailyDf.set_index('trade_date')
                dailyDf = dailyDf[['open', 'high', 'low', 'close', 'vol', 'amount']]
                dailyDf = dailyDf.dropna()
                dailyDf['vol'] *= 100
                dailyDf['amount'] *=1000
                dailyDf.index = pd.to_datetime(dailyDf.index, format='%Y%m%d')
                break
            except Exception as ex:
                lastEx = ex
                print("{}({})TuSharePro异常[{}, {}]: {}, retrying...".format(code, name, startDate, endDate, ex))
                sleep(1)
        else:
            print("{}({})TuSharePro异常[{}, {}]: {}, retried {} times".format(code, name, startDate, endDate, lastEx, retry))
            return None

        df = dailyDf

        # no turn and factor for index
        df['turnover_rate'] = 0
        df['adj_factor'] = 1

        # change to Wind's indicators
        df = df.sort_index()
        df.index.name = 'datetime'
        df.reset_index(inplace=True) # 把时间索引转成列
        df.rename(columns={'amount': 'amt', 'turnover_rate': 'turn', 'adj_factor': 'adjfactor', 'vol': 'volume'}, inplace=True)

        # select according @fields
        df = df[['datetime'] + fields]

        return None if df is None else list(df.T.to_dict().values())




    def getCodeDaysFromTuSharePro(self, code, startDate, endDate, fields, name=None):
        """
            从TuSharePro获取个股日线数据
        """
        self.startTuSharePro()

        print("TuSharePro: {}, {} ~ {}".format(code, startDate, endDate))

        proStartDate = startDate.replace('-', '')
        proEndDate = endDate.replace('-', '')

        lastEx = None
        retry = 3
        for _ in range(retry):
            try:
                #
                dailyDf = self.tuSharePro.daily(ts_code=code, start_date=proStartDate, end_date=proEndDate)
                dailyDf = dailyDf.set_index('trade_date')
                dailyDf = dailyDf[['open', 'high', 'low', 'close', 'vol', 'amount']]
                dailyDf = dailyDf.dropna()
                dailyDf['vol'] *= 100
                dailyDf['amount'] *=1000
                dailyDf.index = pd.to_datetime(dailyDf.index, format='%Y%m%d')

                # adj factor
                adjFactorDf = self.tuSharePro.adj_factor(ts_code=code, start_date=proStartDate, end_date=proEndDate)
                adjFactorDf = adjFactorDf.set_index('trade_date')
                adjFactorDf = adjFactorDf[['adj_factor']]
                adjFactorDf = adjFactorDf.dropna()
                adjFactorDf.index = pd.to_datetime(adjFactorDf.index, format='%Y%m%d')

                # turn
                dailyBasicDf = self.tuSharePro.daily_basic(ts_code=code, start_date=proStartDate, end_date=proEndDate)
                dailyBasicDf = dailyBasicDf.set_index('trade_date')
                dailyBasicDf = dailyBasicDf[['turnover_rate']]
                dailyBasicDf = dailyBasicDf.dropna()
                dailyBasicDf.index = pd.to_datetime(dailyBasicDf.index, format='%Y%m%d')
                break
            except Exception as ex:
                lastEx = ex
                print("{}({})TuSharePro异常[{}, {}]: {}, retrying...".format(code, name, startDate, endDate, ex))
                sleep(1)
        else:
            print("{}({})TuSharePro异常[{}, {}]: {}, retried {} times".format(code, name, startDate, endDate, lastEx, retry))
            return None

        # 清洗数据
        df = pd.concat([dailyDf, dailyBasicDf], axis=1)
        df = df[df['vol'] > 0] # 剔除停牌
        df = df.merge(adjFactorDf, how='left', left_index=True, right_index=True) # 以行情为基准
        if df.isnull().sum().sum() > 0:
            print("{}({})TuSharePro有些数据缺失[{}, {}]".format(code, name, startDate, endDate))
            print(df[df.isnull().any(axis=1)])

            print("{}({})TuSharePro有些数据缺失[{}, {}]".format(code, name, startDate, endDate))
            return None

        # change to Wind's indicators
        df = df.sort_index()
        df.index.name = 'datetime'
        df.reset_index(inplace=True) # 把时间索引转成列
        df.rename(columns={'amount': 'amt', 'turnover_rate': 'turn', 'adj_factor': 'adjfactor', 'vol': 'volume'}, inplace=True)

        # select according @fields
        df = df[['datetime'] + fields]
        return df


    def getTradeDaysFromTuSharePro(self, startDate, endDate):
        self.startTuSharePro()

        print("TuSharePro: 获取交易日数据[{} ~ {}]".format(startDate, endDate))

        proStartDate = startDate.replace('-', '')
        proEndDate = endDate.replace('-', '')

        lastEx = None
        retry = 3
        for _ in range(retry):
            try:
                df = self.tuSharePro.trade_cal(exchange='SSE', start_date=proStartDate, end_date=proEndDate)

                df = df.set_index('cal_date')
                df = df[proStartDate:proEndDate]
                dfDict = df.to_dict()

                # get trade days
                dates = Time.getDates(startDate, endDate, strFormat=True)
                tDays = []
                for date in dates:
                    if dfDict['is_open'][date.replace('-', '')] == 1:
                        tDays.append(date)

                return tDays
            except Exception as ex:
                lastEx = ex
                print("TuSharePro: 获取交易日数据[{} ~ {}]异常: {}, retrying...".format(startDate, endDate, ex))
                sleep(1)

        print(
            "TuSharePro: 获取交易日数据[{} ~ {}]异常: {}, retried {} times".format(startDate, endDate, lastEx, retry)
            )
        return None

