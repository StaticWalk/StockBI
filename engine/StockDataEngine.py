import operator

from common.Common import *
import tushare as ts
import pandas as pd
from time import sleep


class StockDataEngine(object):

    def __init__(self, mongoDbEngine,logThread):
        self.tuSharePro = None
        self.mongoDbEngine = mongoDbEngine
        self._updatedCodeCount = 0  # 更新日线数据的计数器
        self.logThread = logThread


    def startTuSharePro(self):
        if self.tuSharePro is None:
            ts.set_token(Config.tuShareProToken)
            self.tuSharePro = ts.pro_api()


    # 从tusharePro第三方获取数据
    def getStockCodesFromTuSharePro(self):
        self.logThread.print("从TuSharePro获取股票代码表...")

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
                self.logThread.print("从TuSharePro获取股票代码表异常: {}, retrying...".format(ex))
                sleep(1)
        else:
            self.logThread.print("从TuSharePro获取股票代码表异常: {}, retried {} times".format(lastEx, retry))
            return None

        self.logThread.print("从TuSharePro获取股票代码表成功")
        return codes

    # 单独更新每只股票
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

        # 区分股票代码，包括指数和基金
        if code in StockData.indexes:
            df = self.getIndexDaysFromTuSharePro(code, startDate, endDate, sorted(data))
        elif code in StockData.funds:
            df = self.getFundDaysFromTuShare(code, startDate, endDate, StockData.dayIndicators)
        else:
            df = self.getCodeDaysFromTuSharePro(code, startDate, endDate, StockData.dayIndicators)

        # 更新到数据库
        self.mongoDbEngine.updateDays(code, df)

    def getIndexDaysFromTuSharePro(self, code, startDate, endDate, fields, name=None):
        """
            从TuSharePro获取指数日线数据
        """
        self.startTuSharePro()

        self.logThread.print("TuSharePro指数日线数据: {}, {} ~ {}".format(code, startDate, endDate))

        proStartDate = startDate.replace('-', '')
        proEndDate = endDate.replace('-', '')

        lastEx = None
        retry = 20
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
                self.logThread.print("{}({})TuSharePro异常[{}, {}]: {}, retrying...".format(code, name, startDate, endDate, ex))
                sleep(3)
        else:
            self.logThread.print("{}({})TuSharePro异常[{}, {}]: {}, retried {} times".format(code, name, startDate, endDate, lastEx, retry))
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

    def getFundDaysFromTuShare(self, code, startDate, endDate, fields, name=None):
        """
            从tushare获取基金（ETF）日线数据。
            # !!!TuShare没有提供换手率，复权因子和成交额，所以只能假设。
            # 策略针对ETF的，需要注意。
        """
        tuShareCode = code[:-3]
        self.logThread.print("TuSharePro基金（ETF）日线数据: {}, {} ~ {}".format(code, startDate, endDate))
        sleepTime = 3
        try:
            try:
                # 以无复权方式从腾讯获取OHCLV，成交量是手（整数化过）
                # 此接口支持ETF日线数据
                df = ts.get_k_data(tuShareCode, startDate, endDate, autype=None, pause=sleepTime)
                if df is None or df.empty: # If no data, TuShare return None
                    df = pd.DataFrame(columns=['date', 'open', 'high', 'close', 'low', 'volume'])
                else:
                    df = df.sort_index()
            except Exception as ex:
                self.logThread.print("从TuShare获取{}({})日线数据[{}, {}]失败: {}".format(code, name, startDate, endDate, ex))
                return None

            df['volume'] = df['volume']*100

            # !!!TuShare没有提供换手率，复权因子和成交额，所以只能假设。
            # 策略针对ETF的，需要注意。
            df['turnover'] = 0
            df['factor'] = 1
            df['amount'] = 0
            df.index.name = None

            # change to Wind's indicators
            df.rename(columns={'date': 'datetime', 'amount': 'amt', 'turnover': 'turn', 'factor': 'adjfactor'}, inplace=True)

            # 把日期的HH:MM:SS转成 00:00:00
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')

            # select according @fields
            df = df[['datetime'] + fields]
        except Exception as ex:
            self.logThread.print("从TuShare获取{}({})日线数据[{}, {}]失败: {}".format(code, name, startDate, endDate, ex))
            return None

        return None if df is None else list(df.T.to_dict().values())




    def getCodeDaysFromTuSharePro(self, code, startDate, endDate, fields, name=None):
        """
            从TuSharePro获取个股日线数据
        """
        self.startTuSharePro()

        self.logThread.print("TuSharePro个股日线数据: {}, {} ~ {}".format(code, startDate, endDate))

        proStartDate = startDate.replace('-', '')
        proEndDate = endDate.replace('-', '')

        lastEx = None
        retry = 20
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
                self.logThread.print("{}({})TuSharePro异常[{}, {}]: {}, retrying...".format(code, name, startDate, endDate, ex))
                sleep(1)
        else:
            self.logThread.print("{}({})TuSharePro异常[{}, {}]: {}, retried {} times".format(code, name, startDate, endDate, lastEx, retry))
            return None

        # 清洗数据
        df = pd.concat([dailyDf, dailyBasicDf], axis=1)
        df = df[df['vol'] > 0] # 剔除停牌
        df = df.merge(adjFactorDf, how='left', left_index=True, right_index=True) # 以行情为基准
        if df.isnull().sum().sum() > 0:
            self.logThread.print("{}({})TuSharePro有些数据缺失[{}, {}]".format(code, name, startDate, endDate))
            print(df[df.isnull().any(axis=1)])

            self.logThread.print("{}({})TuSharePro有些数据缺失[{}, {}]".format(code, name, startDate, endDate))
            return None

        # change to Wind's indicators
        df = df.sort_index()
        df.index.name = 'datetime'
        df.reset_index(inplace=True) # 把时间索引转成列
        df.rename(columns={'amount': 'amt', 'turnover_rate': 'turn', 'adj_factor': 'adjfactor', 'vol': 'volume'}, inplace=True)

        # select according @fields
        df = df[['datetime'] + fields]

        return None if df is None else list(df.T.to_dict().values())



    def getTradeDaysFromTuSharePro(self, startDate, endDate):
        self.startTuSharePro()

        self.logThread.print("TuSharePro: 获取交易日数据[{} ~ {}]".format(startDate, endDate))

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
                self.logThread.print("TuSharePro: 获取交易日数据[{} ~ {}]异常: {}, retrying...".format(startDate, endDate, ex))
                sleep(1)

        self.logThread.print(
            "TuSharePro: 获取交易日数据[{} ~ {}]异常: {}, retried {} times".format(startDate, endDate, lastEx, retry)
            )
        return None

