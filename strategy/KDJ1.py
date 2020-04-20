import pandas as pd
import time
import talib as ta
import numpy as np
from common.Talib import *
from datetime import datetime, date, timedelta
from common.Common import StockData
from engine.StockMongoDbEngine import StockMongoDbEngine

class KDJ1(object):

    """
    https://www.joinquant.com/view/community/detail/04314dec73ac4a63b67ab6f8e8f57aae?type=2
    1.首先趋势判断，eft500趋势判断，
        日线级别kdj金叉多区间，
        上一交易日为大阴线3/5实体（并且50日atr大于2/3平均值），
        obos（超买超卖指标）小于-100，满足条件则选股 （obos通过选取的市值前五百的中小板和创业板股进行计算）
    2.选股 当日9：25-9：30之间高开五个点以上（包含），不含st，开盘前五秒上涨，选出并打印
    """

    # 初始化函数，设定基准等等
    def __init__(self):

        # 使用etf代替指数
        self.etfCode = StockData.zz500Index
        self.indicators = StockData.dayIndicators
        self.mongoDbEngine = StockMongoDbEngine()
        self.atr_window = 50  # atr窗口数量
        self.buy_list = []  # 满足买入条件的股票
        # self.tick_list = []  # 要获取tick的标的
        self.date_list = []
        self.sortDf = {}
        self.codeDaysDf = {}

        # 设置基准日期、阴线实体比例、50日atr平均值、obos阈值
        self.baseDate = '2020-4-06'
        self.bodyRatio = 0.6
        self.atrOverMean = 0.67
        self.obos = -100
        self.gap = -50-14
        self.result = []

        # 是否满足选股条件
        self.isOk = True

    def getDate(self):
        return time.strftime("%Y-%m-%d", time.localtime())

    def load(self):
        # 选取中小板和创业板股票，并获取交易信息
        codes = {}
        df = {}
        self.codeDaysDf = self.mongoDbEngine.getDaysLatestDate()
        data = self.mongoDbEngine.getStockCodes()
        for doc in data:
            codes[doc['code']] = doc['name']
        # 选取中小创流通市值前500只
        for code in codes:
            # 由于DY里没有中证500成分股信息，所以用中小创代替
            indexCode = StockData.getIndex(code)
            if indexCode not in [StockData.cybIndex, StockData.zxbIndex]:
                continue
            temp = self.mongoDbEngine.getOneCodeDaysByRelative(code, self.indicators, self.getDate(), self.gap)
            if temp is None:
                continue
            temp = temp.reindex(index=temp.index[::-1])
            df[code] = temp['amt'][-1] / temp['turn'][-1]
            self.codeDaysDf[code] = temp

        sortedCodes = sorted(df, key=lambda k: df[k], reverse=True)

        self.sortDf = sortedCodes[:500]
        print("kdj数据加载完成")

    def run(self):
        # 加载股票日线数据
        self.load()
        # 开盘前运行
        self.before_market_open()
        if self.isOk:
            # 开盘时运行
            self.market_open()

    # 计算obos指标
    def processOnObos(self):
        # 一般而言，OBOS指标参数选择的不同，其市场表现也迥然两异。
        # 参数选择的小，OBOS值上下变动的空间就比较大，曲线的起伏就比较剧烈;
        # 参数选择的小，OBOS值上下变动的空间就比较小，曲线的上下起伏就比较平稳。
        # 目前，市场上比较常用的参数是10、20等
        # http://www.xj315.com/gupiao/6968.html
        N = 10

        # date range cut
        df = self.codeDaysDf.get(StockData.paraCode)
        dateRange = df[:self.baseDate].tail(N + 1).index
        startDate, endDate = dateRange[0].strftime("%Y-%m-%d"), dateRange[-1].strftime("%Y-%m-%d")

        zeros = pd.Series([0] * N, index=df[:self.baseDate].tail(N).index)
        ups = zeros
        downs = zeros

        for code in self.sortDf:
            df = self.codeDaysDf.get(code)
            if df is None:
                continue
            closes = df['close'][startDate:endDate]
            pctChanges = closes.pct_change().dropna()

            # 上涨
            temp = zeros + pctChanges.apply(lambda x: 1 if x > 0 else 0)
            ups = ups + temp.fillna(0)

            # 下跌
            temp = zeros + pctChanges.apply(lambda x: 1 if x < 0 else 0)
            downs = downs + temp.fillna(0)

        obos = ups.sum() - downs.sum()
        if obos >= self.obos:
            print("中小板创业板obos: {}".format(obos))
        else:
            self.isOk = False

    # 开盘前运行函数
    def before_market_open(self):
        # 校验obos
        self.processOnObos()

        # 获取etf500的数据
        df = self.mongoDbEngine.getOneCodeDaysByRelative(StockData.etf500, self.indicators, self.getDate(), self.gap)
        df = df.reindex(index=df.index[::-1])
        # 日线级别kdj金叉多区间
        K, D, J = KDJ(df['high'].values, df['low'].values, df['close'].values, adjust=False)
        if K[-1] < D[-1]:
            print("etf500: KDJ不是金叉区间")
            self.isOk = False
            return

        # 上一交易日为大阴线3/5实体
        if df['close'][-2] >= df['open'][-2]:
            print("etf500: 上一交易日不是阴线")
            self.isOk = False
            return

        bodyRatio = abs(df['close'][-2] - df['open'][-2])/abs(df['low'][-2] - df['high'][-2])
        if bodyRatio < self.bodyRatio:
            print("etf500: 上一交易日阴线实体比例: {}".format(bodyRatio))
            self.isOk = False
            return

        # 50日atr大于2/3平均值
        atr = np.array(ATR(df['high'].values, df['low'].values, df['close'].values))
        atrOverMean = float(atr[-1]/atr[-50:].mean())
        if atrOverMean <= self.atrOverMean:
            print("etf500: 50日atr/平均值: {}".format(atrOverMean))
            self.isOk = False
            return

    #  开盘运行 选股函数
    def market_open(self):
        for code in self.sortDf:
            df = self.codeDaysDf.get(code)
            if df is None:
                continue

            # 选股9：25-9：30之间高开五个点以上（包含）
            # 运行前已更新为最新数据，所以选取的五百只股票中不会包含st
            openIncrease = (df['open'][-1] - df['close'][-2])/df['close'][-2] * 100
            if openIncrease < 5:
                continue

            # 设置结果
            row = [code, self.codeDaysDf[code]]
            print(code)
            self.result.append(row)

        if not len(self.result):
            print('选股结果为空，无满足条件的股票')
