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
    1.首先趋势判断，中证500趋势判断，
        日线级别kdj金叉多区间，
        上一交易日为大阴线3/5实体（并且50日atr大于2/3平均值），
        obos（超买超卖指标）小于-100，满足条件则选股
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
        self.df = {}
        self.codeDaysDf = {}

        # 设置基准日期、阴线实体比例、50日atr平均值、obos阈值
        self.baseDate = '2020-4-06'
        self.bodyRatio = 0.6
        self.atrOverMean = 0.67
        self.obos = -100

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
            temp = self.mongoDbEngine.getOneCodeDaysByRelative(code, self.indicators, self.getDate(), -64)
            if temp is None:
                continue
            temp = temp.reindex(index=temp.index[::-1])
            df[code] = temp['amt'][-1] / temp['turn'][-1]
            self.codeDaysDf[code] = temp

        sortedCodes = sorted(df, key=lambda k: df[k], reverse=True)

        self.df = sortedCodes[:500]
        print("kdj数据加载完成")

    def run(self):
        # 加载需要用到的etf500数据
        self.load()
        # 开盘前运行
        self.before_market_open()
        # 开盘时运行
        self.market_open()
        # 收盘后运行
        self.after_market_close()

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

        for code in self.df:
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
            self._canContinue = False


    def get_ATR(df,period):
        atr = ta.ATR(df['high'].values,df['low'].values,df['close'].values,timeperiod = period)
        atr_mean_list = []
        for _ in atr:
            if str(_) == 'nan':
                atr_mean_list.append(0)
            else:
                atr_mean_list.append(_)
        atr_mean = np.mean(atr_mean_list)
        return atr[-1],atr_mean

    # 开盘前运行函数
    def before_market_open(self):
        # 校验处理obos
        self.processOnObos()

        # 获取etf500的数据
        df = self.mongoDbEngine.getOneCodeDaysByRelative(StockData.etf500, self.indicators, self.getDate(), -66)
        df = df.reindex(index=df.index[::-1])
        # 日线级别kdj金叉多区间
        K, D, J = KDJ(df['high'].values, df['low'].values, df['close'].values, adjust=False)
        if K[-1] < D[-1]:
            print("etf500: KDJ不是金叉区间")
            return

        # 上一交易日为大阴线3/5实体
        if df['close'][-2] >= df['open'][-2]:
            print("etf500: 上一交易日不是阴线")
            return

        bodyRatio = abs(df['close'][-2] - df['open'][-2])/abs(df['low'][-2] - df['high'][-2])
        if bodyRatio < self.bodyRatio:
            print("etf500: 上一交易日阴线实体比例: {}".format(bodyRatio))
            return

        # 50日atr大于2/3平均值
        atr = np.array(ATR(df['high'].values, df['low'].values, df['close'].values))
        atrOverMean = float(atr[-1]/atr[-50:].mean())
        if atrOverMean <= self.atrOverMean:
            print("etf500: 50日atr/平均值: {}".format(atrOverMean))
            return



    # ## 开盘时运行函数
    # def market_open(context):
    #
    #     log.info('函数运行时间(market_open):'+str(context.current_dt.time()))
    #     if g.zs == 0:
    #         log.info('大盘择时条件不满足'+str(context.current_dt.time()))
    #         return
    #     if g.zs == 1:
    #         log.info('大盘择时条件满足开始选股'+str(context.current_dt.time()))
    #         g.oknum += 1
    #         trader(context)
    #         g.date_list.append(str(context.current_dt.date()))
    #         return
    # def trader(context):
    #     current_data = get_current_data()
    #     for _ in g.scu_list:
    #         dt1 = current_data[_]
    #         dt2 = get_price(_,end_date=context.previous_date,fields=['close'],count = 1)
    #         day_change = (dt1.day_open - dt2.close.values[-1]) / dt2.close.values[-1]#计算当日开盘涨幅
    #         #print(_,day_change,context.current_dt.time())
    #         if day_change >= 0.04 and day_change <= 0.06:
    #             tk = get_ticks(_,'%s 9:30:07'%(context.current_dt.date()),  count=2, fields=['time', 'current'])#获取tick数据
    #             if tk['current'][-1] > dt1.day_open:
    #                 #print(_,day_change,context.current_dt.time())
    #                 g.buy_list.append(_)
    # def after_market_close(context):
    #     if len(g.buy_list)>0:
    #         print(g.buy_list)
    #     if g.date_list:
    #         log.info(g.date_list)
    #     g.buy_list = []