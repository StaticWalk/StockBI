import pandas as pd
import time
import talib as ta
import numpy as np
from common.Talib import *
from datetime import datetime, date, timedelta
from common.Common import StockData
from engine.StockMongoDbEngine import StockMongoDbEngine


class HighTurn(object):

    """
    高换手：理论上，低位高换手率时可买入，高位高换手时应卖出。

    """

    def __init__(self,logThread):
        self.logThread = logThread

        self.mongoDbEngine = StockMongoDbEngine(self.logThread)

        self.colNames = ['代码', '名称', '排名', '换手率(%)', '成交额(亿)', '流通股本(亿股)', '昨日换手率(%)']

        # 基准日期和候选股票数量
        self.baseDate = datetime.today().strftime("%Y-%m-%d")
        self.candidateCount = 100

        self.codeTable = {}
        self.codeDaysDf = {}

        self.tempData = {}
        self.result = []

    def getDate(self):
        return time.strftime("%Y-%m-%d", time.localtime())

    # 加载数据
    def load(self):
        data = self.mongoDbEngine.getStockCodes()
        for doc in data:
            code = doc['code']
            self.codeTable[code] = doc['name']
            temp = self.mongoDbEngine.getOneCodeDaysByRelative(code, StockData.dayIndicators, self.getDate(),-1)
            if temp is None:
                continue
            temp = temp.reindex(index=temp.index[::-1])
            self.codeDaysDf[code] = temp
        self.logThread.print("高换手数据加载完成")

    # 重新处理数据
    def processOnData(self):
        for code in self.codeTable:
            df = self.codeDaysDf.get(code)
            if df is None or len(df) < 2:
                continue
            else:
                turn = df.ix[-1, 'turn']
                amt = df.ix[-1, 'amt'] / 10 ** 8
                volume = df.ix[-1, 'volume']
                preTurn = df.ix[-2, 'turn']
                float = volume / turn * 100 / 10 ** 8
                self.tempData[code] = [self.codeTable[code], turn, amt, float, preTurn]
        self.logThread.print("高换手策略数据处理完成")

    # 执行策略选股
    def select(self):
        df = pd.DataFrame(self.tempData).T
        start = self.colNames.index('换手率(%)')
        df.rename(columns={i: x for i, x in enumerate(['名称'] + self.colNames[start:])}, inplace=True)

        series = df['换手率(%)'].rank(ascending=False)
        rankSeries = series

        series = df['成交额(亿)'].rank(ascending=False)
        rankSeries += series

        # 流通股本越大越好，这样对相对的换手率形成制约。盘子越大的股票，意味着大资金关注多，一般认为大资金是聪明钱。
        series = df['流通股本(亿股)'].rank(ascending=False)
        rankSeries += series

        rankSeries = rankSeries.rank()
        rankSeries.name = '排名'

        df = pd.concat([rankSeries, df], axis=1)
        df.sort_values('排名', ascending=True, inplace=True)

        if self.candidateCount > 0:
            df = df.ix[:self.candidateCount]

        df = df.reindex(columns=self.colNames[1:])
        df.reset_index(inplace=True)

        self.result = df.values.tolist()
        self.logThread.print("选股完成，输出候选股票信息")
        self.logThread.print(self.colNames)
        for i in self.result:
            self.logThread.print(i)


    def run(self):
        self.load()
        self.processOnData()
        self.select()



