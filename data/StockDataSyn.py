from time import sleep

from common.Common import  StockData
from data.StockCodeDataTable import StockCodeDataTable
from data.StockTradeDataTable import StockTradeDataTable
from engine.StockDataEngine import StockDataEngine
from engine.StockMongoDbEngine import StockMongoDbEngine


class StockDataSyn(object):
    """
        股票日线数据，从TuSharePro获取
        https://tushare.pro/
    """
    def __init__(self,logThread):
        self.logThread = logThread
        self.mongoDbEngine = StockMongoDbEngine(self.logThread)
        self.dataEngine = StockDataEngine(self.mongoDbEngine,self.logThread)
        self.tradeDataTable = StockTradeDataTable(self.mongoDbEngine, self.dataEngine,self.logThread)
        self.codeDataTable = StockCodeDataTable(self.mongoDbEngine, self.dataEngine,self.logThread)
        self.table = {}
        self.compactTable = []

    def updateMain(self, startDate, endDate):
        # 更新所有库存A股代码表
        if not self.codeDataTable.update() & self.tradeDataTable.update(startDate, endDate):
            return
        # 更新日线数据
        self.updateHistDays(startDate, endDate, StockData.dayIndicators, False, None)

    def updateHistDays(self, startDate, endDate, indicators, isForced=False, codes=None):
        codes = self.getUpdatedCodes(startDate, endDate, indicators, isForced, codes)
        if codes is None: return
        self.logThread.print("开始更新{0}只股票(指数,基金)的历史日线数据...".format(len(codes)))
        while codes:
            code = sorted(codes)[0]
            self.dataEngine.updateOneCode(code, codes[code])
            self.logThread.print("{}更新完成！".format(code))
            del codes[code]

    def getDaysNotInDb(self, tradeDays, codes, indicators):
        """ @tradeDays: [trade day]
            @codes: {code: name}
            @indicators: [indicator]
            @return: {code: {indicator: [trade day]}}
        """
        self.logThread.print('开始从数据库获取日线不存在的数据...')
        data = {}
        for code in codes:
            lastEx = None
            for _ in range(3):
                try:
                    temp = self.mongoDbEngine.getNotExistingDates(code, tradeDays, indicators)
                    break
                except Exception as ex:
                    sleep(1)
                    lastEx = ex
                    self.logThread.print("self._mongoDbEngine.getNotExistingDates异常: {}".format(ex))
            else:
                self.logThread.print("self._mongoDbEngine.getNotExistingDates异常: {}".format(lastEx))
                return None

            if temp:
                data[code] = temp

        return data if data else None

    # 获取要更新的股票代码和要更新的时间范围
    def getUpdatedCodes(self, startDate, endDate, indicators, isForced, codes=None):
        """
            @return: {code: {indicator: [trade day]}}
        """
        # 获取交易日范围
        tradeDays = self.tradeDataTable.get(startDate, endDate)

        # 获取股票代码，包括指数和基金
        codes = self.codeDataTable.stockAllCodesFunds if codes is None else codes

        # get not existing from DB
        if not isForced:
            codes = self.getDaysNotInDb(tradeDays, codes, indicators)
            if not codes:
                self.logThread.print("历史日线数据已经在数据库")
                return None
        else:
            newCodes = {}
            if tradeDays and indicators:
                for code in codes:
                    newCodes[code] = {}
                    for indicator in indicators:
                        newCodes[code][indicator] = tradeDays

            codes = newCodes
            if not codes:
                self.logThread.print("没有日线数据需要更新")
                return None

        return codes
