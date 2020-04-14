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
    def __init__(self):
        self.mongoDbEngine = StockMongoDbEngine()
        self.dataEngine = StockDataEngine(self.mongoDbEngine)
        self.tradeDataTable = StockTradeDataTable(self.mongoDbEngine, self.dataEngine)
        self.codeDataTable = StockCodeDataTable(self.mongoDbEngine, self.dataEngine)
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
        print("开始更新{0}只股票(指数,基金)的历史日线数据...".format(len(codes)))
        while codes:
            code = sorted(codes)[0]
            self.dataEngine.updateOneCode(code, codes[code])
            print("{}更新完成！".format(code))
            del codes[code]

    def getDaysNotInDb(self, tradeDays, codes, indicators):
        """ @tradeDays: [trade day]
            @codes: {code: name}
            @indicators: [indicator]
            @return: {code: {indicator: [trade day]}}
        """
        print('开始从数据库获取日线不存在的数据...')
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
                    print("self._mongoDbEngine.getNotExistingDates异常: {}".format(ex))
            else:
                print("self._mongoDbEngine.getNotExistingDates异常: {}".format(lastEx))
                return None

            if temp:
                data[code] = temp

        return data if data else None

    def getUpdatedCodes(self, startDate, endDate, indicators, isForced, codes=None):
        """
            @return: {code: {indicator: [trade day]}}
        """
        # get trade days
        tradeDays = self.tradeDataTable.get(startDate, endDate)

        # get stock codes, including indexes and funds
        codes = self.codeDataTable.stockAllCodesFunds if codes is None else codes

        # get not existing from DB
        if not isForced:
            codes = self.getDaysNotInDb(tradeDays, codes, indicators)
            if not codes:
                print("历史日线数据已经在数据库")
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
                print("没有日线数据需要更新")
                return None

        return codes
