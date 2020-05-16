import copy

from common.Common import *
from engine.StockDataEngine import StockDataEngine


class StockTradeDataTable(object):

    def __init__(self, mongoDbEngine, dataEngine,logThread):
        self.mongoDbEngine = mongoDbEngine
        self.dataEngine = dataEngine
        self._init()
        self.logThread = logThread

    def _init(self):
        self.table = {}
        self.compactTable = []

    # 更新交易日表数据
    def update(self, startDate, endDate):
        self.logThread.print('开始更新交易日数据...')

        if self.load([startDate, endDate]):
            self.logThread.print('交易日数据已在数据库')
            return True

        tradeDays = self.dataEngine.getTradeDaysFromTuSharePro(startDate, endDate)
        if tradeDays is None: return False

        # set to tables and then update to DB
        if not self.set(startDate, endDate, tradeDays):
            return False

        self.logThread.print('交易日数据更新完成')
        return True

    def set(self, startDate, endDate, tradeDays):
        return self.set2Table(startDate, endDate, tradeDays) and self.update2Db(startDate, endDate, tradeDays)


    def update2Db(self, startDate, endDate, tradeDays):

        # convert to MongoDB format
        datesForDb = []
        dates = Time.getDates(startDate, endDate)

        for date in dates:
            doc = {'datetime':date}

            if date.strftime('%Y-%m-%d') in tradeDays:
                doc['tradeDay'] = True
            else:
                doc['tradeDay'] = False

            datesForDb.append(doc)

        # update into DB
        return self.mongoDbEngine.updateTradeDays(datesForDb)


    def get(self, start, end):
        """ @return: [trade day] """

        dates = Time.getDates(start, end)

        tradeDays = []
        for date in dates:
            dateSave = date.strftime("%Y-%m-%d")
            date = dateSave.split('-')
            if date[0] in self.table:
                if date[1] in self.table[date[0]]:
                    if date[2] in self.table[date[0]][date[1]]:
                        if self.table[date[0]][date[1]][date[2]][0]:
                            tradeDays.append(dateSave)

        return tradeDays



    def load(self, dates):
        self.logThread.print("开始载入交易日数据{0}...".format(dates))

        # 根据不同格式载入
        if len(dates) == 2:
            startDate, endDate, tradeDays = self.load2(dates[0], dates[1])
        else:
            startDate, endDate, tradeDays = self.load3(dates[0], dates[1], dates[2])

        if startDate is None:
            return False

        if not self.set2Table(startDate, endDate, tradeDays):
            return False

        self.logThread.print("交易日数据[{0}, {1}]载入完成".format(startDate, endDate))

        return True

    def set2Table(self, start, end, tradeDays):
        """ [@start, @end] is range """

        dates = Time.getDates(start, end)

        dates = [x.strftime("%Y-%m-%d") for x in dates]
        days = tradeDays

        for day in dates:
            dayTemp = day.split('-')

            if dayTemp[0] not in self.table:
                self.table[dayTemp[0]] = {}

            if dayTemp[1] not in self.table[dayTemp[0]]:
                self.table[dayTemp[0]][dayTemp[1]] = {}

            if day in days:
                self.table[dayTemp[0]][dayTemp[1]][dayTemp[2]] = [True, -1]
            else:
                self.table[dayTemp[0]][dayTemp[1]][dayTemp[2]] = [False, -1]

        self.compactTable.extend(days)

        return self.updateIndex()


    def buildIndex(self, date):
        year, month, day = date.split('-')

        i = 0
        while i < len(self.compactTable):
            tradeDay = self.compactTable[i]
            if date < tradeDay: break
            i += 1

        self.table[year][month][day][1] = i - 1

    def updateIndex(self):
        self.compactTable.sort()

        preDate = None
        oldest = None
        date = None # will be latest after loop

        years = sorted(self.table)
        for year in years:
            months = sorted(self.table[year])
            for month in months:
                days = sorted(self.table[year][month])
                for day in days:
                    date = year + '-' + month + '-' + day

                    if not oldest: oldest = date

                    # index should be built based on continous days
                    if preDate:
                        if Time.getDateStr(preDate, 1) != date:
                            self.logThread.print("Days in TradeDay Table aren't continous!")
                            return False
                    preDate = date

                    # build index for day
                    self.buildIndex(date)

        return True


    def load2(self, startDate, endDate):
        if isinstance(endDate, int):
            tradeDays = self.mongoDbEngine.getTradeDaysByRelative(startDate, endDate)
            if tradeDays is None: return None, None, None
            assert(tradeDays)
            tradeDays = self.convertTradeDays(tradeDays)
            startDateNew = tradeDays[0]
            endDateNew = tradeDays[-1]
            if startDate > endDateNew:
                endDateNew = startDate
            elif startDate < startDateNew:
                startDateNew = startDate
            return startDateNew, endDateNew, tradeDays

        else:
            tradeDays = self.mongoDbEngine.getTradeDaysByAbsolute(startDate, endDate)
            if tradeDays is None: return None, None, None

            tradeDays = self.convertTradeDays(tradeDays)

            return  startDate, endDate, tradeDays


    def convertTradeDays(self, tradeDays):
        tradeDays = [doc['datetime'].strftime("%Y-%m-%d") for doc in tradeDays]
        tradeDays.sort()
        return tradeDays


    def load3(self, startDate, endDate, n):
        # 分部分载入
        # front part
        startDateNew, endDateNew = startDate, endDate
        if isinstance(startDate, int):
            startDateNew, endDateNew = endDateNew, startDateNew

        frontStartDate, frontEndDate, frontTradeDays = self.load2(startDateNew, endDateNew)
        if frontStartDate is None: return None, None, None

        # back part
        backStartDate, backEndDate, backTradeDays = self.load2(endDate, n)
        if backStartDate is None: return None, None, None

        # combine trade days, always zero offset trade day is duplicated
        for day in frontTradeDays:
            if day in backTradeDays:
                backTradeDays.remove(day)

        tradeDays = frontTradeDays + backTradeDays
        tradeDays.sort()

        # combine date range
        if frontStartDate < backStartDate:
            startDate = frontStartDate
        else:
            startDate = backStartDate

        if backEndDate > frontEndDate:
            endDate = backEndDate
        else:
            endDate = frontEndDate

        # combine with trade days
        startDateNew = tradeDays[0]
        endDateNew = tradeDays[-1]

        if startDate < startDateNew:
            startDateNew = startDate

        if endDate > endDateNew:
            endDateNew = endDate

        return  startDateNew, endDateNew, tradeDays
