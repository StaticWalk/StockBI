import pymongo
import pandas as pd
from time import sleep

from envs.Lib.datetime import datetime

from common.Common import *


class StockMongoDbEngine(object):
    host = Config.mongoDbHost
    port = Config.mongoDbPort

    stockTicksDb = 'stockTicksDb'  # 股票分笔数据
    stockDaysDb = 'stockDaysDb'  # 股票日线行情数据

    # DB for TuShare Data Source
    stockCommonDbTuShare = 'stockCommonDbTuShare'
    tradeDayTableNameTuShare = "tradeDayTableTuShare"
    codeTableNameTuShare = "codeTableTuShare"
    stockDaysDbTuShare = 'stockDaysDbTuShare'  # 股票日线行情数据

    def updateStockCodes(self, codes):
        collection = self.getCodeTableCollection()

        # create index
        collection.create_index([('code', pymongo.ASCENDING)], unique=True)

        # update into DB
        try:
            for code in codes:
                flt = {'code': code['code']}
                collection.update_one(flt, {'$set': {'name': code['name']}}, upsert=True)

        except Exception as ex:
            self.logThread.print("更新股票代码数据到MongoDB异常:{0}".format(str(ex) + ', ' + str(ex.details)))
            return False

        return True




    def __init__(self,logThread):

        self.client = pymongo.MongoClient(self.host, self.port)

        self.logThread = logThread

    def getNotExistingDates(self, code, dates, indicators):
        """ @dates: sorted [date]
            @indicators: [indicator]

            @return: {indicator:[date]}
        """
        if (not dates) or (not indicators):
            return None

        collection = self.getStockDaysDb()[code]

        dateStart = datetime.strptime(dates[0], '%Y-%m-%d')
        dateEnd = datetime.strptime(dates[-1] + ' 23:00:00', '%Y-%m-%d %H:%M:%S')

        flt = {'datetime': {'$gte': dateStart,
                            '$lt': dateEnd}}

        try:
            cursor = collection.find(flt)
        except Exception as ex:
            self.logThread.print(
                "MongoDB Exception({0}): find existing dates[{1}, {2}] for {3}".format(str(ex) + ', ' + str(ex.details),
                                                                                       dates[0], dates[-1], code),
                )
            return None

        # assume all not in DB
        data = {x: dates.copy() for x in indicators}

        for d in cursor:
            date = datetime.strftime(d['datetime'], '%Y-%m-%d')

            for indicator in d:
                if indicator in data:
                    if date in data[indicator]:
                        # remove existing date
                        data[indicator].remove(date)

                        if not data[indicator]:
                            del data[indicator]

        return data if data else None



    def updateDays(self, code, data):
        """ @data: [{row0}, {row1}] """
        # create index
        for _ in range(3):
            try:
                collection = self.getStockDaysDb()[code]
                collection.create_index([('datetime', pymongo.ASCENDING)], unique=True)
                break
            except pymongo.errors.AutoReconnect as ex:
                lastEx = ex
                self.logThread.print("更新{}日线数据到MongoDB异常: {}".format(code, str(ex) + ', ' + str(ex.details)))
                sleep(1)
        else:
            self.logThread.print("更新{}日线数据到MongoDB异常: {}".format(code, str(lastEx) + ', ' + str(lastEx.details)))
            return False

        if data:
            # update to DB
            try:
                for doc in data:
                    flt = {'datetime': doc['datetime']}
                    collection.update_one(flt, {'$set': doc}, upsert=True)
            except Exception as ex:
                self.logThread.print("更新{0}日线数据到MongoDB异常:{1}".format(code, str(ex) + ', ' + str(ex.details)))
                return False

        return True



    def updateTradeDays(self, dates):
        collection = self.getTradeDayTableCollection()

        # create index
        collection.create_index([('datetime', pymongo.ASCENDING)], unique=True)

        # update into DB
        try:
            for date in dates:
                flt = {'datetime': date['datetime']}
                result = collection.update_one(flt, {'$set':{'tradeDay': date['tradeDay']}}, upsert=True)
                if not (result.acknowledged and (result.matched_count == 1 or result.upserted_id is not None)):
                    self.logThread.print("更新交易日数据到MongoDB失败: date={}, raw_result={}".format(date, result.raw_result))
                    return False
        except Exception as ex:
            self.logThread.print("更新交易日数据到MongoDB异常: {}".format(str(ex) + ', ' + str(ex.details)))
            return False
        return True


    def getTradeDaysByRelativeZero(self, baseDate):
        """ 基准日期向前找到第一个交易日 """

        baseDateSave = baseDate

        collection = self.getTradeDayTableCollection()

        baseDate = datetime.strptime(baseDate, '%Y-%m-%d')
        flt = {'datetime':{'$lte':baseDate}}

        try:
            cursor = collection.find(flt).sort('datetime', pymongo.DESCENDING)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): @_getTradeDaysByRelativeZero({1})".format(str(ex) + ', ' + str(ex.details), baseDateSave))
            return None

        for d in cursor:
            if d['tradeDay']:
                return [d]

        return None


    def getDaysLatestDate(self):
        """ 获取数据库里交易日数据的最新日期，不是交易日 """

        while True:
            try:
                cursor = self.findTradeDays()
                if cursor is None: return None

                cursor = cursor.sort('datetime', pymongo.DESCENDING).limit(1)

                for d in cursor:
                    return d

                return None

            except Exception as ex:
                self.logThread.print("MongoDB 异常({0}): 获取最新日期".format(str(ex) + ', ' + str(ex.details)))

                if '无法连接' in str(ex):
                    self.logThread.print('MongoDB正在启动, 等待60s后重试...')
                    sleep(60)
                    continue

                return None


    def getTradeDaysByRelativePositive(self, baseDate, n):

        baseDateSave = baseDate
        nSave = n

        # always get 0 offset trade day
        baseDate = self.getTradeDaysByRelativeZero(baseDate)
        if baseDate is None: return None

        # find backward n trade days
        collection = self.getTradeDayTableCollection()

        flt = {'datetime': {'$gt': baseDate[0]['datetime']}}

        try:
            cursor = collection.find(flt).sort('datetime', pymongo.ASCENDING)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): @_getTradeDaysByRelativePositive({1}, {2})".format(str(ex) + ', ' + str(ex.details), baseDateSave, nSave))
            return None

        dates = [baseDate[0]]
        for d in cursor:
            if d['tradeDay']:
                dates.append(d)

                n -= 1
                if n == 0:
                    return dates

        # 如果数据库里的最新日期不是今日，提醒更新数据, 并返回None
        date = self.getDaysLatestDate()
        if date is not None:
            now = datetime.now()
            if now > datetime(now.year, now.month, now.day, 18, 0, 0) and Time.dateCmp(date['datetime'], now) != 0:
               self.logThread.print("数据库里的最新日期不是今日, 请更新历史日线数据")
               return None
        return dates

    def getTradeDaysByRelativeNegative(self, baseDate, n):

        baseDateSave = baseDate
        nSave = n

        # always get 0 offset trade day
        baseDate = self.getTradeDaysByRelativeZero(baseDate)
        if baseDate is None: return None

        # find forward n trade days
        collection = self.getTradeDayTableCollection()

        flt = {'datetime': {'$lt': baseDate[0]['datetime']}}

        try:
            cursor = collection.find(flt).sort('datetime', pymongo.DESCENDING)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): @_getTradeDaysByRelativeNegative({1}, {2})".format(
                str(ex) + ', ' + str(ex.details), baseDateSave, nSave))
            return None

        dates = [baseDate[0]]
        for d in cursor:
            if d['tradeDay']:
                dates.append(d)

                n += 1
                if n == 0:
                    return dates

        self.logThread.print("数据库里没有{0}向前{1}个交易日的日期数据".format(baseDateSave, abs(nSave)))
        return None


    def getTradeDaysByRelative(self, baseDate, n):
        """ 从数据库获取相对日期的交易日数据
            @n: 向前或者向后多少个交易日
            @return: [doc of trade day]
        """
        if n > 0:
            tradeDays = self.getTradeDaysByRelativePositive(baseDate, n)
        elif n < 0:
            tradeDays = self.getTradeDaysByRelativeNegative(baseDate, n)
        else:
            tradeDays = self.getTradeDaysByRelativeZero(baseDate)

        if tradeDays is None: return None

        return tradeDays


    def getTradeDaysByAbsolute(self, startDate, endDate):
        """ 从数据库获取指定日期区间的交易日数据 """
        cursor = self.findTradeDays(startDate, endDate)
        if cursor is None:
            return None

        if startDate is not None:
            # some of dates can not be found in DB
            if len(Time.getDates(startDate, endDate)) != cursor.count():
                self.logThread.print("有些交易日[{0}, {1}]没有在数据库".format(startDate, endDate))
                return None

        tradeDays = []
        for d in cursor:
            if d['tradeDay']:
                tradeDays.append(d)

        return tradeDays

    def findTradeDays(self, startDate=None, endDate=None):
        collection = self.getTradeDayTableCollection()

        if startDate is None:
            flt = None
        else:
            dateStart = datetime.strptime(startDate, '%Y-%m-%d')
            dateEnd = datetime.strptime(endDate + ' 23:00:00', '%Y-%m-%d %H:%M:%S')

            flt = {'datetime':{'$gte':dateStart,
                               '$lt':dateEnd}}

        try:
            cursor = collection.find(flt)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): find TradeDays[{1}, {2}]".format(str(ex) + ', ' + str(ex.details), startDate, endDate),
                             )
            return None

        return cursor


    def getTradeDayTableCollection(self):
        collection = self.client[self.stockCommonDbTuShare][self.tradeDayTableNameTuShare]
        return collection


    def getStockCodes(self, codes=None):

        # 不载入任何股票
        if codes == []:
            return []

        collection = self.getCodeTableCollection()

        if codes is None:
            flt = None
        else:
            flt = {'code': {'$in': codes}}

        try:
            cursor = collection.find(flt)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): 查询股票名称表".format(str(ex) + ', ' + str(ex.details))
                        )
            return None

        data = []
        for d in cursor:
            data.append(d)

        return data if data else None

    def getCodeTableCollection(self):
        collection = self.client[self.stockCommonDbTuShare][self.codeTableNameTuShare]
        return collection

    def getStockDaysDb(self):
        db = self.client[self.stockDaysDbTuShare]
        return db

    def getCodeDay(self, code, baseDate, name=None):
        """ 得到个股的当日交易日, 向前贪婪 """
        collection = self.getStockDaysDb()[code]

        date = datetime.strptime(baseDate + ' 23:00:00', '%Y-%m-%d %H:%M:%S')
        flt = {'datetime': {'$lt': date}}

        sortMode = pymongo.DESCENDING

        try:
            cursor = collection.find(flt).sort('datetime', sortMode).limit(1)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): @_findOneCodeDaysByZeroRelative{1}:{2}, [{3}, {4}]日线数据".format(
                str(ex) + ', ' + str(ex.details),
                code, name,
                baseDate, n))
            return None

        for d in cursor:
            return d['datetime'].strftime('%Y-%m-%d')

        return None

    def getOneCodeDaysByRelative(self, code, indicators, baseDate, n=0, name=None):

        cursor = self.findOneCodeDaysByRelative(code, baseDate, n, name)
        if cursor is None: return None

        return self.getOneCodeDaysByCursor(cursor, indicators)

    def findOneCodeDaysByRelative(self, code, baseDate, n=0, name=None):
        """
            包含当日，也就是说offset 0总是被包含的
        """
        # 获取当日日期
        baseDay = self.getCodeDay(code, baseDate, name)
        if baseDay is None: return None

        collection = self.getStockDaysDb()[code]

        if n <= 0:
            date = datetime.strptime(baseDay + ' 23:00:00', '%Y-%m-%d %H:%M:%S')
            flt = {'datetime': {'$lt': date}}

            sortMode = pymongo.DESCENDING
        else:
            date = datetime.strptime(baseDay, '%Y-%m-%d')
            flt = {'datetime': {'$gte': date}}  # ignore baseDate, no matter its in DB or not

            sortMode = pymongo.ASCENDING

        # 向前贪婪
        n = abs(n) + 1

        try:
            cursor = collection.find(flt).sort('datetime', sortMode).limit(n)
        except Exception as ex:
            self.logThread.print("MongoDB Exception({0}): @_findOneCodeDaysByRelative{1}:{2}, [{3}, {4}]日线数据".format(
                str(ex) + ', ' + str(ex.details),
                code, name,
                baseDate, n))
            return None

        # We don't check any thing about if we actually get n days data.
        # The reason is that we don't know future, as well as 何时股票上市

        return cursor



    def findOneCodeDays(self, code, startDate, endDate, name=None):
        collection = self.getStockDaysDb()[code]

        dateStart = datetime.strptime(startDate, '%Y-%m-%d')
        dateEnd = datetime.strptime(endDate + ' 23:00:00', '%Y-%m-%d %H:%M:%S')

        flt = {'datetime': {'$gte': dateStart,
                            '$lt': dateEnd}}

        try:
            cursor = collection.find(flt)
        except Exception as ex:
            self.logThread.print(
                "MongoDB Exception({0}): 查找{1}:{2}, [{3}, {4}]日线数据".format(str(ex) + ', ' + str(ex.details),
                                                                           code, name,
                                                                           startDate, endDate))
            return None

        return cursor

    def getOneCodeDaysByCursor(self, cursor, indicators):
        try:
            columns = indicators + ['datetime']
            if 'adjfactor' not in columns:
                columns.append('adjfactor')

            df = pd.DataFrame(list(cursor), columns=columns)
            df = df.dropna(axis=1, how='all') # 去除全为NaN的列，比如指数数据，没有'mf_vol'
            df = df.set_index('datetime')

        except Exception as ex:
            return None

        return None if df.empty else df

    def getOneCodeDays(self, code, startDate, endDate, indicators, name=None, raw=False):
        """
            通过绝对日期获取个股日线数据
            @raw: True - not via cache, for called by DB cache
        """
        cursor = self.findOneCodeDays(code, startDate, endDate, name)
        if cursor is None: return None

        return self.getOneCodeDaysByCursor(cursor, indicators)