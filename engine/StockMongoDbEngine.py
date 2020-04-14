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
            print("更新股票代码数据到MongoDB异常:{0}".format(str(ex) + ', ' + str(ex.details)))
            return False

        return True



    def getStockDaysDb(self):
        db = self.client[self.stockDaysDbTuShare]
        return db

    def __init__(self):

        self.client = pymongo.MongoClient(self.host, self.port)

        # DB cache
        self._dbCache = None

        # todo process on DBcache
        # if cache:
        #     self._dbCache = DyGetStockDbCache(self._info, self)

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
            print(
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
                print("更新{}日线数据到MongoDB异常: {}".format(code, str(ex) + ', ' + str(ex.details)))
                sleep(1)
        else:
            print("更新{}日线数据到MongoDB异常: {}".format(code, str(lastEx) + ', ' + str(lastEx.details)))
            return False

        # update to DB
        try:
            for doc in data:
                flt = {'datetime': doc['datetime']}
                collection.update_one(flt, {'$set':doc}, upsert=True)
        except Exception as ex:
            print("更新{0}日线数据到MongoDB异常:{1}".format(code, str(ex) + ', ' + str(ex.details)))
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
                    print("更新交易日数据到MongoDB失败: date={}, raw_result={}".format(date, result.raw_result))
                    return False
        except Exception as ex:
            print("更新交易日数据到MongoDB异常: {}".format(str(ex) + ', ' + str(ex.details)))
            return False
        return True


    def getTradeDaysByRelative(self, baseDate, n):
        """ 从数据库获取相对日期的交易日数据
            @n: 向前或者向后多少个交易日
            @return: [doc of trade day]
        """
        if n > 0:
            tradeDays = self._getTradeDaysByRelativePositive(baseDate, n)
        elif n < 0:
            tradeDays = self._getTradeDaysByRelativeNegative(baseDate, n)
        else:
            tradeDays = self._getTradeDaysByRelativeZero(baseDate)

        if tradeDays is None: return None

        return tradeDays

    def getabc(self):
        print("okok")

    def getTradeDaysByAbsolute(self, startDate, endDate):
        """ 从数据库获取指定日期区间的交易日数据 """
        cursor = self.findTradeDays(startDate, endDate)
        if cursor is None:
            return None

        if startDate is not None:
            # some of dates can not be found in DB
            if len(Time.getDates(startDate, endDate)) != cursor.count():
                print("有些交易日[{0}, {1}]没有在数据库".format(startDate, endDate))
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
            print("MongoDB Exception({0}): find TradeDays[{1}, {2}]".format(str(ex) + ', ' + str(ex.details), startDate, endDate),
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
            print("MongoDB Exception({0}): 查询股票名称表".format(str(ex) + ', ' + str(ex.details))
                        )
            return None

        data = []
        for d in cursor:
            data.append(d)

        return data if data else None

    def getCodeTableCollection(self):
        collection = self.client[self.stockCommonDbTuShare][self.codeTableNameTuShare]
        return collection


