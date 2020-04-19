from collections import OrderedDict
from datetime import datetime, timedelta


class Time:
    def getTimeInterval(time1, time2):
        """
            获取时间差，单位是秒
            @time: 'hh:mm:ss'
        """
        time1S = int(time1[:2] ) *3600 + int(time1[3:5] ) *60 + int(time1[-2:])
        time2S = int(time2[:2] ) *3600 + int(time2[3:5] ) *60 + int(time2[-2:])

        return time2S - time1S

    def getDate(start, step):
        if isinstance(start, str):
            start = start.split('-')
            start = datetime( int(start[0]), int(start[1]), int(start[2]) )

        start += timedelta(days=step)

        return start

    def getDateStr(start, step):
        if isinstance(start, str):
            start = start.split('-')
            start = datetime( int(start[0]), int(start[1]), int(start[2]) )

        start += timedelta(days=step)

        return start.strftime("%Y-%m-%d")

    def dateCmp(date1, date2):
        if isinstance(date1, str):
            date1 = date1.split('-')
        if isinstance(date1, list):
            date1 = datetime( int(date1[0]), int(date1[1]), int(date1[2]) )
        if isinstance(date1, datetime):
            date1 = datetime(date1.year, date1.month, date1.day)

        if isinstance(date2, str):
            date2 = date2.split('-')
        if isinstance(date2, list):
            date2 = datetime( int(date2[0]), int(date2[1]), int(date2[2]) )
        if isinstance(date2, datetime):
            date2 = datetime(date2.year, date2.month, date2.day)

        if date1 > date2: return 1
        elif date1 == date2: return 0
        else: return -1


    def isDateFormatCorrect(date):
        if not isinstance(date, str): return False

        date = date.split('-')

        if len(date) != 3: return False

        if len(date[0]) != 4 or len(date[1]) != 2 or len(date[2]) != 2: return False

        for part in date:
            for c in part:
                if c not in ['0' ,'1' ,'2' ,'3' ,'4' ,'5' ,'6' ,'7' ,'8' ,'9']:
                    return False

        # year
        if int(date[0][0]) not in range(1, 10): return False
        if int(date[0][1]) not in range(0, 10): return False
        if int(date[0][2]) not in range(0, 10): return False
        if int(date[0][3]) not in range(0, 10): return False

        # month
        if int(date[1]) not in range(1, 13): return False
        # day
        if int(date[2]) not in range(1, 32): return False

        return True


    def getDates(start, end, strFormat=False):
        if isinstance(start, str):
            start = start.split('-')
            start = datetime( int(start[0]), int(start[1]), int(start[2]) )

        if isinstance(end, str):
            end = end.split('-')
            end = datetime( int(end[0]), int(end[1]), int(end[2]) )


        dates = []

        i = timedelta(days=0)
        while i <= end - start:
            dates.append((start + i).strftime("%Y-%m-%d") if strFormat else (start + i))
            i += timedelta(days=1)

        return dates

    def isInMonths(year, month, months):
        """ @months: {year:{month:None}} """

        if year not in months: return False
        if month not in months[year]: return False

        return True

    def getNextMonth(date):
        date = date.split('-')

        month = int(date[1])

        day = '01'

        if month == 12:
            year = str(int(date[0]) + 1)
            month = '01'
        else:
            year = date[0]
            month = str(month + 1)
            if len(month) == 1: month = '0' + month

        date = year + '-' + month + '-' + day
        return date

    def getPreMonth(date):
        date = date.split('-')

        month = int(date[1])

        day = '01'

        if month == 1:
            year = str(int(date[0]) - 1)
            month = '12'
        else:
            year = date[0]
            month = str(month - 1)
            if len(month) == 1: month = '0' + month

        date = year + '-' + month + '-' + day
        return date


class Config:

# mongoDb config
    mongoDbHost = "localhost"
    mongoDbPort = 27017

# tuShareToken
    tuShareProToken = "88c374fc7b3bf2bd35659d324a86c5fcbe1dac0d1c8c78c06c364a30"


class StockData:

    # volume是成交量，单位是股数。数据库里的成交量也是股数。 adj Factor 调整因子
    dayIndicators = ['open', 'high', 'low', 'close', 'volume', 'amt', 'turn', 'adjfactor']
    adjFactor = 'adjfactor'

    indexes = OrderedDict([
        ('000001.SH', '上证指数'),
        ('399001.SZ', '深证成指'),
        ('399006.SZ', '创业板指'),
        ('399005.SZ', '中小板指'),
    ])

    # 可以考虑使用etf 代替板块指数
    funds = {'510050.SH': '50ETF',
             '510300.SH': '300ETF',
             '510500.SH': '500ETF'
             }

    # 板块指数
    sectors = {'000016.SH': '上证50',
               '399300.SZ': '沪深300',
               '399905.SZ': '中证500'
               }
    sz50Index = '000016.SH'
    hs300Index = '399300.SZ'
    zz500Index = '399905.SZ'


    etf50 = '510050.SH' # 50ETF是2005.02.23上市的
    etf300 = '510300.SH' # 300ETF是2012.05.28上市的
    etf500 = '510500.SH' # 500ETF是2013.03.15上市的

    # 大盘指数
    shIndex = '000001.SH'
    szIndex = '399001.SZ'
    cybIndex = '399006.SZ'
    zxbIndex = '399005.SZ'

    paraCode ='002001.SZ'

    def getIndex(code):
        """
            获取个股对应的大盘指数
        """
        if code[-2:] == 'SH': return StockData.shIndex

        if code[:3] == '002': return StockData.zxbIndex
        if code[:3] == '300': return StockData.cybIndex

        if code[-2:] == 'SZ': return StockData.szIndex

        assert (0)
        return None

