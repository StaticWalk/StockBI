import pandas as pd
import matplotlib.pyplot as plt
import tushare as ts
#引入TA-Lib库
import talib as ta

#正常显示画图时出现的中文和负号
from pylab import mpl
mpl.rcParams['font.sans-serif']=['SimHei']
mpl.rcParams['axes.unicode_minus']=False

#设置token
token='88c374fc7b3bf2bd35659d324a86c5fcbe1dac0d1c8c78c06c364a30'
pro=ts.pro_api(token)

index={'上证综指': '000001.SH','深证成指': '399001.SZ',
        '沪深300': '000300.SH','创业板指': '399006.SZ',
        '上证50': '000016.SH','中证500': '000905.SH',
        '中小板指': '399005.SZ','上证180': '000010.SH'}

#获取当前交易的股票代码和名称
def get_code():
    df = pro.stock_basic(exchange='', list_status='L')
    codes=df.ts_code.values
    names=df.name.values
    stock=dict(zip(names,codes))
    stocks=dict(stock,**index)
    return stocks

#默认设定时间周期为当前时间往前推120个交易日
#日期可以根据需要自己改动
def get_data(code,n=120):
    from datetime import datetime,timedelta
    t=datetime.now()
    t0=t-timedelta(n)
    start=t0.strftime('%Y%m%d')
    end=t.strftime('%Y%m%d')
    #如果代码在字典index里，则取的是指数数据
    if code in index.values():
        df=pro.index_daily(ts_code=code,start_date=start, end_date=end)
    #否则取的是个股数据
    else:
        df=pro.daily(ts_code=code, start_date=start, end_date=end)
    #将交易日期设置为索引值
    df.index=pd.to_datetime(df.trade_date)
    df=df.sort_index()
    #计算收益率
    return df

#计算AR、BR指标
def arbr(stock,n=120):
    gap = 5
    code=get_code()[stock]
    df=get_data(code,n)[['open','high','low','close']]
    df['HO']=df.high-df.open
    df['OL']=df.open-df.low
    df['HCY']=df.high-df.close.shift(1)
    df['CYL']=df.close.shift(1)-df.low
    #计算AR、BR指标
    df['AR']=ta.SUM(df.HO, timeperiod=gap)/ta.SUM(df.OL, timeperiod=gap)*100
    df['BR']=ta.SUM(df.HCY, timeperiod=gap)/ta.SUM(df.CYL, timeperiod=gap)*100
    df['close']=df.close.shift(1)/4
    df['40']=40
    df['180']=180
    df['300']=300
    df['400']=400
    return df[['40','180','300','400','close','AR','BR']].dropna()

#对价格和ARBR进行可视化
def plot_arbr(stock,n=120):
    df=arbr(stock,n)
    # df['close'].plot(color='r',figsize=(14,5))
    # plt.xlabel('')
    # plt.title(stock+'价格走势',fontsize=15)
    df[['40','180','300','400','close','AR','BR']].plot(figsize=(16,5))
    plt.xlabel('')
    plt.show()

# plot_arbr('上证综指')
plot_arbr('创业板指',250)
# plot_arbr('沪深300',250)
# plot_arbr('东方通信',90)