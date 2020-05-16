import numpy as np
import pandas as pd



def KDJ(highs, lows, closes, N=9, M1=3, M2=3, adjust=True):
    """
        @closes, highs, lows: numpy array
        @return: numpy array
    """
    C = closes
    H = highs
    L = lows

    RSV = (C - LLV(L, N)) / (HHV(H, N) - LLV(L, N)) * 100
    RSV = RSV[~np.isnan(RSV)]
    K = SMA(RSV, N, M1, adjust=adjust)
    D = SMA(K, N, M2, adjust=adjust)
    J = 3 * np.array(K) - 2 * np.array(D)
    return K, D, J


def LLV(v, N):
    """
        @v: numpy array or list
        @return: numpy array
    """
    return pd.Series(v).rolling(N).min().values

def HHV(v, N):
    """
        @v: numpy array or list
        @return: numpy array
    """
    return pd.Series(v).rolling(N).max().values

def SMA(C, N, M, adjust=True):
    """
        同花顺的SMA
        N > M
    """
    alpha = M/N

    return EWMA(C, alpha, adjust=adjust)


def EMA(X, N, adjust=True):
    """
        同花顺的EMA
        数据长度5000以下时，比pandas要快
        @X: numpy array or list
        @N: 周期
        @adjust: pandas里默认是True，这里默认是False跟同花顺保持一致。True时，最近的值权重会大些。
        @return: list
    """
    alpha = 2/(N + 1)

    return EWMA(X, alpha, adjust=adjust)


def ATR(highs, lows, closes, timeperiod=14, adjust=True):
    """
        @return: list，前@timeperiod个元素的值是NaN。主要因为计算差值多占用了一个元素。
    """
    assert len(highs) == len(lows) == len(closes)

    trs = [0]*(len(highs) - 1)
    for i in range(1, len(highs)):
        tr = max(highs[i], closes[i-1]) - min(lows[i], closes[i-1])
        trs[i-1] = tr

    atr = EMA(trs, timeperiod, adjust=adjust)

    atr.insert(0, np.nan)
    atr[:timeperiod] = [np.nan]*timeperiod

    return atr

def EWMA(X, alpha, adjust=True):
    """
        指数加权移动平均值
        数据长度10000以下时，比pandas要快
        @X: numpy array or list
        @alpha: 平滑指数
        @adjust: pandas里默认是True，这里默认是False跟同花顺保持一致。True时最近的权重会大些。
        @return: list
    """
    weightedX = [0]*len(X)
    weightedX[0] = X[0]

    if adjust:
        numerator = X[0]
        denominator = 1

        for i in range(1, len(X)):
            numerator = X[i] + numerator*(1-alpha)
            denominator = 1 + denominator*(1-alpha)

            weightedX[i] = numerator/denominator
    else:
        for i in range(1, len(X)):
            weightedX[i] = alpha*X[i] + (1-alpha)*weightedX[i-1]

    return weightedX