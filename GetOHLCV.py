import FinanceDataReader as fdr
from pykrx import stock
# import pandas_datareader.data as pdr
import yfinance as yf
import pandas as pd


class GetCodeOHLCV:
    '''
    종목 코드를 받아서 OHLCV dataframe 반환
    '''

    def __init__(self):
        pass

    def GetListedOHLCV(self, start_date, end_date):
        df_fdr = fdr.DataReader('001040', start=start_date, end=end_date)