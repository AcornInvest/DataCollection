import FinanceDataReader as fdr
from pykrx import stock
#import pandas_datareader.data as pdr
import yfinance as yf

tickers = stock.get_market_ticker_list()

class GetAllTicker:
    '''
    기준 일자를 받아서 해당날 기준 상장종목, 상폐종목 코드, 상장일, 상폐일 dataframe 리턴
    '''


    '''
    def __init__(self):
        pass
    '''
    def GetListingStocks(self, date):
        stocks = fdr.StockListing('KRX')  # 코스피, 코스닥, 코넥스 전체
        #stocks = fdr.StockListing('KRX-DESC')  # 코스피, 코스닥, 코넥스 전체
        return stocks

    def GetDelistingStocks(self, date):
        pass



