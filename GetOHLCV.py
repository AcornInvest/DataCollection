import os
import configparser
import FinanceDataReader as fdr
from pykrx import stock
# import pandas_datareader.data as pdr
import yfinance as yf
import pandas as pd
from datetime import datetime


class GetOHLCV:
    '''
    종목 코드를 받아서 OHLCV dataframe 반환
    '''

    def __init__(self, logger):
        self.logger = logger
        self.limit_change_day = startday = datetime(2015, 6, 15) #가격제한폭이 30%로 확대된 날

        # 설정 로드
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_GetOHLCV.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_codeLists = config['path']['path_codelists']
        self.path_OHLCV_init = config['path']['path_OHLCV_init']

    def get_OHLCV(self, code, start_date, end_date, listed_status):
        df_OHLCV_fdr = fdr.DataReader(code, start=start_date, end=end_date)
        if( listed_status == 'Listed'): #상장 종목
            code_yahoo = code + '.KS'
            ticker = yf.Ticker(code_yahoo)
            df_OHLCV_yf = ticker.history(interval='1d', start=start_date, end=end_date, auto_adjust=False)
            df_OHLCV_yf.reset_index(inplace=True)
            df_OHLCV_yf['Date'] = pd.to_datetime(df_OHLCV_yf['Date']).dt.strftime('%Y-%m-%d') #인덱스 열을 바꾸는 것으로 코드 수정
            df_OHLCV_yf.set_index('Date', inplace=True)

        folder = f"{self.path_OHLCV_init}\\{listed_status}\\"
        filename = f"{code}_OHLCV_fdr.xlsx"
        path_file = folder + filename
        df_OHLCV_fdr.to_excel(path_file, index=True)

        filename = f"{code}_OHLCV_yf.xlsx"
        path_file = folder + filename
        df_OHLCV_yf.to_excel(path_file, index=True)










        #종목 데이터 무결성 확인: NaN 없는지. 전일 종가 대비 30% 이상 변하지 않았는지

