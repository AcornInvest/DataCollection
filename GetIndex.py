import os
import configparser
import FinanceDataReader as fdr
from pykrx import stock
# import pandas_datareader.data as pdr
#import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import utils
import sqlite3

class GetIndex:
    '''
    KOSPI, KOSDAQ index 정보 가져오기
    ohlcv
    '''

    # 날짜 받기
    # pykrx 에서 데이터 얻기
    # dataframe 에서 필요한 것만 추출
    # sqlite에 저장

    def __init__(self, logger):
        self.suffix = 'index'  # 파일 이름 저장시 사용하는 접미사
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_GetIndex.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.path_data = config['path']['path_data']

    def get_index(self, start_date, end_date, datemanage):
        # 관심 있는 열 선택
        selected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']

        df_KOSPI = fdr.DataReader('KS11', start=start_date, end=end_date)
        df_KOSPI = df_KOSPI.reset_index()
        df_KOSPI = df_KOSPI[selected_columns]
        df_KOSPI['Date'] = pd.to_datetime(df_KOSPI['Date']).dt.date
        df_KOSPI['code'] = 'kospi'  # 종목 코드 열 추가

        df_KOSDAQ = fdr.DataReader('KQ11', start=start_date, end=end_date)
        df_KOSDAQ = df_KOSDAQ.reset_index()
        df_KOSDAQ = df_KOSDAQ[selected_columns]
        df_KOSDAQ['Date'] = pd.to_datetime(df_KOSDAQ['Date']).dt.date
        df_KOSDAQ['code'] = 'kosdaq'  # 종목 코드 열 추가

        folder_sql = f'{self.path_data}\\index\\{datemanage.workday_str}\\'
        if not os.path.exists(folder_sql):
            os.makedirs(folder_sql)
        filename = f'index_data_{datemanage.workday_str}.db'
        path_sql = folder_sql + filename
        conn = sqlite3.connect(path_sql)

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS index_data (
            code TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL      
        );
        '''

        conn.execute(create_table_query)
        conn.commit()

        df_KOSPI.to_sql('index_data', conn, if_exists='append', index=False)
        df_KOSDAQ.to_sql('index_data', conn, if_exists='append', index=False)

        # 인덱스 생성 (쿼리 성능 향상)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON index_data (code);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON index_data (date);')
        conn.commit()

        # 데이터베이스 연결 종료
        conn.close()