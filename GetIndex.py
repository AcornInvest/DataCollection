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
import shutil

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

    def get_index(self, start_date, end_date, datemanage): #이번 작업에 추가되는 데이터
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
            amount REAL,
            PRIMARY KEY (stock_code, date)
        );
        '''

        conn.execute(create_table_query)
        conn.commit()

        df_KOSPI.to_sql('index_data', conn, if_exists='append', index=False)
        df_KOSDAQ.to_sql('index_data', conn, if_exists='append', index=False)

        # 인덱스 생성 (쿼리 성능 향상)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_code ON index_data (code);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON index_data (date);')
        conn.commit()

        # 데이터베이스 연결 종료
        conn.close()

    def merge_data(self, datemanage):
        # 기존의 merged 데이터에 새로 추가되는 index 데이터를 넣고 새 이름으로(이번 작업일) 저장함.
        folder_new_data = f'{self.path_data}\\index\\{datemanage.workday_str}\\' # 마지막 데이터 작업일
        folder_merged = f'{self.path_data}\\index\\'
        filename_new_data = f'index_data_{datemanage.workday_str}.db'
        filename_merged_last = f'index_data_merged_{datemanage.startday_str}.db'
        filename_merged_new = f'index_data_merged_{datemanage.workday_str}.db'

        path_new_data = folder_new_data + filename_new_data
        path_merged_last = folder_merged + filename_merged_last
        path_merged_new = folder_merged + filename_merged_new

        # Create a new database file by copying the existing merged_last database
        shutil.copyfile(path_merged_last, path_merged_new)

        # Connect to the new merged database and the new_data database
        conn_merged_new = sqlite3.connect(path_merged_new)
        conn_new_data = sqlite3.connect(path_new_data)

        # Create cursors for both databases
        cur_merged_new = conn_merged_new.cursor()
        cur_new_data = conn_new_data.cursor()

        # Get column names from the new_data database's index_data table
        cur_new_data.execute("PRAGMA table_info(index_data)")
        columns_info = cur_new_data.fetchall()
        columns = [info[1] for info in columns_info]  # Extract column names
        column_count = len(columns) # path_new_data db 의 열 수(num of column) 추출

        # Create a placeholder string for the INSERT statement
        placeholders = ', '.join(['?'] * column_count)
        insert_query = f"INSERT INTO index_data VALUES ({placeholders})"

        # Select data from the new_data database
        cur_new_data.execute("SELECT * FROM index_data")
        rows_new_data = cur_new_data.fetchall()

        # Select 'code' and 'date' pairs from the merged_last database
        cur_merged_new.execute("SELECT code, date FROM index_data")
        existing_keys = set((row[0], row[1]) for row in cur_merged_new.fetchall())

        # 10. Filter out rows from new_data that are already in merged_last
        filtered_rows = [row for row in rows_new_data if \
                         (row[columns.index('code')], row[columns.index('date')]) not in existing_keys]

        # Insert the selected data into the new merged database
        cur_merged_new.executemany(insert_query, filtered_rows)
        conn_merged_new.commit()

        # Recreate the indices on the new merged database
        cur_merged_new.execute("CREATE INDEX IF NOT EXISTS idx_code ON index_data (code)")
        cur_merged_new.execute("CREATE INDEX IF NOT EXISTS idx_date ON index_data (date)")

        # Commit changes and close the connections
        conn_merged_new.commit()
        conn_new_data.close()
        conn_merged_new.close()



