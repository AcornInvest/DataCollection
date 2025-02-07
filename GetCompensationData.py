import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import date
import configparser
from UseIntelliquant import UseIntelliquant
import pandas as pd
import re
import sqlite3

class GetCompensationData(UseIntelliquant):
    def __init__(self, logger, num_process):
        super().__init__(logger, num_process)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_batchsize = 20 # Delisted
        #self.max_batchsize = 10 # Listed
        #self.max_unit_year = 480  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 20종목*24년 = 480 유닛만큼 끊어서 시뮬레이션 하겠다는 의미
        self.max_unit_year = 4800 # 480*10
        self.path_base_code = self.cur_dir + '\\' + 'GetNoOfShares_base.js'
        self.suffix = 'compensation'  # 파일 이름 저장시 사용하는 접미사

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_backtest_save = config['path']['path_backtest_save']

    def process_backtest_result(self, path_file):  # backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        # OHLCV backtest 일반 데이터 패턴: 숫자가 5개 또는 6개 연속으로 있고, 그 뒤에 옵셔널하게 알파벳 문자가 1개 있는 것
        data_pattern = r'\[\d{4}-\d{2}-\d{2}\]\s\d{5,6}[A-Za-z]?,'
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'
        code_pattern = r'\] (\d{5}[A-Za-z]?|\d{6}),'
        old_pattern = r'o: (\d+),'
        new_pattern = r'n: (\d+)'
        num_codes = 0
        num_stocks = 0
        num_load_failure_stocks = 0
        num_delisting_data_error_stocks = 0
        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                if re.search(data_pattern, line):  # 일반 데이터 처리
                    date = re.search(date_pattern, line).group(1)
                    code = re.search(code_pattern, line).group(1)
                    old = re.search(old_pattern, line).group(1)
                    new = re.search(new_pattern, line).group(1)
                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date, old, new))
                elif 'list_index:' in line:
                    num_codes = int(line.split('list_index:')[1].strip())
                elif 'NumOfStocks:' in line:
                    num_stocks = int(line.split('NumOfStocks:')[1].strip())
                elif 'load_failure_list:' in line:
                    extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                    load_failure_stocks = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
                    num_load_failure_stocks = len(load_failure_stocks)
                elif 'DelistingDate_Error_list:' in line:
                    extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                    delisting_data_error_stocks = [x.strip() for x in extracted_data if x.strip()]
                    num_delisting_data_error_stocks = len(delisting_data_error_stocks)

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print(path_file, ': backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info(
                'Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d' % (
                path_file, num_codes, num_stocks, num_load_failure_stocks, num_delisting_data_error_stocks))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['date', 'old_share', 'new_share'])
            # 날짜순으로 정렬
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df.sort_values('date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes

    def make_sql(self, datemanage):
        # 처리 결과(sql) 저장할 폴더
        process_result_folder = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'

        # 처리 결과 폴더가 존재하지 않으면 생성
        if not os.path.exists(process_result_folder):
            os.makedirs(process_result_folder)

        # 테이블 생성 쿼리
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS compensation (
            stock_code TEXT,
            date TEXT,
            old_share REAL,
            new_share REAL           
        );
        '''

        # SQLite 데이터베이스 파일 연결 (없으면 새로 생성)
        #filename_db = f'{self.suffix}_{listed_status}_{datemanage.workday}.db'
        filename_db = f'{self.suffix}_{datemanage.workday}.db'
        file_path_db = process_result_folder + filename_db
        conn = sqlite3.connect(file_path_db)
        conn.execute(create_table_query)  # 테이블 생성
        conn.commit()

        # 인덱스 생성 (쿼리 성능 향상)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON compensation (stock_code);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON compensation (date);')
        conn.commit()
        # 데이터베이스 연결 종료
        conn.close()

    def add_data_to_sql(self, datemanage, df_processed_stock_data):
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday}.db'
        path_data = folder_data + file_data
        conn_data = sqlite3.connect(path_data)
        table_name = 'compensation'

        for key, df in df_processed_stock_data.items():
            # processed_data 를 db에 넣기
            df['stock_code'] = key  # 종목코드 열 추가
            df.to_sql('compensation', conn_data, if_exists='append', index=False)

       # 데이터베이스 연결 종료
        conn_data.close()

    def load_df_codes(self, datemanage):
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday}.db'
        path_data = folder_data + file_data
        conn = sqlite3.connect(path_data)
        table_name = 'compensation'

        # 종목 코드 목록 가져오기
        query = 'SELECT DISTINCT stock_code FROM compensation'
        stock_codes = pd.read_sql(query, conn)['stock_code'].tolist()

        return stock_codes

    def load_df(self, datemanage):
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday}.db'
        path_data = folder_data + file_data
        conn = sqlite3.connect(path_data)
        table_name = 'compensation'

        # 종목 코드 목록 가져오기
        query = 'SELECT DISTINCT stock_code FROM compensation'
        stock_codes = pd.read_sql(query, conn)['stock_code'].tolist()