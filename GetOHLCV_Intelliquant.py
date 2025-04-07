import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import date
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd
from pandas import Timedelta
import re

class GetOHLCV_Intelliquant(UseIntelliquant): # 인텔리퀀트에서 모든 종목 OHLCV 3일씩
    def __init__(self, logger, num_process, datemanage, flag_mod=False):
        super().__init__(logger, num_process, datemanage, flag_mod)

        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_batchsize = 60 # For_Intelliquant 파일 내의 code 숫자
        self.max_unit_year = 33
        #self.max_unit_year = 104  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24.5년 * 4종목 =  98 단위 + 마진 = 102으로 시뮬레이션 하도록 함
        #self.max_unit_year = 125  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24.5년 * 5종목 =  122.5 단위 + 마진 = 125으로 시뮬레이션 하도록 함
        #self.max_unit_year = 300  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24년 * 12종목 =  288 단위 + 마진 12 = 300으로 시뮬레이션 하도록 함
        self.path_base_code = self.cur_dir + '\\' + 'GetOHLCV_Intelliquant_base.js'

        if self.flag_mod:
            self.suffix = 'OHLCV_intelliquant_mod' # 수정주가가 발생된 경우
        else:
            self.suffix = 'OHLCV_intelliquant'  # 파일 이름 저장시 사용하는 접미사

        # 테이블 생성 쿼리. volume 은 저장하지 않음. volume 데이터가 따로 있으니까
        self.create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.suffix} (
            stock_code TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,   
            volume REAL,                   
            cap REAL           
        );
        '''
        # 2025.4.3 volume 은 get volume 에서 구하니까 삭제함

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetOHLCV_Intelliquant.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page_list = [config['intelliquant']['page_0'], config['intelliquant']['page_1'], config['intelliquant']['page_2'], config['intelliquant']['page_3']]
        self.name_list = [config['intelliquant']['name_0'], config['intelliquant']['name_1'], config['intelliquant']['name_2'], config['intelliquant']['name_3']]
        self.page = self.page_list[self.num_process]
        self.name = self.name_list[self.num_process]

        if self.flag_mod:
            self.path_backtest_save = config['path']['path_backtest_save_mod']  # 수정주가가 발생된 경우
            self.path_compensation_data = config['path']['path_compensation_data']
        else:
            self.path_backtest_save = config['path']['path_backtest_save']

    def process_backtest_result(self, path_file):  # backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        # OHLCV backtest 일반 데이터 패턴: 숫자가 5개 또는 6개 연속으로 있고, 그 뒤에 옵셔널하게 알파벳 문자가 1개 있는 것
        data_pattern = r'\[\d{4}-\d{2}-\d{2}\]\s\d{5,6}[A-Za-z]?,'
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'
        code_pattern = r'\] (\d{5}[A-Za-z]?|\d{6}),'
        open_pattern = r'O: (\d+(\.\d+)?),'
        high_pattern = r'H: (\d+(\.\d+)?),'
        low_pattern = r'L: (\d+(\.\d+)?),'
        close_pattern = r'C: (\d+(\.\d+)?),'
        volume_pattern = r'V: (\d+),'
        cap_pattern = r'cap: (\d+)'
        num_codes = 0
        num_stocks = 0
        num_load_failure_stocks = 0
        num_delisting_data_error_stocks = 0
        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                if re.search(data_pattern, line):  # 일반 데이터 처리
                    date = re.search(date_pattern, line).group(1)
                    code = re.search(code_pattern, line).group(1)
                    Open = re.search(open_pattern, line).group(1)
                    high = re.search(high_pattern, line).group(1)
                    low = re.search(low_pattern, line).group(1)
                    close = re.search(close_pattern, line).group(1)
                    volume = re.search(volume_pattern, line).group(1)
                    cap = re.search(cap_pattern, line).group(1)

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    # volume 은 get volume 에서도 구하지만 verify ohlcv 할 때 거래 정지 여부를 판단하는데 필요해서 넣는다.
                    data_by_code[code].append((date, Open, high, low, close, volume, cap))

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
            df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'cap'])

            # 날짜순으로 정렬
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df.sort_values('date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes

    '''
    def make_txt_from_ticker(self, datemanage):
        pass
        if self.flag_mod:
            overrided 하도록 함. 
    '''