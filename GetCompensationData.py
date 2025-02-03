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
            df = pd.DataFrame(data, columns=['Date', 'OldNoShare', 'NewNoShare'])
            # 날짜순으로 정렬
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df.sort_values('Date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes

    # 2024.6.11 기준 예전 코드
    '''
    def process_backtest_result(self, path_file): #backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        with open(path_file, 'r') as file:
            for line in file:
                if 'list_index:' in line:
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
                else:
                    # 일반 데이터 처리
                    parts = line.split(',')
                    date2 = parts[0].split("] ")[1]  # '날짜2' 추출
                    code = parts[1].strip()
                    old_no_share = int(parts[2].split(':')[1].strip())  # old_no_share 추출
                    new_no_share = int(parts[3].split(':')[1].strip())  # new_no_share 추출

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date2, old_no_share, new_no_share))

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print('backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info("Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d" % (path_file,num_codes,num_stocks,num_load_failure_stocks,num_delisting_data_error_stocks ))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        # 각 코드의 df 에서 old, new 맞나 체크하는 루틴
        # 문제가 없으면 각각 new 데이터만 남긴다
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['Date', 'OldNoShare', 'NewNoShare'])

            # OldNoShare를 한 칸 위로 shift한 후 NewNoShare와 비교. 마지막 행은 제외하고. backtest 시 누락된 데이터가 없는지 확인하는 용도
            df['ShiftedOldNoShare'] = df['OldNoShare'].shift(-1)
            comparison_result_without_last = (df['ShiftedOldNoShare'][:-1] == df['NewNoShare'][:-1])

            # 모든 결과가 True인지 확인하고, 아니라면 해당 행의 날짜를 str 변수에 저장
            if not comparison_result_without_last.all():
                # 불일치하는 행의 날짜 찾기
                mismatched_dates = df.loc[~comparison_result_without_last, 'Date']
                err_date = ', '.join(mismatched_dates)
                self.logger.info(f"주식수 데이터 에러. Code: {code}, date:{err_date}")

            del df['ShiftedOldNoShare']

            # 시뮬레이션 마지막 해에 load한 데이터로 인해 중복되는 데이터 처리
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df.sort_values('Date', inplace=True)

            # Drop duplicates keeping the last occurrence
            df.drop_duplicates(subset=['Date'], keep='last', inplace=True)

            # Use forward fill to create a column of the correct OldNoShare values
            df['CorrectOldNoShare'] = df['NewNoShare'].shift(1).ffill()

            # Filter out rows where the OldNoShare doesn't match the CorrectOldNoShare, except for the first occurrence
            # The first occurrence does not have a previous NewNoShare value, so it's considered correct
            df = df[(df['OldNoShare'] == df['CorrectOldNoShare']) | (df['Date'] == df['Date'].min())]

            # Drop the CorrectOldNoShare column as it is no longer needed
            df.drop(columns=['CorrectOldNoShare'], inplace=True)

            # Reset index
            df.reset_index(drop=True, inplace=True)

            dataframes[code] = df
        return dataframes
        '''