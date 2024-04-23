import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import datetime
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd
from pandas import Timedelta
import re

class GetFinancialData(UseIntelliquant):
    def __init__(self, logger):
        super().__init__(logger)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_batchsize = 20 # Delisted
        self.max_batchsize = 20 # Listed. list txt 파일이 20개 종목씩 되어 있음
        self.max_unit_year = 100  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 100유닛만큼 끊어서 시뮬레이션 하겠다는 의미
        self.path_base_code = self.cur_dir + '\\' + 'get_financials_base.js'

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetFinancialData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_backtest_save = config['path']['path_backtest_save']

    def calculate_batch_indices(self, length_code_list, max_batchsize, listingdate_content, delistingdate_content, startday, workday): #run_backtest_rep() 에서 한번에 시뮬레이션 할 리스트 만들어서 리턴
        listingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", listingdate_content)
        delistingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", delistingdate_content)
        listingdate_list_timestamp = pd.to_datetime(listingdate_list)
        delistingdate_list_timestamp = pd.to_datetime(delistingdate_list)
        adjusted_listingdate_list_timestamp = [ts if ts >= startday else startday for ts in listingdate_list_timestamp]
        adjusted_delistingdate_list_timestamp = [ts if ts <= workday else workday for ts in delistingdate_list_timestamp]

        unit_year_list = []
        td_year = Timedelta(days=365)
        for listingdate, delistingdate in zip(adjusted_listingdate_list_timestamp, adjusted_delistingdate_list_timestamp):
            unit_year = (delistingdate - listingdate)/td_year
            unit_year_list.append(unit_year)

        indices = []
        sum_unit_year = 0
        start_index = 0
        end_index = 0
        for index, unit_year in enumerate(unit_year_list):
            if(sum_unit_year + unit_year > self.max_unit_year):
                end_index = index - 1
                indices.append((start_index, end_index))
                start_index = index
                sum_unit_year=0
            sum_unit_year += unit_year

        if sum_unit_year > 0:
            indices.append((start_index, len(unit_year_list) - 1))

        # 인텔리퀀트 start date, end date도 계산해서 리턴할까? 근데 그건 처음에 전체 데이터 읽어올 때만 좀 유용하고 업데이트할 때는 큰 의미가 없다.
        return indices

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