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

class GetCompensationData(UseIntelliquant):
    def __init__(self):
        super().__init__(logger)
        #self.length_code_list = 20 # raw 데이터 텍스트 파일 1개당 포함된 데이터 개수
        self.batchsize = 20  # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수

        self.path_base_code = self.cur_dir + '\\' + 'GetNoOfShares_base.js'

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']

    def process_backtest_result(self, path_file):
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        with open(path_file, 'r') as file:
            for line in file:
                if 'list_index:' in line:
                    num_codes = int(line.split('list_index:')[1].strip())
                    #other_data.append(('list_index', num_codes))
                elif 'NumOfStocks:' in line:
                    num_stocks = int(line.split('NumOfStocks:')[1].strip())
                    #other_data.append(('NumOfStocks', num_stocks))
                elif 'load_failure_list:' in line:
                    load_failure_stocks = line.split('load_failure_list:')[1].strip()
                    num_load_failure_stocks = len(load_failure_stocks)
                    #other_data.append(('load_failure_list', load_failure_stocks))
                elif 'DelistingDate_Error_list:' in line:
                    delisting_data_error_stocks = line.split('DelistingDate_Error_list:')[1].strip()
                    num_delisting_data_error_stocks = len(delisting_data_error_stocks)
                    #other_data.append(('DelistingDate_Error_list', delisting_data_error_stocks))
                else:
                    # 일반 데이터 처리
                    parts = line.split(',')
                    date2 = parts[1].strip()[1:-1]  # '날짜2' 추출
                    code = parts[2].strip()
                    old_no_share = int(parts[3].split(':')[1].strip())  # old_no_share 추출
                    new_no_share = int(parts[4].split(':')[1].strip())  # new_no_share 추출

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date2, old_no_share, new_no_share))

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print('backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info("Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d" % (path_file,num_codes,num_stocks,num_load_failure_stocks,num_delisting_data_error_stocks ))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['Date', 'OldNoShare', 'NewNoShare'])
            dataframes[code] = df

        return dataframes

    def run_backtest_process(self, datemanage): # Backtest 결과를 가지고 No_shares 정보의 xlsx 파일로 처리
        # category = ['Delisted', 'Listed']
        category = ['Delisted']
        for type in category:
            # 폴더에서 backtest 파일 이름 목록 찾기 --> file_names
            backtest_result_folder = self.path_compensation + '\\' + type + '\\From_Intelliquant\\' + datemanage.workday_str + '\\'
            start_string = 'backtest_result_' + datemanage.workday_str
            file_names = self.get_files_starting_with(backtest_result_folder, start_string)

            # 처리 결과 저장할 폴더
            no_share_folder = self.path_compensation + '\\' + type + datemanage.workday_str + '\\'
            # 폴더가 존재하지 않으면 생성
            if not os.path.exists(no_share_folder):
                os.makedirs(no_share_folder)

            for backtest_result_file in file_names:
                path_backtest_result_file = backtest_result_folder + backtest_result_file
                df_no_share = self.process_backtest_result(path_backtest_result_file)
                # 여기서 각 코드의 df 에서 old, new 맞나 체크하는 루틴이 필요하다
                # 문제가 없으면 각각 new 데이터만 남기면 된다.
                self.save_dfs_to_excel(df_no_share, ('_' + datemanage.workday_str), no_share_folder)

    def save_dfs_to_excel(self, dfs_dict, custom_string, folder):
        for code, df in dfs_dict.items():
            filename = f"{code}_{custom_string}.xlsx"
            path_file = folder + filename
            df.to_excel(path_file, index=False)

'''
오늘(log용), 기준일(tikerlist 받아온 작업일) 정보 필요

 # 필요한 파일 이름들이 뭐가 있지?
 로그
불러올 파일 - for intelliquant

저장할 파일 - intelliquant results, Noshare xls 파일

# 통합하는 프로그램에서는 필요한 파일 이름들이 뭐가 있지?
불러올 파일: intelliquant result, ohlcv results, tickerlist xls 파일
저장할 파일: ohlcv results
'''

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = datetime(2000, 1, 4)
workday = datetime(2024, 3, 21)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)

logger = logging.getLogger('GetCompensationData')
logger.setLevel(logging.INFO)
# 3 formatter 지정하여 log head를 구성해줍니다.
## asctime - 시간정보
## levelname - logging level
## funcName - log가 기록된 함수
## lineno - log가 기록된 line
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

GetCompData = GetCompensationData()
#GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
#GetCompData.run_backtest_rep(datemanage) # 인텔리퀀트로 백테스트 돌려서 no_share raw data 크롤링
GetCompData.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리

#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 0, 19)
#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 5, 10)
#GetCompData.intel.update_code(js_code)
#GetCompData.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', logger)
