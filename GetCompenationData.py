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

    def process_backtest_result(self, backtest_list, type, datemanage, file_index, idx):
        self.path_backtest_result = self.path_compensation + '\\' + type + '\\From_Intelliquant\\' + datemanage.workday_str + '\\' + 'backtest_result_' + datemanage.workday_str + '_' + str(file_index) + '_' + str(idx) + '.txt'
        folder = os.path.dirname(self.path_backtest_result)
        # 폴더가 존재하지 않으면 생성
        if not os.path.exists(folder):
            os.makedirs(folder)

        self.path_load_failure_list = folder + '\\' + 'load_failure_list_' + datemanage.workday_str + '.txt'
        self.path_delisting_date_error_list = folder + '\\' + 'delisting_date_error_list_' + datemanage.workday_str + '.txt'

        # 각 백테스트 결과 파일 txt로 저장
        f = open(self.path_backtest_result, 'w', encoding='utf-8')
        f.writelines(backtest_list)
        f.close()
        self.logger.info("Backtest 결과 저장 완료: %s" % self.path_backtest_result)

        #load_failure_list, delisting_date_error_list 저장
        load_failure_list, delisting_date_error_list = self.parse_list_data(backtest_list)
        if load_failure_list: # 여기 들어오는지 확인할 것
            self.save_list_to_file_append(load_failure_list, self.path_load_failure_list)
            self.logger.info("load_failure_list 결과 저장 완료: %s" % self.path_load_failure_list)
        if delisting_date_error_list:
            self.save_list_to_file_append(delisting_date_error_list, self.path_delisting_date_error_list)
            self.logger.info("delisting_date_error_list 결과 저장 완료: %s" % self.path_delisting_date_error_list)

        # 각 테스트 결과에서 종목별 추출, pd 를 xlsx로 저장

    def parse_list_data(self, lines):
        load_failure_list = []
        delisting_date_error_list = []

        for line in lines:
            if "load_failure_list:" in line:
                extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                load_failure_list = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
            elif "DelistingDate_Error_list:" in line:
                extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                delisting_date_error_list = [x.strip() for x in extracted_data if x.strip()]

        return load_failure_list, delisting_date_error_list

    # 파일에 데이터를 추가하는 함수로 save_list_to_file 수정하기
    def save_list_to_file_append(self, data_list, filename):
        with open(filename, 'a') as file:  # 'a' 모드는 파일에 내용을 추가합니다
            for item in data_list:
                file.write(f"{item}\n")

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
startday = datetime(2023, 1, 4)
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
GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
GetCompData.run_backtest_rep(datemanage)
#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 0, 19)
#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 5, 10)
#GetCompData.intel.update_code(js_code)
#GetCompData.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', logger)
