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
        super().__init__()
        self.length_code_list = 20 # raw 데이터 텍스트 파일 1개당 포함된 데이터 개수
        self.batchsize = 20  # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수

        self.path_base_code = self.cur_dir + '\\' + 'GetNoOfShares_base.js'

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = json.loads(config['intelliquant']['page'])
        self.name = json.loads(config['intelliquant']['name'])

    def run_backtest_rep(self, datemanage):
        # 데이터를 하나씩 추출해서 인텔리퀀트의 코드 수정해가면서 백테스트 수행.
        #chrome_on()은 되어 있는 상태에서 호출

        data_indices = self.calculate_batch_indices(self.length_code_list, self.batchsize)

        category = ['Delisted', 'Listed']
        for type in category:
            self.path_dir = self.path_codeLists + '\\' + type + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'

            file_index 최대값 찾기

            for file_index in range(1, max_file_index): #폴더 내의 파일 갯수만큼 반복
                self.path_code = self.path_dir + 'A_Code_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
                self.path_listingdate = self.path_dir + 'A_ListingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
                self.path_delistingdate = self.path_dir + 'A_DelistingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'

                num_data_index = 1
                for index in data_indices: # 한 파일 내에서의 인덱스
                    js_code = GetCompData.make_js_code(datemanage, 'Delisted', file_index, index[0], index[1])

                # GetCompData.intel.update_code(js_code)
                # GetCompData.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', logger)


                # 백테스트 돌리고 각 백테스트 결과 파일 txt로 저장
                # 각 테스트 결과에서 종목별 추출, pd 를 xlsx로 저장
                # load_failure_list, DelistingDate_Error_list 를 하나의 txt로 만들어서 종목 코드 추가
                num_data_index++;


        '''
        category = ['Delisted', 'Listed']
        for type in category:
            sdf
        '''

        pass

    def calculate_batch_indices(self, length_code_list, batchsize):
        indices = []
        for start in range(0, length_code_list, batchsize):
            end = min(start + batchsize - 1, length_code_list - 1)
            indices.append((start, end))
        return indices



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
#GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 0, 19)
#js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 5, 10)
#GetCompData.intel.update_code(js_code)
#GetCompData.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', logger)
