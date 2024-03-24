import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import datetime
import configparser
from UseIntelliquant import UseIntelliquant
import json

class GetCompensationData(UseIntelliquant):
    def __init__(self):
        super().__init__()
        self.batchsize = 20  # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = json.loads(config['intelliquant']['page'])
        self.name = json.loads(config['intelliquant']['name'])

    def load_dataset_code(self, datemanage: DateManage, listed: str, file_index: int, start_num:int, end_num:int): # index에 해당하는 code, listing date, delisting date 합성하여 str 리턴
        path_dir = self.path_codeLists + '\\' + listed + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'
        path_code = path_dir + 'A_Code_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_listingdate = path_dir + 'A_ListingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_delistingdate = path_dir + 'A_DelistingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'

        with open(path_code, 'r') as file:
            code_content = file.read()
        #print(code_content)
        with open(path_listingdate, 'r') as file:
            listingdate_content = file.read()
        #print(listingdate_content)
        with open(path_delistingdate, 'r') as file:
            delistingdate_content = file.read()
        #print(delistingdate_content)

        js_code_dataset = self.create_js_code_dataset(datemanage.startday_str, datemanage.workday_str, code_content, listingdate_content, delistingdate_content)
        return js_code_dataset
        #financial 에서는 batch에 따른 구조 변경 필요

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
GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
js_code = GetCompData.make_js_code(datemanage, 'Delisted', 1, 0, 19)
GetCompData.intel.update_code(js_code)
