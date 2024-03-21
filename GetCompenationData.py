import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import datetime
import configparser

class GetCompensationData:
    '''
    인텔리퀀트에서 데이터 가져오는 기능
    '''
    def __init__(self):
        self.startday = datetime(2000, 1, 4)
        self.workday = datetime(2024, 3, 21) # 이거 날짜 관리 어떻게 하지? datemanage를 쓴다? 그럼 클래스 생성자 인수로 받아야지
        self.finalday = self.workday
        self.startday_string = self.startday.strftime('%Y-%m-%d')
        self.workday_string = self.workday.strftime('%Y-%m-%d')
        self.finalday_string = self.finalday.strftime('%Y-%m-%d')

        self.intel = Intelliquant()
        self.BatchSize_NoOfShares = 20 # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수
        self.BatchSize_Financial = 10 # 인텔리퀀트 시뮬레이션 재무정보 조회시 한번에 돌리는 종목 수

        # 설정 로드
        self.LoadConfig()

    def LoadConfig(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_data = config['path']['path_data']
        self.path_CodeLists = config['path']['path_CodeLists']
        self.path_Compensation = config['path']['path_Compensation']

    def LoadCodeList(self, listed, index): # index에 해당하는 code, listing date, delisting date 합성하여 str 리턴
        path_dir = self.path_CodeLists + '\\' + listed + '\\For_Intelliquant\\' + self.workday_string + '\\'
        path_code = path_dir + 'A_Code_' + self.workday_string + '_' + str(index) + '.txt'
        print(path_dir)
        print(path_code)

        return


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
workday = datetime(2024, 3, 21)
datemanage = DateManage(filename, workday)

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
GetCompData.LoadCodeList('Delisted', 1)
