import os
import logging
from datetime import date, datetime
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from LoadConfig import LoadConfig

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기

paths = LoadConfig()

startday = datetime(2000, 1, 4) # get ticker list 에서는 start 는 2000, 1, 4 로 고정함
#startday = datetime(2024, 3, 29)
#workday = datetime(2025, 1, 14)
workday = datetime(2025, 7, 11)
datemanage = DateManage(filename, paths)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)

logger = logging.getLogger('main')
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

get_ticker = GetTicker(logger)
get_ticker.make_raw_ticker_list(datemanage) # original ticker list 받기
get_ticker.process_tickerlist(datemanage) # original ticker list 데이터의 처리
get_ticker.check_code_list(datemanage) # process 된 ticker list check
