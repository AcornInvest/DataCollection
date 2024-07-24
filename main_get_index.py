import os
import logging
from datetime import date
from DateManage import DateManage
from GetIndex import GetIndex

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = date(2000, 1, 4)
workday = date(2024, 3, 29)
datemanage = DateManage(filename)
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

get_index = GetIndex(logger)
start_date = startday.strftime('%Y-%m-%d')
end_date = workday.strftime('%Y-%m-%d')
get_index.get_index(start_date, end_date, datemanage)


