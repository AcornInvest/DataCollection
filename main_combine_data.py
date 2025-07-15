import os
import logging
from datetime import date, datetime
from DateManage import DateManage
from CombineData import CombineData
from LoadConfig import LoadConfig

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
paths = LoadConfig()


#startday = datetime(2000, 1, 4)
#workday = datetime(2024, 3, 29)
#startday = datetime(2024, 3, 29)
#workday = datetime(2025, 1, 14)
startday = datetime(2025, 1, 14)
workday = datetime(2025, 7, 11)
datemanage = DateManage(filename, paths)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

combine_data = CombineData(logger, paths, datemanage)
combine_data.combine_data(datemanage) # OHLCV, NoOfShare, Volume 폴더의 파일 목록과 codelist 맞는지 체크 후 하나로 합침


