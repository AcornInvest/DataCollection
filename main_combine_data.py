import os
import logging
from datetime import date
from DateManage import DateManage
from CombineData import CombineData

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = date(2000, 1, 4)
workday = date(2024, 3, 29)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

combine_data = CombineData(logger)
combine_data.combine_data(datemanage) # OHLCV, NoOfShare, Volume 폴더의 파일 목록과 codelist 맞는지 체크 후 하나로 합침


