import os
from VerifyCompensation import VerifyCompensation
from DateManage import DateManage
from datetime import date
import logging

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

filename = f'{os.path.splitext(os.path.basename(__file__))[0]}'  # 실행하고 있는 스크립트 파일 이름 가져오기
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

verify_compensation = VerifyCompensation(logger)
#verify_compensation.check_data(datemanage, 'Listed')
verify_compensation.check_data(datemanage, 'Delisted')

