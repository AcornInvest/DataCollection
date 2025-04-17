import os
#from MergeData import MergeData
from MergeOHLCV import MergeOHLCV
from DateManage import DateManage
from datetime import date
import logging
from LoadConfig import LoadConfig

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

i = 0

filename = f'{os.path.splitext(os.path.basename(__file__))[0]}_Proc_{i}'  # 실행하고 있는 스크립트 파일 이름 가져오기
#startday = date(2000, 1, 4)
#workday = date(2007, 12, 31)
#startday = date(2008, 1, 1)
#workday = date(2015, 12, 31)
#startday = date(2016, 1, 1)
#workday = date(2024, 3, 29)
startday = date(2024, 3, 29)
workday = date(2025, 1, 14)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

paths = LoadConfig()
merge_ohlcv = MergeOHLCV(logger, paths, datemanage, flag_mod=True)
flag_error, flag_mod_stocks = merge_ohlcv.check_continuity(datemanage)
print(f'flag_error: {flag_error}, flag_mode_stocks: {flag_mod_stocks}')