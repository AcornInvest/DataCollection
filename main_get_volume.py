import os
import logging
from datetime import date, datetime
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetOHLCV_Intelliquant import GetOHLCV_Intelliquant
from GetVolume import GetVolume
from VerifyVolume import VerifyVolume
from multiprocessing import Process
from LoadConfig import LoadConfig

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

i = 1

filename = f'{os.path.splitext(os.path.basename(__file__))[0]}_Proc_{i}'  # 실행하고 있는 스크립트 파일 이름 가져오기

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

paths = LoadConfig()

'''
get_volume = GetVolume(logger, paths, i, datemanage)
get_volume.make_txt_from_ticker(datemanage)
get_volume.intel.chrome_on(logger, get_volume.page, get_volume.name)
#get_volume.run_backtest_rep(datemanage, all_files=False, first_index=0, final_index=1)
get_volume.run_backtest_rep(datemanage, all_files=True)
get_volume.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리
'''


verify_volume = VerifyVolume(logger, paths)
verify_volume.check_data(datemanage)
