import os
import logging
from datetime import date, datetime
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from VerifyCompensation import VerifyCompensation
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetOHLCV_Intelliquant import GetOHLCV_Intelliquant
from LoadConfig import LoadConfig

import sys
sys.path.append("C:\\Work_Dotori\\Screener_Desktop")

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

'''
get_compensation_data = GetCompensationData(logger, paths, i, datemanage)
get_compensation_data.make_txt_from_ticker(datemanage)
get_compensation_data.intel.chrome_on(logger, get_compensation_data.page, get_compensation_data.name)
#get_compensation_data.run_backtest_rep(datemanage, all_files=False, first_index=1, final_index=1)
get_compensation_data.run_backtest_rep(datemanage, all_files=True)
No_error = get_compensation_data.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리
'''


verify_compensation = VerifyCompensation(logger, paths)
verify_compensation.check_data(datemanage)





