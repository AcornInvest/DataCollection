import os
import logging
from datetime import date
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetOHLCV_Intelliquant import GetOHLCV_Intelliquant
from VerifyOHLCV import VerifyOHLCV
from multiprocessing import Process
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
#startday = date(2000, 1, 4)
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

#get_OHLCV_Intelliquant = GetOHLCV_Intelliquant(logger, paths, i, datemanage, flag_mod=False)
#get_OHLCV_Intelliquant.make_txt_from_ticker(datemanage)
#get_OHLCV_Intelliquant.intel.chrome_on(logger, get_OHLCV_Intelliquant.page, get_OHLCV_Intelliquant.name)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, all_files=False, first_index=1, final_index=1)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, all_files=True)
#get_OHLCV_Intelliquant.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리

#verify_ohlcv = VerifyOHLCV(logger, datemanage, flag_mod=False)
#verify_ohlcv.check_data(datemanage)


startday = date(2000, 1, 4)
workday = date(2025, 1, 14)
datemanage2 = DateManage(filename)
datemanage2.SetStartday(startday)
datemanage2.SetWorkday(workday)
get_OHLCV_Intelliquant2 = GetOHLCV_Intelliquant(logger, paths, i, datemanage, flag_mod=True)
#get_OHLCV_Intelliquant2.make_txt_from_ticker(datemanage2)
#get_OHLCV_Intelliquant2.intel.chrome_on(logger, get_OHLCV_Intelliquant2.page, get_OHLCV_Intelliquant2.name)
#get_OHLCV_Intelliquant2.run_backtest_rep(datemanage2, all_files=True)
get_OHLCV_Intelliquant2.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리

#verify_ohlcv2 = VerifyOHLCV(logger, datemanage, flag_mod=True)
#verify_ohlcv2.check_data(datemanage)


