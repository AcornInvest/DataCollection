import os
import logging
from datetime import date
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetOHLCV_Intelliquant import GetOHLCV_Intelliquant
from multiprocessing import Process

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

i = 0

filename = f'{os.path.splitext(os.path.basename(__file__))[0]}_Proc_{i}'  # 실행하고 있는 스크립트 파일 이름 가져오기
#startday = date(2000, 1, 4)
#workday = date(2007, 12, 31)
startday = date(2008, 1, 1)
workday = date(2015, 12, 31)
#startday = date(2016, 1, 1)
#workday = date(2024, 3, 29)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

get_OHLCV_Intelliquant = GetOHLCV_Intelliquant(logger, i)
get_OHLCV_Intelliquant.intel.chrome_on(logger, get_OHLCV_Intelliquant.page, get_OHLCV_Intelliquant.name)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, 111, 128) # workday = date(2024, 3, 29)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, 0, 128) # workday = date(2024, 3, 29)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, 0, 89) # workday = date(2015, 12, 31)
#get_OHLCV_Intelliquant.run_backtest_rep(datemanage, 356, 356) # workday = date(2007, 12, 31)

for num in [167, 36, 219, 178, 65, 74, 210, 98, 158, 152, \
            154, 169, 199, 72, 63, 48, 1, 184, 215, 34, \
            161,412, 43, 408, 190, 67, 78, 188, 174, 88, \
            182, 21, 69, 186, 76, 156, 83]:
    get_OHLCV_Intelliquant.run_backtest_rep(datemanage, num, num)
