import os
import logging
from datetime import date
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetOHLCV_Intelliquant import GetOHLCV_Intelliquant
from GetVolume import GetVolume
from multiprocessing import Process

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

i = 3

filename = f'{os.path.splitext(os.path.basename(__file__))[0]}_Proc_{i}'  # 실행하고 있는 스크립트 파일 이름 가져오기
#startday = date(2000, 1, 4)
#workday = date(2007, 12, 31)
#startday = date(2008, 1, 1)
#workday = date(2015, 12, 31)
#startday = date(2016, 1, 1)
#workday = date(2024, 3, 29)
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

get_volume = GetVolume(logger, i)
get_volume.intel.chrome_on(logger, get_volume.page, get_volume.name)
#get_volume.run_backtest_rep(datemanage, 1487, 1501)


for num in [1384, 1456, 1499, 1500, 191
            ]:
    get_volume.run_backtest_rep(datemanage, num, num)

