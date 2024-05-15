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

filename = []
startday = []
workday = []
datemanage = []
logger = []

for i in range(2):
    filename.append(f'{os.path.splitext(os.path.basename(__file__))[0]}_Proc_{i}')  # 실행하고 있는 스크립트 파일 이름 가져오기
    startday.append(date(2000, 1, 4))
    workday.append(date(2024, 3, 29))
    datemanage.append(DateManage(filename[i]))
    datemanage[i].SetStartday(startday[i])
    datemanage[i].SetWorkday(workday[i])
    logger.append(logging.getLogger('main'))
    logger[i].setLevel(logging.INFO)
    file_handler_info = logging.FileHandler(filename=datemanage[i].path_log)
    file_handler_info.setFormatter(formatter)
    logger[i].addHandler(file_handler_info)

def run_process(index, datemanage, logger):
    get_OHLCV_Intelliquant = GetOHLCV_Intelliquant(logger, index)
    get_OHLCV_Intelliquant.intel.chrome_on(logger, get_OHLCV_Intelliquant.page, get_OHLCV_Intelliquant.name)
    get_OHLCV_Intelliquant.run_backtest_rep(datemanage, index * 5, index * 5 + 4)

if __name__ == "__main__":
    processes = []
    for i in range(2):  # 원하는 프로세스 수만큼 반복
        p = Process(target=run_process, args=(i, datemanage[i], logger[i]))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
