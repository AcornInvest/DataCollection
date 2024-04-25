import os
import logging
from datetime import datetime
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = datetime(2000, 1, 4)
workday = datetime(2024, 3, 29)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)

logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
# 3 formatter 지정하여 log head를 구성해줍니다.
## asctime - 시간정보
## levelname - logging level
## funcName - log가 기록된 함수
## lineno - log가 기록된 line
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")
file_handler_info = logging.FileHandler(filename=datemanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(file_handler_info)

#GetCompData = GetCompensationData(logger)
#GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
#GetCompData.run_backtest_rep(datemanage) # 인텔리퀀트로 백테스트 돌려서 no_share raw data 크롤링
#GetCompData.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리


GetFinancData = GetFinancialData(logger)
GetFinancData.intel.chrome_on(logger, GetFinancData.page, GetFinancData.name)
GetFinancData.run_backtest_rep(datemanage) # 인텔리퀀트로 백테스트 돌려서 no_share raw data 크롤링
#GetFinancData.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리
