import os
import logging
from datetime import date
from DateManage import DateManage
from GetFinancialData import GetFinancialData
from VerifyFinance import VerifyFinance
from LoadConfig import LoadConfig

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
#startday = date(2000, 1, 4)
#workday = date(2024, 3, 29)
startday = date(2023, 12, 1) # 직전 financial 결과가 나오는 날부터로 함
workday = date(2025, 1, 14)
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

i = 0

paths = LoadConfig()

GetFinancData = GetFinancialData(logger, paths, i, datemanage)
#GetFinancData.make_txt_from_ticker(datemanage)
#GetFinancData.intel.chrome_on(logger, GetFinancData.page, GetFinancData.name)
#GetFinancData.run_backtest_rep(datemanage,  all_files=False, first_index=1, final_index=1) # 인텔리퀀트로 백테스트 돌려서 크롤링
#GetFinancData.run_backtest_rep(datemanage, all_files=True) # 인텔리퀀트로 백테스트 돌려서 크롤링
GetFinancData.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리, db 파일로 만듦

verify_finance = VerifyFinance(logger, paths)
verify_finance.check_data(datemanage)
