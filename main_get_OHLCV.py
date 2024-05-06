import os
import logging
from datetime import date
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV

filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = date(2000, 1, 4)
workday = date(2024, 3, 29)
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

get_OHLCV = GetOHLCV(logger)
#get_OHLCV.run_get_OHLCV_original(datemanage)
get_OHLCV.process_OHLCV_original(datemanage, 'Listed')


# path_date_reference = f"{path_OHLCV_init}\\Listed\\{datemanage.workday_str}\\date_reference_{datemanage.workday_str}.xlsx"
# df_date_reference = pd.read_excel(path_date_reference, index_col=0)
# df_date_reference['Date'] = pd.to_datetime(df_date_reference['ListingDate']).dt.strftime('%Y-%m-%d')

