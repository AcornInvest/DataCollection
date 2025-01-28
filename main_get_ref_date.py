import os
import logging
from datetime import date
from DateManage import DateManage
from GetCompensationData import GetCompensationData
from GetFinancialData import GetFinancialData
from GetTicker import GetTicker
from GetOHLCV import GetOHLCV
from GetDateRef import GetDateRef
from multiprocessing import Process

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

get_ref_date = GetDateRef(logger, i)
date_prefix = 'bussiness_day_ref'
get_ref_date.intel.chrome_on(logger, get_ref_date.page, get_ref_date.name)
path_backtest_result = get_ref_date.run_backtest(datemanage)
#path_backtest_result = 'F:\WorkDotori\StockDataset\date_reference\From_Intelliquant' + '\\' + 'backtest_result_' + datemanage.workday_str + '.txt'
df_date_ref_new = get_ref_date.process_backtest_result(path_backtest_result)
df_date_ref_combined = get_ref_date.merge_date_ref(datemanage, df_date_ref_new, date_prefix)

date_prefix = 'financial_day_ref'
df_financial_date_new = get_ref_date.make_financial_date_ref(df_date_ref_new)
df_financial_date_ref_combined = get_ref_date.merge_financial_date_ref(datemanage, df_financial_date_new, date_prefix)
get_ref_date.count_num_days_financial_date(datemanage, df_financial_date_ref_combined, df_date_ref_combined, date_prefix)

