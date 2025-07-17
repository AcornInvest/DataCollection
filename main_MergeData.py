import os
#from MergeData import MergeData
from MergeOHLCV import MergeOHLCV
from MergeFinancial import MergeFinancial
from DateManage import DateManage
from datetime import datetime
import logging
from LoadConfig import LoadConfig

# 3 formatter 지정하여 log head를 구성해줍니다.
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")

i = 0

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


# combine 로 먼저 continuity  확인 후 merge
merge_ohlcv = MergeOHLCV(logger, paths, datemanage, flag_mod=False)
flag_error, flag_mod_stocks = merge_ohlcv.check_continuity(datemanage)
print(f' \n OHLCV merge continuity check')
print(f'flag_error: {flag_error}, flag_mod_stocks: {flag_mod_stocks}')
#merge_ohlcv.merge_dbs()


#check_continuity() 를 해서 mod 가 필요하면 merge를 안한다.
#merge_dbs() 를 실행할 때 flag_mod=False인데 combined 폴더에 mod_stock_codes 가 있으면 안한다. --> 에러 출력
#이 경우, 사용자가 get ohlcv mod 를 해서 backtest를 하고, merge_ohlcv를 flag_mod=True 로 만들고 나서 merge_dbs()를 2회 호출해야 한다.



'''
# flag_mode_stocks == True 인 경우, mod_stock 도 merge
merge_ohlcv = MergeOHLCV(logger, paths, datemanage, flag_mod=True)
merge_ohlcv.merge_dbs()
'''

'''
# Financial
startday = date(2023, 12, 1) # 직전 financial 결과가 나오는 날부터로 함
workday = date(2025, 1, 14)
datemanage_3 = DateManage(filename)
datemanage_3.SetStartday(startday)
datemanage_3.SetWorkday(workday)

merge_financial = MergeFinancial(logger, paths, datemanage_3)
flag_error, flag_mod_stocks = merge_financial.check_continuity(datemanage_3)
print(f' \nFinancial merge continuity check')
print(f'flag_error: {flag_error}, flag_mode_stocks: {flag_mod_stocks}\n')
merge_financial.merge_dbs()
'''