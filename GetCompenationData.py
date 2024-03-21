import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant

logger = logging.getLogger('GetCompensationData')
logger.setLevel(logging.INFO)
# 3 formatter 지정하여 log head를 구성해줍니다.
## asctime - 시간정보
## levelname - logging level
## funcName - log가 기록된 함수
## lineno - log가 기록된 line
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")
text_handler = LogStringHandler(self.LogText)
text_handler.setFormatter(formatter)
# file_handler_info = logging.FileHandler(filename="log_info.log")
file_handler_info = logging.FileHandler(filename=self.daymanage.path_log)
file_handler_info.setFormatter(formatter)
logger.addHandler(text_handler)
logger.addHandler(file_handler_info)

intel = Intelliquant()
intel.ChromeOn(logger, 'NoOfShares')
