import os
from DateManage import DateManage
import configparser
import pandas as pd
import shutil
import sqlite3
from MergeData import MergeData

# combined ohlcv (share, volume 포함) 를 merge
class MergeOHLCV(MergeData):
    def __init__(self, logger, paths, flag_mod=False):
        #flag_mod == True 사용하는 경우
        #check_continuity() 시 ohlcv를 검사할 때. share 에 차이가 있는 종목이 하나라도 있을 때.
        #merge_dbs()시 ohlcv_mod를 붙일 때. 이 때는 아예 merge_dbs_mod() 함수를 따로 만들어야겠다.

        super().__init__(logger, flag_mod)
        self.suffix = 'combined_ohlcv'  # 파일 이름 저장시 사용하는 접미사

        self.path_data = paths.OHLCV_Combined
        self.path_compensation_data = paths.OHLCV_Compensation
        self.table_name = 'combined_ohlcv'

    def load_config(self):
        pass