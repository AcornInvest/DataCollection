import os
from DateManage import DateManage
import configparser
import pandas as pd
import shutil
import sqlite3
from MergeData import MergeData

# combined ohlcv (share, volume 포함) 를 merge
class MergeOHLCV(MergeData):
    def __init__(self, logger, paths, datemanage, flag_mod=False):
        #flag_mod == True 사용하는 경우: #merge_dbs()시 ohlcv_mod를 붙일 때
        #check_continuity() 시는 flag_mode False 로 한다.

        self.flag_mod = flag_mod

        self.suffix = 'combined_ohlcv'  # 파일 이름 저장시 사용하는 접미사
        if self.flag_mod:
            self.suffix_mod = 'OHLCV_intelliquant_mod'

        self.datemanage = datemanage
        self.path_data = paths.OHLCV_Combined
        self.path_ohlcv_mod = paths.OHLCV_Intelliquant_mod
        self.table_name = 'combined_ohlcv'

        super().__init__(logger, flag_mod)

    def load_config(self):
        # 폴더 경로
        self.folder_merged = self.path_data
        self.file_merged = f'{self.suffix}_merged.db'
        self.path_merged = self.folder_merged + '\\' + self.file_merged
        self.table_merged = self.suffix

        if self.flag_mod:
            self.folder_new = os.path.join(self.path_ohlcv_mod, f'{self.datemanage.workday_str}')
            self.file_new = f'{self.suffix_mod}_{self.datemanage.workday_str}.db'
            self.table_new = self.suffix_mod
        else:
            self.folder_new = os.path.join(self.path_data, f'{self.datemanage.workday_str}')
            self.file_new = f'{self.suffix}_{self.datemanage.workday_str}.db'
            self.table_new = self.suffix

        self.path_new = self.folder_new + '\\' + self.file_new


