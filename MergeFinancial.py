import os
from MergeData import MergeData

# financial merge
class MergeFinancial(MergeData):
    def __init__(self, logger, paths, datemanage, flag_mod=False):
        #financial 에서는 flag_mod 는 항상 False.  super()로 넘기지 않는다.
        self.path_data = paths.Financial
        self.suffix = 'financial'  # 파일 이름 저장시 사용하는 접미사
        self.datemanage = datemanage
        self.table_name = 'financial'

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


