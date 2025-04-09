import os
from DateManage import DateManage
import configparser
import pandas as pd
import shutil

# 이걸 parent 로 하고, ohlcv, financial, processed, technical 로 자식 class 를 만든다.
# listed_stock_data, chancelist 는 1년/6개월에 한번만 추가하면 된다.


class MergeData:
    '''
   지난 데이터와 현재 데이터를 합치는 기능의 parent class
   '''

    def __init__(self, logger, datemanage, flag_mod=False):
        self.logger = logger
        self.flag_mod = flag_mod  # ohlcv share 변경된 데이터 대상 확인 용도
        # 설정 로드
        self.load_config() # 자식클래스에서 정의할 것
        # self.suffix = 'data' # 파일 이름 저장시 사용하는 접미사. 자식 클래스에서 정의할 것

    def check_continuity(self):
        folder1 = f'{self.path_data}\\'
        file1 = f'{self.suffix}_merged.db'
        path1 = folder1 + file1
        folder2 = f'{self.path_data}\\{self.datemanage.workday_str}\\'
        file2 = f'{self.suffix}_{self.datemanage.workday_str}.db'
        path2 = folder2 + file2



    def merge_dbs(self):




    def merge_files(self, listed_status, date_before, date_recent, path_data, suffix):
        folder_data_before = f'{path_data}\\{listed_status}\\{date_before}_merged'
        folder_data_recent = f'{path_data}\\{listed_status}\\{date_recent}'
        folder_data_result = f'{path_data}\\{listed_status}\\{date_recent}_merged'

        # Ensure result folder exists
        os.makedirs(folder_data_result, exist_ok=True)

        file_names_data_before = [f for f in os.listdir(folder_data_before) if suffix in f]
        file_names_data_recent = [f for f in os.listdir(folder_data_recent) if suffix in f]

        codes_before = {f.split('_')[0]: f for f in file_names_data_before}
        codes_recent = {f.split('_')[0]: f for f in file_names_data_recent}

        only_in_before = set(codes_before.keys()) - set(codes_recent.keys())
        only_in_recent = set(codes_recent.keys()) - set(codes_before.keys())
        in_both = set(codes_before.keys()) & set(codes_recent.keys())

        # Save only_in_before list to a text file
        with open(os.path.join(folder_data_result, f'only_existed_in_before_{date_before}.txt'), 'w') as file:
            for code in only_in_before:
                file.write(f'{code}\n')

        # Save only_in_recent list to a text file
        with open(os.path.join(folder_data_result, f'only_existed_in_recent_{date_recent}.txt'), 'w') as file:
            for code in only_in_recent:
                file.write(f'{code}\n')

        # Process only_in_before files
        for code in only_in_before:
            src_file = os.path.join(folder_data_before, codes_before[code])
            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            shutil.copy2(src_file, dest_file)

        # Process only_in_recent files
        for code in only_in_recent:
            src_file = os.path.join(folder_data_recent, codes_recent[code])
            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            shutil.copy2(src_file, dest_file)

        # Process files in both
        for code in in_both:
            file_before = os.path.join(folder_data_before, codes_before[code])
            file_recent = os.path.join(folder_data_recent, codes_recent[code])

            df_before = pd.read_excel(file_before)
            df_recent = pd.read_excel(file_recent)

            # Combine dataframes and drop duplicates
            combined_df = pd.concat([df_before, df_recent]).drop_duplicates(subset='Date').reset_index(drop=True)

            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            combined_df.to_excel(dest_file, index=False)

        print(f'{folder_data_before}, {folder_data_recent} 파일 merge 완료')
        self.logger.info(f'{folder_data_before}, {folder_data_recent} 파일 merge 완료')