import os
from DateManage import DateManage
import configparser
import pandas as pd
import shutil

class MergeData:
    '''
   지난 데이터와 현재 데이터를 합치는 기능의 parent class
   '''

    def __init__(self, logger):
        self.logger = logger

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