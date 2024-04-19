import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import datetime
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd

class GetCompensationData(UseIntelliquant):
    def __init__(self, logger):
        super().__init__(logger)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_batchsize = 20 # Delisted
        self.max_batchsize = 10 # Listed
        self.path_base_code = self.cur_dir + '\\' + 'GetNoOfShares_base.js'

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCompensationData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_backtest_save = config['path']['path_backtest_save']

    def calculate_batch_indices(self, length_code_list, max_batchsize, listingdate_content, delistingdate_content, startday, workday): #run_backtest_rep() 에서 한번에 시뮬레이션 할 리스트 만들어서 리턴
        indices = []
        for start in range(0, length_code_list, max_batchsize):
            end = min(start + max_batchsize - 1, length_code_list - 1)
            indices.append((start, end))
        return indices

    def process_backtest_result(self, path_file):
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        with open(path_file, 'r') as file:
            for line in file:
                if 'list_index:' in line:
                    num_codes = int(line.split('list_index:')[1].strip())
                elif 'NumOfStocks:' in line:
                    num_stocks = int(line.split('NumOfStocks:')[1].strip())
                elif 'load_failure_list:' in line:
                    extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                    load_failure_stocks = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
                    num_load_failure_stocks = len(load_failure_stocks)
                elif 'DelistingDate_Error_list:' in line:
                    extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                    delisting_data_error_stocks = [x.strip() for x in extracted_data if x.strip()]
                    num_delisting_data_error_stocks = len(delisting_data_error_stocks)
                else:
                    # 일반 데이터 처리
                    parts = line.split(',')
                    date2 = parts[0].split("] ")[1]  # '날짜2' 추출
                    code = parts[1].strip()
                    old_no_share = int(parts[2].split(':')[1].strip())  # old_no_share 추출
                    new_no_share = int(parts[3].split(':')[1].strip())  # new_no_share 추출

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date2, old_no_share, new_no_share))

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print('backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info("Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d" % (path_file,num_codes,num_stocks,num_load_failure_stocks,num_delisting_data_error_stocks ))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        # 각 코드의 df 에서 old, new 맞나 체크하는 루틴
        # 문제가 없으면 각각 new 데이터만 남긴다
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['Date', 'OldNoShare', 'NewNoShare'])

            # OldNoShare를 한 칸 위로 shift한 후 NewNoShare와 비교. 마지막 행은 제외하고. backtest 시 누락된 데이터가 없는지 확인하는 용도
            df['ShiftedOldNoShare'] = df['OldNoShare'].shift(-1)
            comparison_result_without_last = (df['ShiftedOldNoShare'][:-1] == df['NewNoShare'][:-1])

            # 모든 결과가 True인지 확인하고, 아니라면 해당 행의 날짜를 str 변수에 저장
            if not comparison_result_without_last.all():
                # 불일치하는 행의 날짜 찾기
                mismatched_dates = df.loc[~comparison_result_without_last, 'Date']
                err_date = ', '.join(mismatched_dates)
                self.logger.info(f"주식수 데이터 에러. Code: {code}, date:{err_date}")

            del df['ShiftedOldNoShare']

            # 시뮬레이션 마지막 해에 load한 데이터로 인해 중복되는 데이터 처리
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df.sort_values('Date', inplace=True)

            # Drop duplicates keeping the last occurrence
            df.drop_duplicates(subset=['Date'], keep='last', inplace=True)

            # Use forward fill to create a column of the correct OldNoShare values
            df['CorrectOldNoShare'] = df['NewNoShare'].shift(1).ffill()

            # Filter out rows where the OldNoShare doesn't match the CorrectOldNoShare, except for the first occurrence
            # The first occurrence does not have a previous NewNoShare value, so it's considered correct
            df = df[(df['OldNoShare'] == df['CorrectOldNoShare']) | (df['Date'] == df['Date'].min())]

            # Drop the CorrectOldNoShare column as it is no longer needed
            df.drop(columns=['CorrectOldNoShare'], inplace=True)

            # Reset index
            df.reset_index(drop=True, inplace=True)

            dataframes[code] = df
        return dataframes

    def run_backtest_process(self, datemanage): # Backtest 결과를 가지고 No_shares 정보의 xlsx 파일로 처리
        # category = ['Delisted', 'Listed']
        category = ['Delisted']
        for listed_status in category:

            # 폴더에서 backtest 파일 이름 목록 찾기 --> file_names
            backtest_result_folder = self.path_backtest_save + '\\' + listed_status + '\\From_Intelliquant\\' + datemanage.workday_str + '\\'
            start_string = 'backtest_result_' + datemanage.workday_str
            file_names = self.get_files_starting_with(backtest_result_folder, start_string)

            # 처리 결과 저장할 폴더
            no_share_folder = self.path_backtest_save + '\\' + listed_status + '\\' + datemanage.workday_str + '\\'

            # 폴더가 존재하지 않으면 생성
            if not os.path.exists(no_share_folder):
                os.makedirs(no_share_folder)

            for backtest_result_file in file_names:
                path_backtest_result_file = backtest_result_folder + backtest_result_file
                df_no_share = self.process_backtest_result(path_backtest_result_file)
                self.save_dfs_to_excel(df_no_share, ('_compensation_' + datemanage.workday_str), no_share_folder)


            #처리한 엑셀 파일들이 Codelist에 있는 모든 종목들을 다 커버하는지 확인
            compensation_file_names = self.find_files_with_keyword(no_share_folder, 'compensation')  # compensation 처리 결과 파일 목록. compensation이 포함된 파일만 골라냄
            file_prefixes = set([name[:6] for name in compensation_file_names]) # 각 파일명의 처음 6글자 추출
            # 파일 처음 6글자가 숫자로 시작하는 것만으로 제한할 것

            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            codelist['DelistingDate'] = pd.to_datetime(codelist['DelistingDate'])
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            #codelist.loc[:, 'Code'] = codelist['Code'].apply(lambda x: f"{x:06d}")  # Code 열 str, 6글자로 맞추기
            codelist_filtered = codelist[codelist['DelistingDate'] >= datemanage.startday] # 상폐일이 기준일(2000.1.4) 보다 앞선 것은 제외시키기
            codes = set(codelist_filtered['Code']) # Ticker 파일에서 가져온 Code column

            if file_prefixes != codes or len(codelist_filtered) != len(file_prefixes):
                # DataFrame의 칼럼에는 있는데 파일 이름에 없는 값 목록
                missing_in_files = codes - file_prefixes

                # 파일 이름에는 있는데 DataFrame의 칼럼에 없는 값 목록
                extra_in_files = file_prefixes - codes

                # 결과를 텍스트 파일로 저장
                missing_in_files_path = no_share_folder + 'missing_in_files_' + datemanage.workday_str + '.txt'
                with open(missing_in_files_path, 'w') as f:
                    for item in missing_in_files:
                        f.write("%s\n" % item)

                extra_in_files_path = no_share_folder + 'extra_in_files_' + datemanage.workday_str + '.txt'
                with open(extra_in_files_path, 'w') as f:
                    for item in extra_in_files:
                        f.write("%s\n" % item)

                print("결과가 'missing_in_files.txt'와 'extra_in_files.txt'에 저장되었습니다.")
            else:
                print("모든 DataFrame의 칼럼 값이 파일 이름에 있습니다.")

    def save_dfs_to_excel(self, dfs_dict, custom_string, folder):
        for code, df in dfs_dict.items():
            filename = f"{code}{custom_string}.xlsx"
            path_file = folder + filename
            df.to_excel(path_file, index=False)

'''
오늘(log용), 기준일(tikerlist 받아온 작업일) 정보 필요

 # 필요한 파일 이름들이 뭐가 있지?
 로그
불러올 파일 - for intelliquant

저장할 파일 - intelliquant results, Noshare xls 파일

# 통합하는 프로그램에서는 필요한 파일 이름들이 뭐가 있지?
불러올 파일: intelliquant result, ohlcv results, tickerlist xls 파일
저장할 파일: ohlcv results
'''

'''
filename = os.path.splitext(os.path.basename(__file__))[0]  # 실행하고 있는 스크립트 파일 이름 가져오기
startday = datetime(2000, 1, 4)
workday = datetime(2024, 3, 29)
datemanage = DateManage(filename)
datemanage.SetStartday(startday)
datemanage.SetWorkday(workday)

logger = logging.getLogger('GetCompensationData')
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

GetCompData = GetCompensationData(logger)
GetCompData.intel.chrome_on(logger, GetCompData.page, GetCompData.name)
GetCompData.run_backtest_rep(datemanage) # 인텔리퀀트로 백테스트 돌려서 no_share raw data 크롤링
#GetCompData.run_backtest_process(datemanage) # 인텔리퀀트로 얻은 백테스트 raw 데이터 처리
'''
