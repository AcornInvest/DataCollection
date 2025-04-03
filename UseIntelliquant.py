import os
from Intelliquant import Intelliquant
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from pandas import Timedelta
import re
import sqlite3

class UseIntelliquant:
    '''
   인텔리퀀트에서 데이터 가져오는 기능
   '''

    def __init__(self, logger, num_process, datemanage):
        self.num_process = num_process  # 멀티 프로세스 번호
        self.intel = Intelliquant(self.num_process)
        self.logger = logger

        # 설정 로드
        self.load_config()
        self.suffix = 'data' # 파일 이름 저장시 사용하는 접미사
        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사

        # 거래일 목록 ref 읽어오기. 기본. 전체 영업일.
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        self.df_business_days = pd.read_excel(path_date_ref)
        self.df_business_days['date'] = pd.to_datetime(self.df_business_days['date']).dt.date
        self.df_business_days = self.df_business_days[(self.df_business_days['date'] >= datemanage.startday) & (self.df_business_days['date'] <= datemanage.workday)]

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_UseIntelliquant.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_data = config['path']['path_data']
        self.path_codeLists = config['path']['path_codelists']
        self.path_date_ref = config['path']['path_date_ref']  # 날짜 기준 정보 경로

    def load_base_code(self, path_base_code):
        with open(path_base_code, 'r', encoding='utf-8') as file:
            js_code_base = file.read()
        #print(js_code_base)
        return js_code_base

    def create_js_code_dataset(self, start_date_str, end_date_str, code_content, listingdate_content,
                               delistingdate_content):
        return (
            f"//Dataset Begin\n"
            f"var StartDate = new Date('{start_date_str}');\n"
            f"var FinalDate = new Date('{end_date_str}');\n\n"
            f"var code = [\n{code_content}\n];\n"
            f"var ListingDate = [\n{listingdate_content}\n];\n"
            f"var DelistingDate = [\n{delistingdate_content}\n];\n"
            f"//Dataset End\n\n"
        )

    def parse_data_to_list(self, data_str): #format_string_data() 에서 사용.
        # 줄바꿈과 콤마를 기준으로 데이터를 리스트로 변환
        return [item.strip() for item in data_str.replace('\n', '').split(',')]

    def format_string_data(self, data_str, start, end, line_length=5): # load_dataset_code 시 한 파일 내의 데이터 중 일부를 추출
        # 데이터 리스트로 변환
        data_list = self.parse_data_to_list(data_str)

        # 추출된 데이터 범위
        extracted_data = data_list[start:end + 1]

        # 5개씩 끊어서 새로운 문자열로 변환
        formatted_data = ''
        for i in range(0, len(extracted_data), line_length):
            line_data = ','.join(extracted_data[i:i + line_length])
            if i + line_length < len(extracted_data):
                line_data += ','
            formatted_data += line_data + '\n'

        # 마지막 줄의 불필요한 줄바꿈 제거
        return formatted_data.rstrip()

    def load_dataset_code(self, datemanage: DateManage, file_index: int):
        # index에 해당하는 code, listing date, delisting date 값 불러와서 리턴
        path_code = self.path_for_intelliquant_dir + 'A_Code_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_listingdate = self.path_for_intelliquant_dir + 'A_ListingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_delistingdate = self.path_for_intelliquant_dir + 'A_DelistingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'

        with open(path_code, 'r') as file:
            code_content = file.read()
            items = code_content.split(',')
            length_code_list = len(items)
        # print(code_content)
        with open(path_listingdate, 'r') as file:
            listingdate_content = file.read()
            items = listingdate_content.split(',')
            length_listingdate_list = len(items)
        # print(listingdate_content)
        with open(path_delistingdate, 'r') as file:
            delistingdate_content = file.read()
            items = delistingdate_content.split(',')
            length_delistingdate_list = len(items)
        # print(delistingdate_content)

        if length_code_list == length_listingdate_list == length_delistingdate_list:
            print("length_code_list: ", length_code_list)
        else:
            raise ValueError(datemanage.workday_str, "_", file_index, "의 파일간 데이터 리스트 수가 다름")

        return length_code_list, code_content, listingdate_content, delistingdate_content

    def run_backtest_rep(self, datemanage, all_files=True, first_index=None, final_index=None):
        # 데이터를 하나씩 추출해서 인텔리퀀트의 코드 수정해가면서 백테스트 수행.
        #chrome_on()은 되어 있는 상태에서 호출

        category = ['Delisted', 'Listed']
        #category = ['Delisted']
        #category = ['Listed']
        for listed_status in category:
            #self.path_for_intelliquant_dir = self.path_backtest_save + '\\' + listed_status + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'
            self.path_for_intelliquant_dir = self.path_backtest_save + '\\' + datemanage.workday_str + '\\' + listed_status + '\\For_Intelliquant\\'

            # max_file_index(폴더 내 데이터 파일 수) 계산
            # 파일 무리별 카운터 초기화
            count_code = 0
            count_delisting = 0
            count_listing = 0

            # 폴더 내의 모든 파일에 대해 반복
            for filename in os.listdir(self.path_for_intelliquant_dir):
                if filename.startswith('A_Code_') and datemanage.workday_str in filename:
                    count_code += 1
                elif filename.startswith('A_DelistingDate_') and datemanage.workday_str in filename:
                    count_delisting += 1
                elif filename.startswith('A_ListingDate_') and datemanage.workday_str in filename:
                    count_listing += 1

            # 카운터 값이 모두 같은지 확인하고, 아니면 에러 발생
            if count_code == count_delisting == count_listing:
                max_file_index = count_code
                print("모든 파일 무리의 개수가 동일합니다. ", max_file_index)
            else:
                raise ValueError("파일 무리의 개수가 서로 다릅니다.")

            if all_files:
                file_range = range(0, max_file_index)
            else:
                file_range = range(first_index, final_index + 1)

            for file_index in file_range:
            #for file_index in range(first_index, final_index + 1):
            #for file_index in range(1, max_file_index+1): #폴더 내의 파일 갯수만큼 반복
                length_code_list, code_content, listingdate_content, delistingdate_content = self.load_dataset_code(datemanage, file_index)

                listingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", listingdate_content)
                delistingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", delistingdate_content)
                # 각 종목별로 시뮬레이션 결과가 출력되는 기간의 날짜 계산(상장일, 상폐일을 startdat, workday와 비교)
                listingdate_list_datetime = pd.to_datetime(listingdate_list)
                delistingdate_list_datetime = pd.to_datetime(delistingdate_list)
                listingdate_list_date = [dt.date() for dt in listingdate_list_datetime]
                delistingdate_list_date = [dt.date() for dt in delistingdate_list_datetime]
                adjusted_listingdate_list_date = [ts if ts >= datemanage.startday else datemanage.startday for ts in listingdate_list_date]
                adjusted_delistingdate_list_date = [ts if ts <= datemanage.workday else datemanage.workday for ts in delistingdate_list_date]

                # 상장일과 상폐일에 따라 시뮬레이션 시작일과 마지막 날 정하기.
                earliest_date = min(adjusted_listingdate_list_date) # 가장 빠른 날짜 추출
                latest_date = max(adjusted_delistingdate_list_date) # 가장 늦은 날짜 추출
                # 날짜를 'YYYY-MM-DD' 형식의 문자열로 변환
                start_date_str = earliest_date.strftime('%Y-%m-%d')
                end_date_str = latest_date.strftime('%Y-%m-%d')
                #start_date_str = '2000-01-04'
                #end_date_str = '2000-01-10'
                js_code_dataset = self.create_js_code_dataset(start_date_str, end_date_str, code_content, listingdate_content, delistingdate_content)
                js_code_base = self.load_base_code(self.path_base_code)
                js_code = js_code_dataset + js_code_base
                self.intel.update_code(js_code) #인텔리퀀트 코드창 수정
                #backtest_list = self.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', self.logger)
                backtest_list = self.intel.backtest(start_date_str, end_date_str, '10000000', self.logger)
                self.save_backtest_result(self.path_backtest_save, backtest_list, listed_status, datemanage, file_index)

    def save_backtest_result(self, path_backtest_save, backtest_list, listed_status, datemanage, file_index):
        #path_compensation 변수 파라미터로 받아오자
        #self.path_backtest_result = path_backtest_save + '\\' + listed_status + '\\From_Intelliquant\\' + datemanage.workday_str + '\\' + 'backtest_result_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        self.path_backtest_result = path_backtest_save + '\\' + datemanage.workday_str + '\\' + listed_status + '\\From_Intelliquant\\' + 'backtest_result_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        folder = os.path.dirname(self.path_backtest_result)
        # 폴더가 존재하지 않으면 생성
        if not os.path.exists(folder):
            os.makedirs(folder)

        self.path_load_failure_list = folder + '\\' + 'load_failure_list_' + datemanage.workday_str + '.txt'
        self.path_delisting_date_error_list = folder + '\\' + 'delisting_date_error_list_' + datemanage.workday_str + '.txt'

        # 각 백테스트 결과 파일 txt로 저장
        '''
        f = open(self.path_backtest_result, 'w', encoding='utf-8')
        f.writelines(backtest_list)
        f.close()
        '''

        # 파일에 저장
        with open(self.path_backtest_result, 'w', encoding='utf-8') as file:
            '''
            for text in backtest_list:
                file.write(text + '\n')
            '''
            file.write('\n'.join(backtest_list) + '\n')
        self.logger.info("Backtest 결과 저장 완료: %s" % self.path_backtest_result)

        #load_failure_list, delisting_date_error_list 저장
        load_failure_list, delisting_date_error_list = self.parse_list_data(backtest_list)
        if load_failure_list:
            utils.save_list_to_file_append(load_failure_list, self.path_load_failure_list)
            self.logger.info("load_failure_list 결과 저장 완료: %s" % self.path_load_failure_list)
        if delisting_date_error_list:
            utils.save_list_to_file_append(delisting_date_error_list, self.path_delisting_date_error_list)
            self.logger.info("delisting_date_error_list 결과 저장 완료: %s" % self.path_delisting_date_error_list)

    def parse_list_data(self, lines):
        load_failure_list = []
        delisting_date_error_list = []

        for line in lines:
            if "load_failure_list:" in line:
                extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                load_failure_list = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
            elif "DelistingDate_Error_list:" in line:
                extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                delisting_date_error_list = [x.strip() for x in extracted_data if x.strip()]

        return load_failure_list, delisting_date_error_list

    '''
    # txt 파일에 데이터를 추가하는 함수
    def save_list_to_file_append(self, data_list, filename):
        with open(filename, 'a') as file:  # 'a' 모드는 파일에 내용을 추가합니다
            for item in data_list:
                file.write(f"{item}\n")
    '''

    # 특정 폴더 내에서 특정 문자열로 시작하는 파일들의 이름을 리스트로 반환하는 함수
    def get_files_starting_with(self, folder_path, start_string):
        files_starting_with = []
        for filename in os.listdir(folder_path):
            if filename.startswith(start_string):
                files_starting_with.append(filename)
        return files_starting_with

    '''
    def save_dfs_to_excel(self, dfs_dict, custom_string, folder):
        for code, df in dfs_dict.items():
            filename = f"{code}{custom_string}.xlsx"
            path_file = folder + filename
            df.to_excel(path_file, index=False)
    '''

    ## 20250205 이전. 코드별로 xlsx 파일로 정리
    '''
    def run_backtest_process(self, datemanage): # Backtest 결과를 가지고 xlsx 파일로 처리
        category = ['Delisted', 'Listed']
        #category = ['Delisted']
        #category = ['Listed']
        for listed_status in category:
            # 폴더에서 backtest 파일 이름 목록 찾기 --> file_names
            backtest_result_folder = self.path_backtest_save + '\\' + listed_status + '\\From_Intelliquant\\' + datemanage.workday_str + '\\'
            start_string = 'backtest_result_' + datemanage.workday_str
            #start_string = 'backtest_result_' + datemanage.workday_str + '_90'
            #start_string = 'backtest_result_' + datemanage.workday_str + '_482'
            file_names = self.get_files_starting_with(backtest_result_folder, start_string)

            # 처리 결과 저장할 폴더
            process_result_folder = self.path_backtest_save + '\\' + listed_status + '\\' + datemanage.workday_str + '\\'
            #process_result_folder = self.path_backtest_save + '\\' + listed_status + '\\' + datemanage.workday_str + '_temp\\'

            # 폴더가 존재하지 않으면 생성
            if not os.path.exists(process_result_folder):
                os.makedirs(process_result_folder)

            for backtest_result_file in file_names:
                path_backtest_result_file = backtest_result_folder + backtest_result_file
                df_processed_stock_data = self.process_backtest_result(path_backtest_result_file)
                utils.save_dfs_to_excel(df_processed_stock_data, ('_' + self.suffix + '_' + datemanage.workday_str), process_result_folder)

            #처리한 엑셀 파일들이 Codelist에 있는 모든 종목들을 다 커버하는지 확인
            processed_file_names = utils.find_files_with_keyword(process_result_folder, self.suffix)  # 데이터 처리 결과 파일 목록. 특정 suffix가 포함된 파일만 골라냄
            file_prefixes = set([name[:6] for name in processed_file_names]) # 각 파일명의 처음 6글자 추출
            # 파일 처음 6글자가 숫자로 시작하는 것만으로 제한할 것

            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            codelist['ListingDate'] = pd.to_datetime(codelist['ListingDate']).apply(lambda x: x.date())
            codelist['DelistingDate'] = pd.to_datetime(codelist['DelistingDate']).apply(lambda x: x.date())
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            #codelist.loc[:, 'Code'] = codelist['Code'].apply(lambda x: f"{x:06d}")  # Code 열 str, 6글자로 맞추기
            #codelist_filtered = codelist[codelist['DelistingDate'] >= datemanage.startday] # 상폐일이 시뮬레이션 시작일보다 앞선 것은 제외시키기
            # 상폐일이 시뮬레이션 시작일보다 늦고, 상장일이 시뮬레이션 마지막 날보다 빠른 것만 남기기
            codelist_filtered = codelist[(codelist['DelistingDate'] >= datemanage.startday) & (codelist['ListingDate'] <= datemanage.workday)]
            codes = set(codelist_filtered['Code']) # Ticker 파일에서 가져온 Code column

            if file_prefixes != codes or len(codelist_filtered) != len(file_prefixes):
                # DataFrame의 칼럼에는 있는데 파일 이름에 없는 값 목록
                missing_in_files = codes - file_prefixes

                # 파일 이름에는 있는데 DataFrame의 칼럼에 없는 값 목록
                extra_in_files = file_prefixes - codes

                # 결과를 텍스트 파일로 저장
                missing_in_files_path = process_result_folder + 'missing_in_files_' + datemanage.workday_str + '.txt'
                with open(missing_in_files_path, 'w') as f:
                    for item in missing_in_files:
                        f.write("%s\n" % item)

                extra_in_files_path = process_result_folder + 'extra_in_files_' + datemanage.workday_str + '.txt'
                with open(extra_in_files_path, 'w') as f:
                    for item in extra_in_files:
                        f.write("%s\n" % item)

                print("결과가 'missing_in_files.txt'와 'extra_in_files.txt'에 저장되었습니다.")
            else:
                print("모든 DataFrame의 칼럼 값이 파일 이름에 있습니다.")
    '''

    ## 20250205 이후. 모든 코드 데이터를 모아서 날짜별 db로 저장
    def run_backtest_process(self, datemanage):  ## 20250205 이후. 모든 코드 데이터를 모아서 날짜별 db로 저장
        category = ['Delisted', 'Listed']
        # category = ['Delisted']
        # category = ['Listed']

        # 해당 날짜 sql db 저정할 파일 생성
        self.make_sql(datemanage)

        for listed_status in category:
            # 폴더에서 backtest 파일 이름 목록 찾기 --> file_names
            #backtest_result_folder = self.path_backtest_save + '\\' + listed_status + '\\From_Intelliquant\\' + datemanage.workday_str + '\\'
            backtest_result_folder = self.path_backtest_save + '\\' + datemanage.workday_str + '\\' + listed_status + '\\From_Intelliquant\\'
            start_string = 'backtest_result_' + datemanage.workday_str
            file_names = self.get_files_starting_with(backtest_result_folder, start_string)

            # backtest result 파일 읽어와서 처리하기
            for backtest_result_file in file_names:
                path_backtest_result_file = backtest_result_folder + backtest_result_file
                df_processed_stock_data = self.process_backtest_result(path_backtest_result_file)
                self.add_data_to_sql(datemanage, df_processed_stock_data)

        ## 처리한 엑셀 파일들이 Codelist에 있는 모든 종목들을 다 커버하는지 확인
        stock_codes = self.load_df_codes(datemanage) # 해당일 db로 저장한 데이터의 코드명 읽어오기
        stock_codes_set = set()
        stock_codes_set.update(stock_codes)

        ticker_codes_set = set()
        num_ticker_codes = 0

        # ticker list 에서 전체 코드명 읽어오기
        for listed_status in category:
            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            codelist['ListingDate'] = pd.to_datetime(codelist['ListingDate']).apply(lambda x: x.date())
            codelist['DelistingDate'] = pd.to_datetime(codelist['DelistingDate']).apply(lambda x: x.date())
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            # codelist.loc[:, 'Code'] = codelist['Code'].apply(lambda x: f"{x:06d}")  # Code 열 str, 6글자로 맞추기
            # codelist_filtered = codelist[codelist['DelistingDate'] >= datemanage.startday] # 상폐일이 시뮬레이션 시작일보다 앞선 것은 제외시키기
            # 상폐일이 시뮬레이션 시작일보다 늦고, 상장일이 시뮬레이션 마지막 날보다 빠른 것만 남기기
            codelist_filtered = codelist[
                (codelist['DelistingDate'] >= datemanage.startday) &  #상폐가 startday 이후
                (codelist['ListingDate'] <= datemanage.workday) & # 상장이 workday 이전
                (codelist['ListingDate'] <= self.df_business_days['date'].iloc[-1]) #상장이 마지막 business day 이전
            ]
            num_ticker_codes += len(codelist_filtered)
            ticker_codes_set.update(codelist_filtered['Code'])
            #codes = set(codelist_filtered['Code'])  # Ticker 파일에서 가져온 Code column

        #if stock_codes_set != ticker_codes_set or len(codelist_filtered) != len(stock_codes_set):
        if stock_codes_set != ticker_codes_set or num_ticker_codes != len(stock_codes_set):
            # ticker_list에 있는데 칼럼에는 있는데 db파일에 없는 코드 목록
            missing_in_files = ticker_codes_set - stock_codes_set

            # db파일에 있는데 칼럼에는 있는데 ticker_list에 없는 코드 목록
            extra_in_files = stock_codes_set - ticker_codes_set

            process_result_folder = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'

            # 결과를 텍스트 파일로 저장
            missing_in_files_path = process_result_folder + 'missing_in_files_' + datemanage.workday_str + '.txt'
            with open(missing_in_files_path, 'w') as f:
                for item in missing_in_files:
                    f.write("%s\n" % item)

            extra_in_files_path = process_result_folder + 'extra_in_files_' + datemanage.workday_str + '.txt'
            with open(extra_in_files_path, 'w') as f:
                for item in extra_in_files:
                    f.write("%s\n" % item)

            print("결과가 'missing_in_files.txt'와 'extra_in_files.txt'에 저장되었습니다.")
            return False
        else:
            print("모든 DataFrame의 칼럼 값이 파일 이름에 있습니다.")
            return True

    def make_txt_from_ticker(self, datemanage):
        category = ['Listed', 'Delisted']
        #category = ['Listed']
        #category = ['Delisted']

        for type_list in category:
            # 엑셀 파일 불러올 경로
            file_path = self.path_codeLists + f'\\{type_list}\\{type_list}_Ticker_{datemanage.workday_str}_modified.xlsx'
            stocks = pd.read_excel(file_path, index_col=0)

            stocks['Code'] = stocks['Code'].astype(str)
            stocks['Code'] = stocks['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.date
            stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.date

            # 상폐일이 startday 보다 빠르거나 상장일이 workday 보다 늦은 행은 제외
            stocks = stocks[(stocks['DelistingDate'] >= datemanage.startday) & (stocks['ListingDate'] <= datemanage.workday)]

            # Intelliquant 를 위한 칼럼
            stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
            stocks['A_ListingDate'] = "new Date('" + stocks['ListingDate'].apply(lambda x: x.strftime('%Y-%m-%d')) + "')"
            stocks['A_DelistingDate'] = "new Date('" + stocks['DelistingDate'].apply(lambda x: x.strftime('%Y-%m-%d')) + "')"

            stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
            stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋

            # txt 파일 저장할 경로
            base_path = f'{self.path_backtest_save}\\{datemanage.workday_str}\\{type_list}\\For_Intelliquant\\'
            #base_path = f'{self.path_backtest_save}\\{type_list}\\For_Intelliquant\\{datemanage.workday_str}_temp\\'
            if not os.path.exists(base_path):
                os.makedirs(base_path)

            data_indices = self.calculate_batch_indices(stocks, datemanage.startday, datemanage.workday)

            for column in ['A_Code', 'A_ListingDate', 'A_DelistingDate']:
                for idx, (start_index, end_index) in enumerate(data_indices):
                    subset = stocks[column].iloc[start_index:end_index + 1]
                    file_name = f"{column}_{datemanage.workday_str}_{idx}.txt"
                    with open(base_path + file_name, 'w') as f:
                        for idx2, val in enumerate(subset):
                            f.write(val)
                            if (idx2 + 1) % 5 == 0 and idx2 != len(subset) - 1:
                                f.write(',\n')
                            elif idx2 != len(subset) - 1:
                                f.write(',')

    def calculate_batch_indices(self, stocks, startday, workday): #stocks 를 보고 max_unit_year에 맞는 indices list 만들어 리턴
        # 각 종목별로 시뮬레이션 결과가 출력되는 기간 목록 생성(상장일~상폐일 혹은 startday~workday)
        adjusted_listingdate_list = [ts if ts >= startday else startday for ts in stocks['ListingDate']]
        adjusted_delistingdate_list = [ts if ts <= workday else workday for ts in stocks['DelistingDate']]

        # index 생성
        unit_year_list = []
        td_year = Timedelta(days=365)
        '''
        for listingdate, delistingdate in zip(adjusted_listingdate_list, adjusted_delistingdate_list):
            unit_year = (delistingdate - listingdate)/td_year
            unit_year_list.append(unit_year)
        '''
        for listingdate, delistingdate in zip(adjusted_listingdate_list, adjusted_delistingdate_list):
            delta = delistingdate - listingdate
            unit_year = delta.days / 365  # 또는 365.25 를 사용하여 윤년 고려 가능
            if unit_year < 0:
                unit_year = 0
            unit_year_list.append(unit_year)

        indices = []
        sum_unit_year = 0
        start_index = 0
        end_index = 0
        for index, unit_year in enumerate(unit_year_list):
            if(sum_unit_year + unit_year > self.max_unit_year):
                end_index = index - 1
                indices.append((start_index, end_index))
                start_index = index
                sum_unit_year=0
            sum_unit_year += unit_year

        if sum_unit_year > 0:
            indices.append((start_index, len(unit_year_list) - 1))

        return indices

    def make_sql(self, datemanage):
        # 처리 결과(sql) 저장할 폴더
        process_result_folder = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'

        # 처리 결과 폴더가 존재하지 않으면 생성
        if not os.path.exists(process_result_folder):
            os.makedirs(process_result_folder)

        # SQLite 데이터베이스 파일 연결 (없으면 새로 생성)
        #filename_db = f'{self.suffix}_{listed_status}_{datemanage.workday}.db'
        filename_db = f'{self.suffix}_{datemanage.workday}.db'
        file_path_db = process_result_folder + filename_db
        conn = sqlite3.connect(file_path_db)
        conn.execute(self.create_table_query)  # 테이블 생성
        conn.commit()

        # 인덱스 생성 (쿼리 성능 향상)
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_stock_code ON {self.suffix} (stock_code);')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_date ON {self.suffix} (date);')
        conn.commit()
        # 데이터베이스 연결 종료
        conn.close()

    def add_data_to_sql(self, datemanage, df_processed_stock_data):
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday}.db'
        path_data = folder_data + file_data
        conn_data = sqlite3.connect(path_data)
        #table_name = 'compensation'

        for key, df in df_processed_stock_data.items():
            # processed_data 를 db에 넣기
            df['stock_code'] = key  # 종목코드 열 추가
            df.to_sql(self.suffix, conn_data, if_exists='append', index=False)

       # 데이터베이스 연결 종료
        conn_data.close()

    def load_df_codes(self, datemanage):
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_backtest_save}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday}.db'
        path_data = folder_data + file_data
        conn = sqlite3.connect(path_data)

        # 종목 코드 목록 가져오기
        query = f'SELECT DISTINCT stock_code FROM {self.suffix}'
        stock_codes = pd.read_sql(query, conn)['stock_code'].tolist()

        return stock_codes