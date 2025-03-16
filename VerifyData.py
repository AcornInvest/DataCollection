import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils

class VerifyData:
    '''
   OHLCV, Financial data의 무결성 검증용 parent class
   '''
    def __init__(self, logger):
        self.logger = logger
        # 설정 로드
        self.load_config()
        #self.suffix = 'data' # 파일 이름 저장시 사용하는 접미사. 자식 클래스에서 정의할 것

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_VerifyData.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        #self.path_data = config['path']['path_data'] # 이건 자식 클래스에서 정의할 것
        self.path_codeLists = config['path']['path_codelists']

    def check_data(self, datemanage, listed_status):
        # 거래일 목록 ref 읽어오기
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        df_business_days = pd.read_excel(path_date_ref)
        df_business_days['Date'] = pd.to_datetime(df_business_days['Date']).dt.date

        # 코드리스트 읽어오기
        codelist_path = f'{self.path_codeLists}\\{listed_status}\\{listed_status}_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist = pd.read_excel(codelist_path, index_col=0)
        df_codelist['Code'] = df_codelist['Code'].astype(str)
        df_codelist['Code'] = df_codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist['ListingDate'] = pd.to_datetime(df_codelist['ListingDate']).dt.date
        df_codelist['DelistingDate'] = pd.to_datetime(df_codelist['DelistingDate']).dt.date

        flag_no_error = True # 에러가 없다고 플래그 초기값 설정
        for index, row in df_codelist.iterrows():
            listing_date = row['ListingDate']
            delisting_date = row['DelistingDate']
            # df_business_days에서 listing_date와 delisting_date 사이의 날짜 추출
            # df_b_day_ref = df_business_days[(df_business_days['Date'] >= listing_date) & (df_business_days['Date'] <= delisting_date)]
            df_b_day_ref = df_business_days[
                (df_business_days['date'] >= listing_date) & (df_business_days['date'] <= delisting_date)].copy()

            # 시작일과 종료일 조정
            df_b_day_ref['date'] = df_b_day_ref['date'].apply(lambda x: max(x, datemanage.startday))
            df_b_day_ref['date'] = df_b_day_ref['date'].apply(lambda x: min(x, datemanage.workday))

            # 코드에 해당하는 데이터 불러와서 무결성 검사
            code = row['Code']

            # db에서 파일 읽어오기
            여기 짜야 함





            #path_file = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\{code}_{self.suffix}_{datemanage.workday_str}.xlsx"
            #path_file = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\{code}_{self.suffix}_{datemanage.workday_str}_merged.xlsx" # 임시

            # 데이터 완전성 검사 - 모든 code 에 해당하는 데이터가 다 있는지
            no_error = True
            if os.path.exists(path_file):
                df_data = pd.read_excel(path_file, index_col=0)
                no_error = self.check_integrity(code, df_b_day_ref, df_data, datemanage, listed_status)  # 무결성 검사. 자식클래스에서 선언할 것
            else:
                path = f"{self.path_data}\\{datemanage.workday_str}\\{self.suffix}_not_existed_list.txt" #2025.3.16 수정
                #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\{self.suffix}_not_existed_list.txt"
                #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\{self.suffix}_not_existed_list.txt" # 임시
                data_not_existed = [code]
                utils.save_list_to_file_append(data_not_existed, path)  # 텍스트 파일에 오류 부분 저장
                print(f"{self.suffix}, {code} 데이터가 없음")
                self.logger.info(f"{self.suffix}, {code} 데이터가 없음")

            if no_error == False:
                flag_no_error = False

        if flag_no_error:
            print("에러 없음")