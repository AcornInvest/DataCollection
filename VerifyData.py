import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
import sqlite3

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

    #def check_data(self, datemanage, listed_status):
    def check_data(self, datemanage): #2025.3.17 listed_status 구분 없이 합쳐서 db에 저장함
        # 거래일 목록 ref 읽어오기
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        df_business_days = pd.read_excel(path_date_ref)
        df_business_days['date'] = pd.to_datetime(df_business_days['date']).dt.date
        df_business_days = df_business_days[(df_business_days['date'] >= datemanage.startday) & (df_business_days['date'] <= datemanage.workday)]

        flag_no_error = True  # 에러가 없다고 플래그 초기값 설정

        # 코드리스트 읽어오기
        codelist_path = f'{self.path_codeLists}\\Listed\\Listed_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist_listed = pd.read_excel(codelist_path, index_col=0)
        df_codelist_listed['Code'] = df_codelist_listed['Code'].astype(str)
        df_codelist_listed['Code'] = df_codelist_listed['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist_listed['ListingDate'] = pd.to_datetime(df_codelist_listed['ListingDate']).dt.date
        df_codelist_listed['DelistingDate'] = pd.to_datetime(df_codelist_listed['DelistingDate']).dt.date

        codelist_path = f'{self.path_codeLists}\\Delisted\\Delisted_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist_delisted = pd.read_excel(codelist_path, index_col=0)
        df_codelist_delisted['Code'] = df_codelist_delisted['Code'].astype(str)
        df_codelist_delisted['Code'] = df_codelist_delisted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist_delisted['ListingDate'] = pd.to_datetime(df_codelist_delisted['ListingDate']).dt.date
        df_codelist_delisted['DelistingDate'] = pd.to_datetime(df_codelist_delisted['DelistingDate']).dt.date

        df_codelist = pd.concat([df_codelist_listed, df_codelist_delisted], ignore_index=True)
        df_codelist = df_codelist[(df_codelist['DelistingDate'] >= datemanage.startday)] # 상폐가 startdat 보다 빠른것 제외

        codes = set(df_codelist['Code'])  # Ticker 파일에서 가져온 Code column

        ## db에서 파일 읽어오기
        #  불러올 데이터 db 경로
        folder_data = f'{self.path_data}\\{datemanage.workday_str}\\'
        file_data = f'{self.suffix}_{datemanage.workday_str}.db'
        path_data = folder_data + file_data
        conn_data = sqlite3.connect(path_data)
        table_name = 'compensation'

        # 종목 코드 목록 가져오기
        query = f'SELECT DISTINCT stock_code FROM {table_name}'
        stock_codes = pd.read_sql(query, conn_data)['stock_code'].tolist()
        stock_codes = set(stock_codes)

        # 두 set이 다를 경우에만 각 set에만 존재하는 값 계산 후 파일에 저장
        if codes != stock_codes:
            only_in_codes = codes - stock_codes
            only_in_stock_codes = stock_codes - codes

            path = folder_data + f"unique_values.txt"
            with open(path, "w", encoding="utf-8") as file:
                file.write("ticker list에만 있는 값:\n")
                for value in only_in_codes:
                    file.write(str(value) + "\n")

                file.write("\ncompensation db에만 있는 값:\n")
                for value in only_in_stock_codes:
                    file.write(str(value) + "\n")

            print(f"tickerlist와 db의 코드목록이 다름. {path} 파일에 결과가 저장되었습니다.")
            flag_no_error = False
            return flag_no_error

        column_load_from_data = ['date', 'old_share', 'new_share'] # compensation db 에서 읽어올 컬럼

        df_modified_codes = pd.DataFrame(columns=['stock_code'])
        for index, row in df_codelist.iterrows():
            listing_date = row['ListingDate']
            delisting_date = row['DelistingDate']
            # df_business_days에서 listing_date, startday 중 나중 날짜와 delisting_date 사이의 날짜 추출
            # df_b_day_ref = df_business_days[(df_business_days['Date'] >= listing_date) & (df_business_days['Date'] <= delisting_date)]
            df_b_day_ref = df_business_days[  (df_business_days['date'] >= listing_date) & (df_business_days['date'] <= delisting_date)].copy()

            ''' # 2025.3.24 위의 줄에서 startday ~ workday 이내로 한정시키니까 이 부분은 필요없어 보인다.
            # 시작일과 종료일 조정 --> 변경 필요
            df_b_day_ref['date'] = df_b_day_ref['date'].apply(lambda x: max(x, datemanage.startday))
            df_b_day_ref['date'] = df_b_day_ref['date'].apply(lambda x: min(x, datemanage.workday))
            '''

            # 코드에 해당하는 데이터 불러와서 무결성 검사
            code = row['Code']

            # db에서 파일 읽어오기
            query = f"SELECT {', '.join(column_load_from_data)} FROM '{table_name}' WHERE date >= '{datemanage.startday}' AND \
                                    date <= '{datemanage.workday}' AND stock_code = '{code}'"
            df_data = pd.read_sql(query, conn_data)

            #no_error = self.check_integrity(code, df_b_day_ref, df_data, datemanage, listed_status)  # 무결성 검사. 자식클래스에서 선언할 것
            no_error, flag_modified = self.check_integrity(code, df_b_day_ref, df_data, datemanage)  # 무결성 검사. 자식클래스에서 선언할 것

            if flag_modified:
                df_modified_codes.loc[len(df_modified_codes)] = code

            if no_error == False:
                flag_no_error = False

        if flag_no_error:
            print(f"{self.suffix} Verificaion No Error")
        else:
            print(f"{self.suffix} Verificaion Error")

        if len(df_modified_codes) > 0:
            path = folder_data + f"share_modified_codes_{datemanage.workday_str}.xlsx"
            df_modified_codes.to_excel(path, index=False)
            print(f"주식수 변동 종목들 저장됨: {path}")