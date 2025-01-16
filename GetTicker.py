import FinanceDataReader as fdr
#from pykrx import stock
#import yfinance as yf
import pandas as pd
import numpy as np
import os
import configparser
from string import ascii_uppercase
from datetime import date
import re
from datetime import datetime

class GetTicker:
    '''
    작업일(오늘) 기준 상장종목, 상폐종목의 이름 코드, 상장일, 상폐일 dataframe 리턴 (original ticker list)
    original ticker list 파일 처리

    '''

    def __init__(self, logger):
        self.logger = logger

        # 설정 로드
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetTicker.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.path_codeLists = config['path']['path_codelists']

    def get_listingstocks(self): # 작업일(오늘) 기준 상장종목 코드, 이름, 상장일, 상폐일 dataframe 리턴
        stocks = fdr.StockListing('KRX-DESC')  # 코스피, 코스닥, 코넥스 전체
        # stocks = fdr.StockListing('KRX')  # 코스피, 코스닥, 코넥스 전체  - 이러면 listing date 정보가 없다.
        # stocks = fdr.StockListing('KOSPI')  # 코스피 전체  - 이러면 listing date 정보가 없다.
        cond = stocks['Market'] != 'KONEX'
        stocks = stocks.loc[cond]  # market == 'KONEX' 제외
        stocks = stocks[['Code', 'Name', 'ListingDate']]  # ocde, name, listingDate 칼럼만 남기기
        stocks = stocks[stocks['Code'].str.endswith('0')]  # 'Code'의 값이 '0'으로 끝나는 행만 남깁니다 --> 우선주 삭제
        ts = pd.to_datetime("2100-01-01")  # DelistingDate = 2100-01-01로 설정하면서 추가
        stocks['DelistingDate'] = ts
        # ListingDate, DelistingDate 열에서 연도-월-일 형식의 날짜만 추출
        stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d')
        stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d')
        stocks = stocks[~stocks['Name'].str.contains('스팩')]  # 스팩 종목 제거
        stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
        stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋

        '''
        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
            '%Y-%m-%d') + "')"
        '''
        return stocks

    def get_delistingstocks(self): # 작업일(오늘) 기준 상폐종목 코드, 이름, 상장일, 상폐일 dataframe 리턴
        stocks = fdr.StockListing('KRX-DELISTING')  # 코스피, 코스닥, 코넥스 전체
        cond = stocks['Market'] != 'KONEX'
        stocks = stocks.loc[cond]  # market == 'KONEX' 제외
        cond = stocks['SecuGroup'] == '주권'
        stocks = stocks.loc[cond]  # SecuGroup == '주권' 만 남기기
        stocks.rename(columns={'Symbol': 'Code'}, inplace=True)  # symbol --> code로 변경
        stocks = stocks[['Code', 'Name', 'ListingDate', 'DelistingDate']]  # synbol, name, listingDate, DelistingDate 칼럼만 남기기
        stocks = stocks[stocks['Code'].str.endswith('0')]  # 'Code'의 값이 '0'으로 끝나는 행만 남깁니다 --> 우선주 삭제
        stocks = stocks[~stocks['Name'].str.contains('스팩')]  # 스팩 종목 제거
        # ListingDate, DelistingDate 열에서 연도-월-일 형식의 날짜만 추출
        stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d')
        stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d')
        stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
        stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋

        '''
        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
            '%Y-%m-%d') + "')"
        '''
        return stocks

    def make_raw_ticker_list(self, datemanage):
        date_str = datemanage.workday_str  # ticker 가져온 작업 기준일
        listed_stocks = self.get_listingstocks()

        base_path = self.path_codeLists + '\\Listed\\'
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        listed_stocks.to_excel(base_path + f"Listed_Ticker_{date_str}.xlsx")

        delisted_stocks = self.get_delistingstocks()

        base_path = "C:\\Work_Dotori\\StockDataset\\CodeLists\\Delisted\\"
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        delisted_stocks.to_excel(base_path + f"Delisted_Ticker_{date_str}.xlsx")


    def get_delistingstocks_test(self):  # 우선주만 남기는 코드. 테스트 용도
        stocks = fdr.StockListing('KRX-DELISTING')  # 코스피, 코스닥, 코넥스 전체
        cond = stocks['Market'] != 'KONEX'
        stocks = stocks.loc[cond]  # market == 'KONEX' 제외
        cond = stocks['SecuGroup'] == '주권'
        stocks = stocks.loc[cond]  # SecuGroup == '주권' 만 남기기
        stocks.rename(columns={'Symbol': 'Code'}, inplace=True)  # symbol --> code로 변경
        stocks = stocks[['Code', 'Name', 'ListingDate', 'DelistingDate']]  # synbol, name, listingDate, DelistingDate 칼럼만 남기기
        stocks = stocks[~stocks['Code'].str.endswith('0')]  # 'Code'의 값이 '0'으로 끝나는 행만 삭제 --> 보통주 삭제
        # df = df[~df['column_name'].str.endswith('0')]
        stocks = stocks[~stocks['Name'].str.contains('스팩')]  # 스팩 종목 제거
        # ListingDate, DelistingDate 열에서 연도-월-일 형식의 날짜만 추출
        stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d')
        stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d')
        stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
        stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋

        '''
        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d') + "')"
        return stocks
        '''

    def find_latest_file(self, folder_path, keyword):
        latest_file = None
        latest_date = None

        # 정규식을 통해 날짜 패턴과 파일 이름을 매칭
        date_pattern = re.compile(r".*" + re.escape(keyword) + r".*_(\d{4}-\d{2}-\d{2})\.xlsx$")

        for file_name in os.listdir(folder_path):
            match = date_pattern.match(file_name)
            if match:
                file_date_str = match.group(1)  # 날짜 추출
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file_name

        return latest_file

    def process_tickerlist(self, datemanage): # 1차 생성된 tickerlist 엑셀 파일을 받아와서 예외처리하여 엑셀로 저장함
        category = ['Delisted'] # 상폐일이 시뮬레이션 시작일보다 늦고, 상장일이 시뮬레이션 마지막 날보다 빠른 것만 남기기
        #category = ['Listed']
        #category = ['Listed', 'Delisted']
        for type_list in category:
            # original ticker list loading
            file_read_path = self.path_codeLists + f'\\{type_list}\\{type_list}_Ticker_{datemanage.workday_str}.xlsx'
            stocks = pd.read_excel(file_read_path, index_col=0)

            stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).apply(lambda x: x.date())
            stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).apply(lambda x: x.date())
            stocks = stocks[(stocks['DelistingDate'] >= datemanage.startday) & (stocks['ListingDate'] <= datemanage.workday)]

            stocks['Code'] = stocks['Code'].astype(str)
            stocks['Code'] = stocks['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d')  # ListingDate 열 type 변경
            stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d')  # LIstingDate 열 type 변경

            #date_str = '2024-03-29' # exception_list에 적용되는 날짜 str. 나중에는 각 파일마다 따로 찾는 루틴 있어야 함.
            date_str = datemanage.workday_str  # exception_list에 적용되는 날짜 str
            folder_read_path = self.path_codeLists + f'\\{type_list}\\exception_list'

            #  스팩 우회 상장 목록 불러오기
            #file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_스팩우회상장.xlsx'
            keyword = f'{type_list}_Ticker_스팩우회상장'
            file_name = self.find_latest_file(folder_read_path, keyword)

            #if os.path.exists(file_read_path):
            if file_name:
                file_read_path = folder_read_path + '\\' + file_name
                stocks_spac = pd.read_excel(file_read_path, index_col=0)
                stocks_spac['Code'] = stocks_spac['Code'].astype(str)
                stocks_spac['Code'] = stocks_spac['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
                stocks_spac['ListingDate'] = pd.to_datetime(stocks_spac['ListingDate']).dt.strftime('%Y-%m-%d')
                stocks_spac['DelistingDate'] = pd.to_datetime(stocks_spac['DelistingDate']).dt.strftime('%Y-%m-%d')
                # stocks df 에 stocks_spac_df 합성
                stocks = stocks.merge(stocks_spac, on='Code', how='left', suffixes=('', '_spac'))
                #  stocks_spac의 유효한 행만 stocks에 덮어 씌우기
                stocks.loc[stocks['ListingDate_spac'].notna(), 'ListingDate'] = stocks['ListingDate_spac']
                stocks.loc[stocks['DelistingDate_spac'].notna(), 'DelistingDate'] = stocks['DelistingDate_spac']

                # stocks_spac에만 있는 Code와 Name 찾기: exception list update 필요
                missing_in_stocks = stocks_spac[~stocks_spac['Code'].isin(stocks['Code'])]
                if not missing_in_stocks.empty:
                    print(f"{file_name}에만 있는 종목들:")
                    print(missing_in_stocks[['Code', 'Name']])

                # 불필요한 열 삭제
                stocks = stocks.drop(columns=['Name_spac', 'ListingDate_spac', 'DelistingDate_spac', '설명'])

            #  제외 목록 불러오기 - 상장 폐쇄형 펀드, 스팩, 주식수 zero
            #file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_제외목록.xlsx'
            keyword = f'{type_list}_Ticker_제외목록'
            file_name = self.find_latest_file(folder_read_path, keyword)

            # if os.path.exists(file_read_path):
            if file_name:
                file_read_path = folder_read_path + '\\' + file_name
                str_columns = ['ListingDate', 'DelistingDate']  # 문자열 형식으로 읽어올 열 이름 리스트
                stocks_excepted = pd.read_excel(file_read_path, index_col=0, dtype={col: str for col in str_columns})
                #stocks_excepted = pd.read_excel(file_read_path, index_col=0)
                stocks_excepted['Code'] = stocks_excepted['Code'].astype(str)
                stocks_excepted['Code'] = stocks_excepted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기

                # stocks_excepted에만 있는 Code와 Name 찾기: exception list update 필요
                missing_in_stocks = stocks_excepted[~stocks_excepted['Code'].isin(stocks['Code'])]
                if not missing_in_stocks.empty:
                    print(f"{file_name}에만 있는 종목들:")
                    print(missing_in_stocks[['Code', 'Name']])

                #  제외 목록을 원래 list에서 삭제
                stocks = stocks[~stocks['Code'].isin(stocks_excepted['Code'])]

            # KONEX에서 이전상장 목록 불러오기
            #file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_KONEX에서_이전상장.xlsx'
            keyword = f'{type_list}_Ticker_KONEX에서_이전상장'
            file_name = self.find_latest_file(folder_read_path, keyword)

            # if os.path.exists(file_read_path):
            if file_name:
                file_read_path = folder_read_path + '\\' + file_name
                stocks_KONEX = pd.read_excel(file_read_path, index_col=0)
                stocks_KONEX['Code'] = stocks_KONEX['Code'].astype(str)
                stocks_KONEX['Code'] = stocks_KONEX['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
                stocks_KONEX['ListingDate'] = pd.to_datetime(stocks_KONEX['ListingDate']).dt.strftime('%Y-%m-%d')
                stocks_KONEX['DelistingDate'] = pd.to_datetime(stocks_KONEX['DelistingDate']).dt.strftime('%Y-%m-%d')

                # stocks_KONEX 에만 있는 Code와 Name 찾기: exception list update 필요
                missing_in_stocks = stocks_KONEX[~stocks_KONEX['Code'].isin(stocks['Code'])]
                if not missing_in_stocks.empty:
                    print(f"{file_name}에만 있는 종목들:")
                    print(missing_in_stocks[['Code', 'Name']])

                #  KONEX 에 해당하는 기록의 목록을 원래 list에서 삭제
                stocks_KONEX_filtered = stocks_KONEX[stocks_KONEX['설명'].str.contains('KONEX')] # stocks_KONEX에서 '설명' 칼럼에 'KONEX'가 포함된 행들을 찾음

                # stocks_KONEX_filtered에서 stocks에 없는 'Code' 제거
                stocks_KONEX_filtered = stocks_KONEX_filtered[stocks_KONEX_filtered['Code'].isin(stocks['Code'])]

                # stocks_KONEX_filtered의 'Code', 'ListingDate', 'DelistingDate'를 기준으로 stocks에서 일치하는 행을 삭제
                #stocks = stocks.merge(stocks_KONEX_filtered, on=['Name', 'Code', 'ListingDate', 'DelistingDate'], how='left', indicator=True)
                # 병합 전 stocks_KONEX_filtered에서 'Name' 열 제거
                stocks_KONEX_filtered = stocks_KONEX_filtered.drop(columns=['Name'])
                stocks = stocks.merge(stocks_KONEX_filtered, on=['Code', 'ListingDate', 'DelistingDate'], how='left', indicator=True)
                stocks = stocks[stocks['_merge'] == 'left_only'].drop(columns=['_merge'])
                stocks = stocks.drop(columns=['설명'])  # '설명' 열 삭제

            # KOSDAQ에서 KOSPI로 이전상장 목록 불러오기
            #file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_KOSDAQ에서_이전상장.xlsx'
            keyword = f'{type_list}_Ticker_KOSDAQ에서_이전상장'
            file_name = self.find_latest_file(folder_read_path, keyword)

            # if os.path.exists(file_read_path):
            if file_name:
                file_read_path = folder_read_path + '\\' + file_name
                stocks_KOSDAQ = pd.read_excel(file_read_path, index_col=0)
                stocks_KOSDAQ['Code'] = stocks_KOSDAQ['Code'].astype(str)
                stocks_KOSDAQ['Code'] = stocks_KOSDAQ['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
                stocks_KOSDAQ['ListingDate'] = pd.to_datetime(stocks_KOSDAQ['ListingDate']).dt.strftime('%Y-%m-%d')
                stocks_KOSDAQ['DelistingDate'] = pd.to_datetime(stocks_KOSDAQ['DelistingDate']).dt.strftime('%Y-%m-%d')
                # Code별로 그룹화 후 병합 함수 적용
                stocks_KOSDAQ_merged = pd.DataFrame()
                for code, group in stocks_KOSDAQ.groupby('Code'):
                    merged_group = self.merge_rows(group)
                    stocks_KOSDAQ_merged = pd.concat([stocks_KOSDAQ_merged, merged_group], ignore_index=True)

                # stocks_KOSDAQ_merged 에서 '설명' 열 제거
                stocks_KOSDAQ_merged = stocks_KOSDAQ_merged.drop(columns=['설명'])
                # stocks_KOSDAQ_merged의 Code 리스트
                codes_to_replace = stocks_KOSDAQ_merged['Code'].unique()
                # stocks에서 해당 코드를 제거하기 전에 Name 정보를 저장. Name 은 Exception list 가 아니라 raw값을 쓰기 위함.
                name_mapping = (
                    stocks[stocks['Code'].isin(codes_to_replace)]
                    .sort_values(by='ListingDate', ascending=False)  # ListingDate 기준으로 정렬
                    .drop_duplicates(subset='Code')  # Code별로 ListingDate가 가장 늦은 행 선택
                    .set_index('Code')['Name']  # Code를 인덱스로 설정하고 Name 열만 저장
                )

                # stocks_KOSDAQ_merged 에만 있는 Code와 Name 찾기: exception list update 필요
                missing_in_stocks = stocks_KOSDAQ_merged[~stocks_KOSDAQ_merged['Code'].isin(stocks['Code'])]
                if not missing_in_stocks.empty:
                    print(f"{file_name}에만 있는 종목들:")
                    print(missing_in_stocks[['Code', 'Name']])

                # stocks_KOSDAQ_merged에서 stocks에 없는 Code를 제거
                stocks_KOSDAQ_merged = stocks_KOSDAQ_merged[stocks_KOSDAQ_merged['Code'].isin(stocks['Code'])]
                # stocks_KOSDAQ_merged에 Name 값을 업데이트
                stocks_KOSDAQ_merged['Name'] = stocks_KOSDAQ_merged['Code'].map(name_mapping)
                # stocks에서 해당 Code를 가진 행을 제거
                stocks = stocks[~stocks['Code'].isin(codes_to_replace)]
                # stocks_KOSDAQ_merged의 데이터를 stocks에 추가
                stocks = pd.concat([stocks, stocks_KOSDAQ_merged], ignore_index=True)

            # 상폐후 재상장 목록 불러오기
            #file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_상폐후_재상장.xlsx'
            keyword = f'{type_list}_Ticker_상폐후_재상장'
            file_name = self.find_latest_file(folder_read_path, keyword)

            # if os.path.exists(file_read_path):
            if file_name:
                file_read_path = folder_read_path + '\\' + file_name
                stocks_relisted = pd.read_excel(file_read_path, index_col=0)
                stocks_relisted['Code'] = stocks_relisted['Code'].astype(str)
                stocks_relisted['Code'] = stocks_relisted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
                stocks_relisted['ListingDate'] = pd.to_datetime(stocks_relisted['ListingDate']).dt.strftime('%Y-%m-%d')
                stocks_relisted['DelistingDate'] = pd.to_datetime(stocks_relisted['DelistingDate']).dt.strftime('%Y-%m-%d')

                # stocks_relisted 에만 있는 Code와 Name 찾기: exception list update 필요
                missing_in_stocks = stocks_relisted[~stocks_relisted['Code'].isin(stocks['Code'])]
                if not missing_in_stocks.empty:
                    print(f"{file_name}에만 있는 종목들:")
                    print(missing_in_stocks[['Code', 'Name']])

                # 동일한 Code를 갖는 그룹에 modify_code 함수 적용
                #stocks_relisted = stocks_relisted.groupby('Code').apply(self.modify_code).reset_index(drop=True)
                stocks_relisted = stocks_relisted.groupby('Code', group_keys=False).apply(self.modify_code).reset_index(drop=True)

                # stocks에서 해당 Code를 모두 제거하고 Name 값을 저장
                for code in stocks_relisted['Code'].unique():
                    name_value = stocks.loc[stocks['Code'] == code, 'Name'].drop_duplicates()
                    name_value = name_value.iloc[0] if not name_value.empty else None

                    # 해당 Code를 가진 행 삭제
                    stocks = stocks[stocks['Code'] != code]

                    # stocks_relisted에서 Code가 일치하는 행의 Name 열 업데이트
                    stocks_relisted.loc[stocks_relisted['Code'] == code, 'Name'] = name_value

                # stocks_relisted의 데이터를 stocks에 추가
                stocks = pd.concat([stocks, stocks_relisted], ignore_index=True)

                '''                
                # '예전상장정보'를 포함하는 stocks_relisted의 행 필터링
                filtered_relisted = stocks_relisted[stocks_relisted['설명'].str.contains('예전상장정보')]
                
                # stocks_relisted의 '예전상장정보' 목록들로 stocks 객체 업데이트
                for idx, relisted_row in filtered_relisted.iterrows():
                    mask = (
                            (stocks['Name'] == relisted_row['Name']) &
                            (stocks['ListingDate'] == relisted_row['ListingDate']) &
                            (stocks['DelistingDate'] == relisted_row['DelistingDate'])
                    )
                    stocks.loc[mask, 'Code'] = relisted_row['Code']
                    stocks.loc[mask, 'Name'] = relisted_row['Name']
                    stocks.loc[mask, 'ListingDate'] = relisted_row['ListingDate']
                    stocks.loc[mask, 'DelistingDate'] = relisted_row['DelistingDate']
                '''

                # 처리 후 stocks 에 2개 이상 code 가 있는 경우 찾기.  exception list update 필요(새로운 재상장 후 상폐 종목)
                # 중복된 Code 값을 가진 행 찾기
                duplicate_codes = stocks[stocks.duplicated(subset='Code', keep=False)]
                if not duplicate_codes.empty:
                    # 중복된 행들에서 Code와 Name 열만 선택하여 출력
                    print(f"{type_list}_Ticker_{datemanage.workday_str} 에 복수개 코드가 있는 종목들:")
                    print(duplicate_codes[['Code', 'Name']])

            # 상장일 기준 정렬
            stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
            stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋
            file_save_path = self.path_codeLists + f'\\{type_list}\\{type_list}_Ticker_{date_str}_modified.xlsx'
            stocks.to_excel(file_save_path)

    def check_code_list(self):
        '''
        #예전에 상폐 후 현재는 재상장되어 listed 에 있는 경우를 찾아야 한다.
        #새롭게 상장된 것?
            기존 listed 에 있던 것과 코드는 동일한데 이름이나 상장일이 변경된 것이 있는지 --> exception list 업데이트 필요
        #새롭게 상폐된 것?
            기존 listed exception list 검토
            code가 duplicate 되는 게 있는지 - 혹시 상폐후 재상장, konex/kosdaq 에서 이전상장한 정보가 넘어오는 경우
        #새롭게 스팩 우회 상장
        # 새롭게 제외 목록
        # 새롭게 이전 상장 - Konex, kosdaq
        # 새롭게 상폐 후 재상장 되었다가 상폐
        '''

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
            stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate'])
            stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate'])
            # Intelliquant 를 위한 칼럼
            stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
            stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime(
                '%Y-%m-%d') + "')"
            stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
                '%Y-%m-%d') + "')"

            stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
            stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋

            # txt 파일 저장할 경로
            base_path = f'{self.path_codeLists}\\{type_list}\\For_Intelliquant\\{datemanage.workday_str}\\'
            #base_path = 'C:\\Work_Dotori\\StockDataset\\CodeLists\\' + type_list + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'
            if not os.path.exists(base_path):
                os.makedirs(base_path)

            # 각 칼럼을 20개씩 끊어서 파일로 저장
            for column in ['A_Code', 'A_ListingDate', 'A_DelistingDate']:
                for i in range(0, len(stocks), 60):
                    subset = stocks[column][i:i + 60]
                    file_name = f"{column}_{datemanage.workday_str}_{i // 60 + 1}.txt"
                    with open(base_path + file_name, 'w') as f:
                        for idx, val in enumerate(subset):
                            f.write(val)
                            if (idx + 1) % 5 == 0 and idx != len(subset) - 1:
                                f.write(',\n')
                            elif idx != len(subset) - 1:
                                f.write(',')

    def merge_rows(self, df): #KOSDAQ에서 KOSPI로 이전상장 목록 처리시 code 같은 것 그룹으로 묶기
        df = df.sort_values(by='ListingDate')
        new_rows = []

        current_row = df.iloc[0].copy()  # 수정: 복사본을 만들어 경고 방지
        for i in range(1, len(df)):
            next_row = df.iloc[i]
            # 현재 행의 DelistingDate가 다음 행의 ListingDate와 같거나 이전이면 병합
            if current_row['DelistingDate'] >= next_row['ListingDate']:
                # 최소 ListingDate와 최대 DelistingDate를 선택
                current_row['DelistingDate'] = max(current_row['DelistingDate'], next_row['DelistingDate'])
            else:
                # 현재 행을 저장하고, 다음 행으로 이동
                new_rows.append(current_row)
                current_row = next_row.copy()  # 수정: 다음 행도 복사본을 만들어 경고 방지
        # 마지막 행 추가
        new_rows.append(current_row)

        return pd.DataFrame(new_rows)

    # 각 Code별로 그룹화하고 처리
    def modify_code(self, group):# 상폐후 재상장 목록 처리할 때 Code 같은것들 ListingDate 기준으로 Code 값의 마지막 문자열 수정함
        # ListingDate로 정렬
        group = group.sort_values(by='ListingDate')

        # 마지막 행을 제외한 모든 행에 대해 Code 수정
        for i in range(len(group) - 1):
            new_code = group.iloc[i]['Code'][:-1] + ascii_uppercase[i]
            group.at[group.index[i], 'Code'] = new_code

        return group