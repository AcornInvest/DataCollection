import FinanceDataReader as fdr
from pykrx import stock
import yfinance as yf
import pandas as pd
import numpy as np
import os
import configparser
from string import ascii_uppercase

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

        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
            '%Y-%m-%d') + "')"
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

        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
            '%Y-%m-%d') + "')"
        return stocks

    def get_delistingstocks_test(self):  # 우선주만 남기는 테스트 용도
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

        # Intelliquant 를 위한 칼럼
        stocks['A_Code'] = "'A" + stocks['Code'] + "'"  # Code 열의 각 값에 "A"를 붙인 열 생성
        stocks['A_ListingDate'] = "new Date('" + pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d') + "')"
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d') + "')"
        return stocks

    def process_tickerlist(self, datemanage): # 1차 생성된 tickerlist 엑셀 파일을 받아와서 예외처리하여 엑셀로 저장함
        category = ['Delisted']
        for type_list in category:
            # original ticker list loading
            file_read_path = self.path_codeLists + f'\\{type_list}\\{type_list}_Ticker_{datemanage.workday_str}.xlsx'
            stocks = pd.read_excel(file_read_path, index_col=0)
            stocks['Code'] = stocks['Code'].astype(str)
            stocks['Code'] = stocks['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate']).dt.strftime('%Y-%m-%d')  # LIstingDate 열 type 변경
            stocks['DelistingDate'] = pd.to_datetime(stocks['DelistingDate']).dt.strftime('%Y-%m-%d')  # LIstingDate 열 type 변경

            date_str = '2024-03-29' # exception_list에 적용되는 날짜 str. 나중에는 각 파일마다 따로 찾는 루틴 있어야 함.
            #  스팩 우회 상장 목록 불러오기
            file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_스팩우회상장.xlsx'
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
            # 불필요한 열 삭제
            stocks = stocks.drop(columns=['Name_spac', 'ListingDate_spac', 'DelistingDate_spac', '설명'])

            #  제외 목록 불러오기 - 상장 폐쇄형 펀드, 스팩, 주식수 zero
            file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_제외목록.xlsx'
            stocks_excepted = pd.read_excel(file_read_path, index_col=0)
            stocks_excepted['Code'] = stocks_excepted['Code'].astype(str)
            stocks_excepted['Code'] = stocks_excepted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            #  제외 목록을 원래 list에서 삭제
            stocks = stocks[~stocks['Code'].isin(stocks_excepted['Code'])]

            # KONEX에서 이전상장 목록 불러오기
            file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_KONEX에서_이전상장.xlsx'
            stocks_KONEX = pd.read_excel(file_read_path, index_col=0)
            stocks_KONEX['Code'] = stocks_KONEX['Code'].astype(str)
            stocks_KONEX['Code'] = stocks_KONEX['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            stocks_KONEX['ListingDate'] = pd.to_datetime(stocks_KONEX['ListingDate']).dt.strftime('%Y-%m-%d')
            stocks_KONEX['DelistingDate'] = pd.to_datetime(stocks_KONEX['DelistingDate']).dt.strftime('%Y-%m-%d')
            #  KONEX 에 해당하는 기록의 목록을 원래 list에서 삭제
            stocks_KONEX_filtered = stocks_KONEX[stocks_KONEX['설명'].str.contains('KONEX')] # stocks_KONEX에서 '설명' 칼럼에 'KONEX'가 포함된 행들을 찾음
            # stocks_KONEX_filtered의 'Code', 'ListingDate', 'DelistingDate'를 기준으로 stocks에서 일치하는 행을 삭제
            stocks = stocks.merge(stocks_KONEX_filtered, on=['Name', 'Code', 'ListingDate', 'DelistingDate'], how='left', indicator=True)
            stocks = stocks[stocks['_merge'] == 'left_only'].drop(columns=['_merge'])
            stocks = stocks.drop(columns=['설명'])  # '설명' 열 삭제

            # KOSDAQ에서 KOSPI로 이전상장 목록 불러오기
            file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_KOSDAQ에서_이전상장.xlsx'
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
            # stocks에서 해당 Code를 가진 행을 제거
            stocks = stocks[~stocks['Code'].isin(codes_to_replace)]
            # stocks_KOSDAQ_merged의 데이터를 stocks에 추가
            stocks = pd.concat([stocks, stocks_KOSDAQ_merged], ignore_index=True)

            # 상폐후 재상장 목록 불러오기
            file_read_path = self.path_codeLists + f'\\{type_list}\\exception_list\\{type_list}_Ticker_{date_str}_상폐후_재상장.xlsx'
            stocks_relisted = pd.read_excel(file_read_path, index_col=0)
            stocks_relisted['Code'] = stocks_relisted['Code'].astype(str)
            stocks_relisted['Code'] = stocks_relisted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            stocks_relisted['ListingDate'] = pd.to_datetime(stocks_relisted['ListingDate']).dt.strftime('%Y-%m-%d')
            stocks_relisted['DelistingDate'] = pd.to_datetime(stocks_relisted['DelistingDate']).dt.strftime('%Y-%m-%d')
            # 동일한 Code를 갖는 그룹에 modify_code 함수 적용
            stocks_relisted = stocks_relisted.groupby('Code').apply(self.modify_code).reset_index(drop=True)

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
            # '설명' 열 제거
            #stocks.drop(columns='설명', inplace=True)

            # list 상장일 기준 정렬
            stocks = stocks.sort_values(by='ListingDate')  # 상장일 기준 오름차순 정렬
            stocks.reset_index(drop=True, inplace=True)  # 인덱스 리셋
            file_save_path = self.path_codeLists + f'\\{type_list}\\{type_list}_Ticker_{date_str}_modified.xlsx'
            stocks.to_excel(file_save_path)

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