import FinanceDataReader as fdr
from pykrx import stock
import yfinance as yf
import pandas as pd
import numpy as np
import os
import configparser

class GetTicker:
    '''
    작업일(오늘) 기준 상장종목, 상폐종목의 이름 코드, 상장일, 상폐일 dataframe 리턴

    '''
    '''
    def __init__(self):
        pass
    '''
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
        stocks = stocks[['Code', 'Name', 'ListingDate',
                         'DelistingDate']]  # synbol, name, listingDate, DelistingDate 칼럼만 남기기
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
        stocks = stocks[['Code', 'Name', 'ListingDate',
                         'DelistingDate']]  # synbol, name, listingDate, DelistingDate 칼럼만 남기기
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
        stocks['A_DelistingDate'] = "new Date('" + pd.to_datetime(stocks['DelistingDate']).dt.strftime(
            '%Y-%m-%d') + "')"
        return stocks

    def process_tickerlist(self, datemanage): # 1차 생성된 tickerlist 엑셀 파일을 받아와서 예외처리하여 엑셀로 저장함
        pass