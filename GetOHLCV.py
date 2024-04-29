import os
import configparser
import FinanceDataReader as fdr
from pykrx import stock
# import pandas_datareader.data as pdr
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import utils


class GetOHLCV:
    '''
    종목 코드를 받아서 OHLCV dataframe 반환
    '''

    def __init__(self, logger):
        self.logger = logger
        self.limit_change_day = startday = datetime(2015, 6, 15) #가격제한폭이 30%로 확대된 날
        self.suffix = 'OHLCV_origin'  # 파일 이름 저장시 사용하는 접미사

        # 설정 로드
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_GetOHLCV.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_codeLists = config['path']['path_codelists']
        self.path_OHLCV_init = config['path']['path_OHLCV_init']

    def get_OHLCV(self, code, start_date, end_date, listed_status):
        # fdr 은 2024-04-29 기준으로 마지막으로 읽어올 수 있는 데이터가 2000-01-10 부터이다. 일단 생략한다.
        '''
        df_OHLCV_fdr = fdr.DataReader(code, start=start_date, end=end_date)
        df_OHLCV_fdr.reset_index(inplace=True)
        df_OHLCV_fdr['Date'] = pd.to_datetime(df_OHLCV_fdr['Date']).dt.strftime('%Y-%m-%d')  # 인덱스 열을 바꾸는 것으로 코드 수정
        df_OHLCV_fdr.set_index('Date', inplace=True)
        df_OHLCV_fdr.drop(['Change'])
        '''

        df_OHLCV_pykrx = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=code)
        df_OHLCV_pykrx.drop(['등락률'], axis=1, inplace=True)
        df_OHLCV_pykrx.reset_index(inplace=True)
        df_OHLCV_pykrx.rename(columns={'날짜': 'Date', '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'}, inplace=True)
        df_OHLCV_pykrx['Date'] = pd.to_datetime(df_OHLCV_pykrx['Date']).dt.strftime('%Y-%m-%d')
        df_OHLCV_pykrx.set_index('Date', inplace=True)

        if( listed_status == 'Listed'): #상장 종목
            code_yahoo = code + '.KS'
            ticker = yf.Ticker(code_yahoo)
            df_OHLCV_yf = ticker.history(interval='1d', start=start_date, end=end_date, auto_adjust=False)
            df_OHLCV_yf.reset_index(inplace=True)
            df_OHLCV_yf['Date'] = pd.to_datetime(df_OHLCV_yf['Date']).dt.strftime('%Y-%m-%d')
            df_OHLCV_yf.set_index('Date', inplace=True)
            df_OHLCV_yf.drop(['Adj Close', 'Volume', 'Dividends', 'Stock Splits'], axis=1, inplace=True)

            # 무결성 검사 1. 두 객체 비교
            comparison_result = (df_OHLCV_pykrx[['Date', 'Open', 'High', 'Low', 'Close']] == df_OHLCV_yf[['Date', 'Open', 'High', 'Low', 'Close']]) | (
                        pd.isna(df_OHLCV_pykrx[['Date', 'Open', 'High', 'Low', 'Close']]) & pd.isna(df_OHLCV_yf[['Date', 'Open', 'High', 'Low', 'Close']]))

            # 모든 값이 True인지 확인
            if comparison_result.all().all():  # 첫 번째 all()은 각 열에 대해, 두 번째 all()은 결과적으로 얻은 시리즈에 대해
                return True, df_OHLCV_pykrx
            else:
                return False, df_OHLCV_pykrx, df_OHLCV_yf



    def get_OHLCV_original(self, datemanage):
        #path_date_reference = f"{path_OHLCV_init}\\Listed\\data_reference_{datemanage.workday_str}.xlsx"
        #df_date_reference = pd.read_excel(path_date_reference, index_col=0)
        #df_date_reference['Date'] = pd.to_datetime(df_date_reference['ListingDate']).dt.strftime('%Y-%m-%d')

        code = '005930'
        listed_status = 'Listed'
        result = self.get_OHLCV.get_OHLCV(code, datemanage.startday_str, datemanage.workday_str, listed_status)
        folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
        if result[0] == True:
            # 여기서 무결성 검사는 다 하자.
            df_OHLCV = result[1]

            # 무결성 검사 2. NaN 있는지 확인
            rows_with_nan = df_OHLCV.isna().any(axis=1) # NaN 있는지 확인
            if rows_with_nan.any():
                self.logger.info("NaN 값이 있는 날짜: %s" % df_OHLCV[rows_with_nan].index.tolist())

            # 무결성 검사 3. 시간적 일관성 확인
            #ref 와 지금 받아온 OHLCV의 date 비교
            unique_to_ref = df_date_reference[~df_date_reference['Date'].isin(df_OHLCV['Date'])] # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
            unique_to_df_OHLCV = df_OHLCV[~df_OHLCV['Date'].isin(df_date_reference['Date'])] # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
            if unique_to_ref is not None:
                self.logger.info("df_OHLCV에 없는 날짜 날짜: %s" % unique_to_ref)
            if unique_to_df_OHLCV is not None:
                self.logger.info("df_OHLCV에만 추가로 있는 날짜: %s" % unique_to_df_OHLCV)
            # 시간 순으로 정렬되지 않은 행 찾기
            df_OHLCV['Out_of_Order'] = df_OHLCV['Date'] < df_OHLCV['Date'].shift(1)
            out_of_order_rows = df_OHLCV[df_OHLCV['Out_of_Order']]
            if not out_of_order_rows.empty:
                self.logger.info("날짜가 역순인 부분: %s" % out_of_order_rows)
            df_OHLCV.drop(['Out_of_Order'], axis=1, inplace=True)

            # 무결성 검사 4. outlier 검출 - 가격제한폭 초과 변동
            df_OHLCV['Pre_Close'] = df_OHLCV['Close'].shift(1) # 전날의 Close 값 계산
            # 기준일에 따른 조건 설정
            conditions_before = (df_OHLCV['Date'] < self.limit_change_day) & (
                    (df_OHLCV['Open'] > df_OHLCV['Pre_Close'] * 1.15) | (df_OHLCV['Open'] < df_OHLCV['Pre_Close'] * 0.85) |
                    (df_OHLCV['High'] > df_OHLCV['Pre_Close'] * 1.15) | (df_OHLCV['High'] < df_OHLCV['Pre_Close'] * 0.85) |
                    (df_OHLCV['Low'] > df_OHLCV['Pre_Close'] * 1.15) | (df_OHLCV['Low'] < df_OHLCV['Pre_Close'] * 0.85) |
                    (df_OHLCV['Close'] > df_OHLCV['Pre_Close'] * 1.15) | (df_OHLCV['Close'] < df_OHLCV['Pre_Close'] * 0.85)
            )

            conditions_after = (df_OHLCV['Date'] >= self.limit_change_day) & (
                    (df_OHLCV['Open'] > df_OHLCV['Pre_Close'] * 1.3) | (df_OHLCV['Open'] < df_OHLCV['Pre_Close'] * 0.7) |
                    (df_OHLCV['High'] > df_OHLCV['Pre_Close'] * 1.3) | (df_OHLCV['High'] < df_OHLCV['Pre_Close'] * 0.7) |
                    (df_OHLCV['Low'] > df_OHLCV['Pre_Close'] * 1.3) | (df_OHLCV['Low'] < df_OHLCV['Pre_Close'] * 0.7) |
                    (df_OHLCV['Close'] > df_OHLCV['Pre_Close'] * 1.3) | (df_OHLCV['Close'] < df_OHLCV['Pre_Close'] * 0.7)
            )
            final_conditions = conditions_before | conditions_after
            outliers = df_OHLCV[final_conditions]
            if not outliers.empty:
                self.logger.info("가격제한폭 초과: %s" % outliers)
            df_OHLCV.drop(['Pre_Close'], axis=1, inplace=True)

            # 엑셀 파일 저장
            #folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
            custom_string = f"_{self.suffix}_pykrx_{datemanage.workday_str}"
            utils.save_df_to_excel(result[1], code, custom_string, folder)
        else: # 두 소스의 파일이 다른 경우
            # 두가지 소스의 엑셀 파일 모두 저장
            custom_string = f"_{self.suffix}_pykrx_{datemanage.workday_str}"
            utils.save_df_to_excel(result[1], code, custom_string, folder)
            custom_string = f"_{self.suffix}_yf_{datemanage.workday_str}"
            utils.save_df_to_excel(result[2], code, custom_string, folder)
            self.logger.info("두 소스의 데이터 다름: %s" % code)
