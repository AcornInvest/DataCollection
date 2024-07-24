import os
import configparser
import FinanceDataReader as fdr
from pykrx import stock
# import pandas_datareader.data as pdr
#import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import utils

class GetOHLCV:
    '''
    종목 코드를 받아서 OHLCV dataframe 반환
    '''

    def __init__(self, logger):
        self.logger = logger
        self.limit_change_day = date(2015, 6, 15) #가격제한폭이 30%로 확대된 날
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
        self.path_date_ref = config['path']['path_date_ref']

    def get_OHLCV(self, code, start_date, end_date, listed_status, df_holiday_ref, datemanage):
        # 관심 있는 열 선택
        columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        if listed_status == 'Delisted':
            df_OHLCV_fdr = fdr.DataReader(f'KRX-DELISTING:{code}', start=start_date, end=end_date)
        else:
            df_OHLCV_fdr = fdr.DataReader(code, start=start_date, end=end_date)
            df_OHLCV_fdr_krx = fdr.DataReader(f'KRX:{code}', start=start_date, end=end_date)  # 거래량만 가져오는 목적

        #df_OHLCV_fdr.drop(['Change'], axis=1, inplace=True)
        df_OHLCV_fdr.reset_index(inplace=True)
        df_OHLCV_fdr['Date'] = pd.to_datetime(df_OHLCV_fdr['Date']).dt.strftime('%Y-%m-%d')  # 인덱스 열을 바꾸는 것으로 코드 수정
        df_OHLCV_fdr[['Open', 'High', 'Low', 'Close']] = df_OHLCV_fdr[['Open', 'High', 'Low', 'Close']].astype(float)
        if listed_status == 'Delisted':  #상폐 종목은 날짜별 오름차순 리셋 필요
            df_OHLCV_fdr = df_OHLCV_fdr.sort_values(by='Date')  # 날짜 오름차순 정렬
        df_OHLCV_fdr.set_index('Date', inplace=True)
        # df_holiday_ref 에 있는 휴장일에 해당하는 데이터 삭제시킴
        indices_to_drop = df_OHLCV_fdr.index.intersection(df_holiday_ref.index)
        df_OHLCV_fdr.drop(indices_to_drop, inplace=True)
        df_OHLCV_fdr = df_OHLCV_fdr[columns] # 관심있는 열만 남김

        if listed_status == 'Listed':  # 상장 종목의 경우 거래량 df 정리
            df_OHLCV_fdr_krx.reset_index(inplace=True)
            df_OHLCV_fdr_krx['Date'] = pd.to_datetime(df_OHLCV_fdr_krx['Date']).dt.strftime('%Y-%m-%d')
            df_OHLCV_fdr_krx.set_index('Date', inplace=True)
            # df_holiday_ref 에 있는 휴장일에 해당하는 데이터 삭제시킴
            indices_to_drop = df_OHLCV_fdr_krx.index.intersection(df_holiday_ref.index)
            df_OHLCV_fdr_krx.drop(indices_to_drop, inplace=True)
            df_OHLCV_fdr_krx = df_OHLCV_fdr_krx[['Volume']]  # 거래량 열만 남김.

        if listed_status == 'Delisted':
            df_OHLCV_pykrx = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=code, adjusted=False)
        else:
            df_OHLCV_pykrx = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=code)
        df_OHLCV_pykrx.reset_index(inplace=True)
        df_OHLCV_pykrx.rename(columns={'날짜': 'Date', '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'}, inplace=True)
        df_OHLCV_pykrx['Date'] = pd.to_datetime(df_OHLCV_pykrx['Date']).dt.strftime('%Y-%m-%d')
        df_OHLCV_pykrx[['Open', 'High', 'Low', 'Close']] = df_OHLCV_pykrx[['Open', 'High', 'Low', 'Close']].astype(float)
        df_OHLCV_pykrx.set_index('Date', inplace=True)
        # df_holiday_ref 에 있는 휴장일에 해당하는 데이터 삭제시킴
        indices_to_drop = df_OHLCV_pykrx.index.intersection(df_holiday_ref.index)
        df_OHLCV_pykrx.drop(indices_to_drop, inplace=True)
        df_OHLCV_pykrx = df_OHLCV_pykrx[columns] # 관심있는 열만 남김

        # 무결성 검사 1. 두 객체 비교
        all_dates = df_OHLCV_pykrx.index.union(df_OHLCV_fdr.index)  # 두 DataFrame의 인덱스로 사용된 'Date'의 고유한 값들을 모두 찾기

        # 이제 두 DataFrame을 all_dates를 사용하여 재인덱싱합니다.
        df_OHLCV_pykrx_reindexed = df_OHLCV_pykrx.reindex(all_dates)
        df_OHLCV_fdr_reindexed = df_OHLCV_fdr.reindex(all_dates)

        # 두 데이터 프레임의 해당 열만 추출
        df_pykrx = df_OHLCV_pykrx_reindexed[columns]
        df_fdr = df_OHLCV_fdr_reindexed[columns]

        # 두 데이터 프레임 결합
        df_combined = df_pykrx.combine_first(df_fdr) # pykrx 값을 기본으로 사용

        # 두 데이터 프레임 값의 편차 계산 및 조건에 따른 처리
        significant_diff = False
        diff_btw_sources = []
        for col in columns:
            diff = df_pykrx[col] - df_fdr[col]
            mask = (diff.abs() >= 1.5)
            df_combined[mask] = df_pykrx[mask]  # pykrx 값을 기본으로 사용
            significant_diff_rows = mask[mask].index.tolist()
            if significant_diff_rows:
                significant_diff = True
                pos_diff = f'{col} 열, 행: {significant_diff_rows}'
                diff_btw_sources.append(pos_diff)

        if diff_btw_sources: # OHLCV 소스간 편차가 있는 경우 txt 파일로 저장
            folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
            if not os.path.exists(folder):
                os.makedirs(folder)
            path = f"{folder}\\{code}_diff_btw_sources.txt"
            utils.save_list_to_file(diff_btw_sources, path) # 텍스트 파일에 오류 부분 저장
            self.logger.info("OHLCV Origin - 두 소스의 데이터 다름: %s" % code)

        volume_diff = False
        volume_diff_source = []
        if listed_status == 'Listed':  # 상장 종목의 경우 거래량을 KRX와 비교
            # 거래량 편차 확인
            all_dates = df_combined.index.union(df_OHLCV_fdr_krx.index)  # 두 DataFrame의 인덱스로 사용된 'Date'의 고유한 값들을 모두 찾기

            df_combined_reindexed = df_combined.reindex(all_dates)
            df_OHLCV_fdr_krx_reindexed = df_OHLCV_fdr_krx.reindex(all_dates)

            # df_OHLCV_fdr_krx_reindexed['Volume'] 중 NaN이 아닌 행만 필터링. fdr의 KRX는 오래된 값들을 불러오지 못할 수 있다.
            mask = df_OHLCV_fdr_krx_reindexed['Volume'].notna()
            # 필터링된 행에서 df_combined_reindexed['Volume']와 비교
            comparison_result = (df_combined_reindexed['Volume'][mask] == df_OHLCV_fdr_krx_reindexed['Volume'][mask])

            if not comparison_result.all():
                del_volume = df_combined_reindexed['Volume'][mask] - df_OHLCV_fdr_krx_reindexed['Volume'][mask]
                mask2 = (del_volume.abs() > 0)
                volume_diff_rows = mask2[mask2].index.tolist()
                if volume_diff_rows:
                    volume_diff = True
                    pos_volume_diff = f'{col} 열, 행: {volume_diff_rows}'
                    volume_diff_source.append(pos_volume_diff)

            if volume_diff_source:
                df_combined_reindexed['Volume'] = df_OHLCV_fdr_krx_reindexed['Volume']
                folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
                if not os.path.exists(folder):
                    os.makedirs(folder)
                path = f"{folder}\\{code}_volume_diff.txt"
                utils.save_list_to_file(volume_diff_source, path)  # 텍스트 파일에 오류 부분 저장
                self.logger.info("OHLCV Origin - KRX와 naver 거래량 다름: %s" % code)

        else:
            df_combined_reindexed = df_combined
            df_OHLCV_fdr_krx_reindexed = df_fdr

        if significant_diff or volume_diff:
            return False, df_pykrx, df_fdr, df_combined_reindexed, df_OHLCV_fdr_krx_reindexed
        else:
            return True, df_combined_reindexed  # df_OHLCV_pykrx

    def get_OHLCV_original(self, code, start_date, end_date, datemanage, listed_status, df_holiday_ref): # 한종목에 대해 상당 기간에 해당하는 OHLCV 데이터 읽어옴
        if code[-1] != '0':
            code_modified = code[:-1] + '0'
        else:
            code_modified = code

        result = self.get_OHLCV(code_modified, start_date, end_date, listed_status, df_holiday_ref, datemanage)
        folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
        if result[0] == True: # 엑셀 파일 저장
            #folder = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\"
            custom_string = f"_{self.suffix}_{datemanage.workday_str}"
            utils.save_df_to_excel(result[1], code, custom_string, folder)
        else: # 두 소스의 파일이 다른 경우
            # 두가지 소스의 엑셀 파일 모두 저장
            custom_string = f"_{self.suffix}_pykrx_{datemanage.workday_str}"
            utils.save_df_to_excel(result[1], code, custom_string, folder)
            custom_string = f"_{self.suffix}_fdr_{datemanage.workday_str}"
            utils.save_df_to_excel(result[2], code, custom_string, folder)
            custom_string = f"_{self.suffix}_{datemanage.workday_str}"
            utils.save_df_to_excel(result[3], code, custom_string, folder)
            custom_string = f"_{self.suffix}_krx_volume_{datemanage.workday_str}"
            utils.save_df_to_excel(result[4], code, custom_string, folder)

    def run_get_OHLCV_original(self, datemanage): # 전체 code list 에 대해 get_OHLCV_original 실행시킴
        df_holiday_ref = pd.read_excel(f'{self.path_date_ref}\\holiday_ref_{datemanage.workday_str}.xlsx', index_col=0)
        category = ['Listed', 'Delisted']
        #category = ['Delisted']
        #category = ['Listed']
        for listed_status in category:
            # 코드리스트 읽어오기
            codelist_path = f'{self.path_codeLists}\\{listed_status}\\{listed_status}_Ticker_{datemanage.workday_str}_modified.xlsx'
            df_codelist = pd.read_excel(codelist_path, index_col=0)
            df_codelist['Code'] = df_codelist['Code'].astype(str)
            df_codelist['Code'] = df_codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            for index, row in df_codelist.iterrows():
                listing_date_obj = datetime.strptime(row['ListingDate'], '%Y-%m-%d').date()
                delisting_date_obj = datetime.strptime(row['DelistingDate'], '%Y-%m-%d').date()
                if datemanage.startday <= delisting_date_obj:
                    if datemanage.startday <= listing_date_obj:
                        start_date = listing_date_obj.strftime('%Y-%m-%d')
                    else:
                        start_date = datemanage.startday_str
                    if datemanage.workday >= delisting_date_obj:
                        end_date = delisting_date_obj.strftime('%Y-%m-%d')
                    else:
                        end_date = datemanage.workday_str
                    self.get_OHLCV_original(row['Code'], start_date, end_date, datemanage, listed_status, df_holiday_ref)

    def update_OHLCV_original(self, code, datemanage, listed_status):
        #기존 OHLCV 로드
        #get_OHLCV() 호출하여 추가되는 부분 읽어옴
        #합쳐서 xlsx로 저장
        pass

    def process_OHLCV_original(self, datemanage, listed_status):
        # 거래일 목록 ref 읽어오기
        path_date_ref = f'{self.path_date_ref}\\bussiness_day_ref_{datemanage.workday_str}.xlsx'
        df_business_days = pd.read_excel(path_date_ref)
        df_business_days['Date'] = pd.to_datetime(df_business_days['Date']).dt.date

        # 코드리스트 읽어오기
        codelist_path = f'{self.path_codeLists}\\{listed_status}\\{listed_status}_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist = pd.read_excel(codelist_path, index_col=0)
        df_codelist['Code'] = df_codelist['Code'].astype(str)
        df_codelist['Code'] = df_codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist['ListingDate'] = pd.to_datetime(df_codelist['ListingDate']).dt.date
        df_codelist['DelistingDate'] = pd.to_datetime(df_codelist['DelistingDate']).dt.date
        #codelist = df_codelist['Code']

        for index, row in df_codelist.iterrows():
            listing_date = row['ListingDate']
            delisting_date = row['DelistingDate']
            # df_business_days에서 listing_date와 delisting_date 사이의 날짜 추출
            #df_b_day_ref = df_business_days[(df_business_days['Date'] >= listing_date) & (df_business_days['Date'] <= delisting_date)]
            df_b_day_ref = df_business_days[(df_business_days['Date'] >= listing_date) & (df_business_days['Date'] <= delisting_date)].copy()

            # 시작일과 종료일 조정
            df_b_day_ref['Date'] = df_b_day_ref['Date'].apply(lambda x: max(x, datemanage.startday))
            df_b_day_ref['Date'] = df_b_day_ref['Date'].apply(lambda x: min(x, datemanage.workday))

            # 코드 불러오기
            code = row['Code']
            path_OHLCV = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\{code}_{self.suffix}_{datemanage.workday_str}.xlsx"
            if os.path.exists(path_OHLCV):
                df_OHLCV = pd.read_excel(path_OHLCV, index_col=0)
                self.check_integrity(code, df_b_day_ref, df_OHLCV, datemanage, listed_status)  # 무결성 검사
            else:
                path = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\OHCLV_not_existed_list.txt"
                OHCLV_not_existed = [code]
                utils.save_list_to_file_append(OHCLV_not_existed, path)  # 텍스트 파일에 오류 부분 저장
                self.logger.info("OHLCV Origin 파일이 없음: %s" % code)

    def check_integrity(self, code, df_b_day_ref, df_OHLCV, datemanage, listed_status):
        # 여기서 무결성 검사는 다 하자.
        df_OHLCV.reset_index(inplace=True)
        df_OHLCV['Date'] = pd.to_datetime(df_OHLCV['Date']).dt.date

        # 무결성 검사 2. NaN 있는지 확인
        rows_with_nan = df_OHLCV.isna().any(axis=1)  # NaN 있는지 확인
        if rows_with_nan.any():
            NaN_exists = df_OHLCV[rows_with_nan]['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            NaN_exists_list = [f"{code}, NaN 값이 있는 날짜: {NaN_exists}"]
            #NaN_exists = [f"{code}, NaN 값이 있는 날짜: {df_OHLCV[rows_with_nan]['Date'].apply(lambda d: d.strftime('%Y-%m-%d'))}"]
            self.logger.info(NaN_exists_list)
            path = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\NaN_exists_list.txt"
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장

        # 무결성 검사 3. 시간적 일관성 확인
        path = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        # ref 와 지금 받아온 OHLCV의 date 비교
        unique_to_ref = df_b_day_ref[~df_b_day_ref['Date'].isin(df_OHLCV['Date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_OHLCV = df_OHLCV[~df_OHLCV['Date'].isin(df_b_day_ref['Date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
        if not unique_to_ref.empty:
            unique_to_ref = unique_to_ref['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_ref_list = [f'{code}, df_OHLCV에 없는 날짜: {unique_to_ref}']
            self.logger.info(unique_to_ref_list)
            utils.save_list_to_file_append(unique_to_ref_list, path)  # 텍스트 파일에 오류 부분 저장
        if not unique_to_df_OHLCV.empty:
            unique_to_df_OHLCV = unique_to_df_OHLCV['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_df_OHLCV_list = [f'{code}, df_OHLCV에만 추가로 있는 날짜: {unique_to_df_OHLCV}']
            self.logger.info(unique_to_df_OHLCV_list)
            utils.save_list_to_file_append(unique_to_df_OHLCV_list, path)  # 텍스트 파일에 오류 부분 저장
        # 시간 순으로 정렬되지 않은 행 찾기. 같은 날짜가 또 있는 것도 포함
        df_OHLCV['Out_of_Order'] = df_OHLCV['Date'] <= df_OHLCV['Date'].shift(1)
        out_of_order_rows = df_OHLCV[df_OHLCV['Out_of_Order']]
        if not out_of_order_rows.empty:
            out_of_order_rows = out_of_order_rows['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            out_of_order_rows_list = [f'{code}, 날짜가 역순인 부분: {out_of_order_rows}']
            self.logger.info(out_of_order_rows_list)
            utils.save_list_to_file_append(out_of_order_rows_list, path)  # 텍스트 파일에 오류 부분 저장
        #df_OHLCV.drop(['Out_of_Order'], axis=1, inplace=True) #처리를 어떻게 할지는 생각해 보자

        # 무결성 검사 4. outlier 검출 - 가격제한폭 초과 변동, 음수 있는지 확인
        # 거래 정지인 경우, 상한가/하한가에서 float 값 int 로 변환했을 때 값 차이나는 경우 고려할 것
        df_OHLCV['Pre_Close'] = df_OHLCV['Close'].shift(1)  # 전날의 Close 값 계산
        '''
        # 기준일에 따른 조건 설정
        conditions_before = (df_OHLCV['Date'] < self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close']*1.15 + 2)) | (df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.85 - 2).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close']*1.15 + 2)) | (df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.85  - 2).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close']*1.15 + 2)) | (df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.85 - 2).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close']*1.15 + 2)) | (df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.85 - 2).astype(float))
        ) & ~( # 거래 정지인 경우는 제외
            (df_OHLCV['Open'] == 0) & (df_OHLCV['High'] == 0) & (df_OHLCV['Low'] == 0) & (df_OHLCV['Close'] != 0) & (df_OHLCV['Volume'] == 0)
        )
        conditions_after = (df_OHLCV['Date'] >= self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close']*1.3 + 2)) | (df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.7 - 2).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close']*1.3 + 2)) | (df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.7 - 2).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close']*1.3 + 2)) | (df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.7 - 2).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close']*1.3 + 2)) | (df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.7 - 2).astype(float))
        )& ~( # 거래 정지인 경우는 제외
            (df_OHLCV['Open'] == 0) & (df_OHLCV['High'] == 0) & (df_OHLCV['Low'] == 0) & (df_OHLCV['Close'] != 0) & (df_OHLCV['Volume'] == 0)
        )
        '''

        # 기준일에 따른 조건 설정
        conditions_before = (df_OHLCV['Date'] < self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float))
        ) & ~(  # 거래 정지인 경우는 제외
                (df_OHLCV['Open'] == 0) & (df_OHLCV['High'] == 0) & (df_OHLCV['Low'] == 0) & (df_OHLCV['Close'] != 0)
        )
        conditions_after = (df_OHLCV['Date'] >= self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float))
        ) & ~(  # 거래 정지인 경우는 제외
                (df_OHLCV['Open'] == 0) & (df_OHLCV['High'] == 0) & (df_OHLCV['Low'] == 0) & (df_OHLCV['Close'] != 0)
        )

        conditions_negative = (df_OHLCV['Open'] < 0) | (df_OHLCV['High'] < 0) | (df_OHLCV['Low'] < 0) | (df_OHLCV['Close'] < 0) | (df_OHLCV['Volume'] < 0)
        final_conditions = conditions_before | conditions_after | conditions_negative
        outliers = df_OHLCV[final_conditions]
        if not outliers.empty:
            outliers = outliers['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, 가격제한폭 초과 혹은 음수: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_OHLCV_init}\\{listed_status}\\{datemanage.workday_str}\\outliers_list.txt"
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장

        df_OHLCV.drop(['Pre_Close'], axis=1, inplace=True)

    def make_date_reference(self):
        #2000년부터 월~금 날짜만 df로 만들기
        #쉬는날 목록 만들어서 제외시키기
        pass

    def update_date_reference(self):
        pass