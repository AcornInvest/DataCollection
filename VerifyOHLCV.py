import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from VerifyData import VerifyData
from datetime import date, datetime, timedelta
import numpy as np

class VerifyOHLCV(VerifyData):
    '''
    OHLCV data의 무결성 검증용 child class
    '''
    def __init__(self, logger):
        super().__init__(logger)
        self.suffix = 'OHLCV_intelliquant'  # 파일 이름 저장시 사용하는 접미사
        self.limit_change_day = date(2015, 6, 15)  # 가격제한폭이 30%로 확대된 날
        self.clearance_days = 17 # 정리매매 기간 최대 15일 + 상폐직전 마진 2일

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_VerifyOHLCV.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        self.path_data = config['path']['path_data']  # 데이터 경로
        self.path_date_ref = config['path']['path_date_ref'] # 날짜 기준 정보 경로

    def check_integrity(self, code, df_b_day_ref, df_OHLCV, datemanage, listed_status):
        df_OHLCV.reset_index(inplace=True)
        df_OHLCV['Date'] = pd.to_datetime(df_OHLCV['Date']).dt.date

        # 무결성 검사 2. NaN 있는지 확인
        rows_with_nan = df_OHLCV.isna().any(axis=1)  # NaN 있는지 확인
        if rows_with_nan.any():
            NaN_exists = df_OHLCV[rows_with_nan]['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            NaN_exists_list = [f"{code}, NaN 값이 있는 날짜: {NaN_exists}"]
            self.logger.info(NaN_exists_list)
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\NaN_exists_list.txt"
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\NaN_exists_list.txt" # 임시
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장

        # 무결성 검사 3. 시간적 일관성 확인
        # path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\time_unconsistency_list.txt" # 임시
        # ref 와 지금 받아온 OHLCV의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['Date'].isin(df_OHLCV['Date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_OHLCV = df_OHLCV[
            ~df_OHLCV['Date'].isin(df_b_day_ref['Date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
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
        # df_OHLCV.drop(['Out_of_Order'], axis=1, inplace=True) #처리를 어떻게 할지는 생각해 보자

        # 무결성 검사 4. outlier 검출 - 가격제한폭 초과 변동, 음수 있는지 확인
        # 거래 정지인 경우, 상한가/하한가에서 float 값 int 로 변환했을 때 값 차이나는 경우 고려할 것
        df_OHLCV['Pre_Close'] = df_OHLCV['Close'].shift(1)  # 전날의 Close 값 계산

        # 정리 매매 고려 조건: 상폐일로부터 self.clearance_days 동안은 outlier 고려 안함
        last_index = len(df_OHLCV) - 1
        clearance_start_index = max(0, last_index - self.clearance_days)

        clearance_condition = (
                (listed_status != 'Delisted') |
                (df_OHLCV.index <= clearance_start_index)
        )

        # 기준일에 따른 조건 설정
        conditions_before = (df_OHLCV['Date'] < self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (
                    df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (
                            df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (
                            df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close'] * 1.151 + 1)) | (
                            df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.849 - 1).astype(float))
        ) & ~(df_OHLCV['Volume'] == 0) #거래 정지인 경우는 제외
        #conditions_before = conditions_before & clearance_condition

        conditions_after = (df_OHLCV['Date'] >= self.limit_change_day) & (
                (df_OHLCV['Open'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (
                    df_OHLCV['Open'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['High'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (
                            df_OHLCV['High'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['Low'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (
                            df_OHLCV['Low'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_OHLCV['Close'] > round(df_OHLCV['Pre_Close'] * 1.31 + 1)) | (
                            df_OHLCV['Close'] < np.floor(df_OHLCV['Pre_Close'] * 0.699 - 1).astype(float))
        ) & ~(df_OHLCV['Volume'] == 0) #거래 정지인 경우는 제외
        #conditions_after = conditions_after & clearance_condition

        conditions_negative = (df_OHLCV['Open'] <= 0) | (df_OHLCV['High'] <= 0) | (df_OHLCV['Low'] <= 0) | (
                    df_OHLCV['Close'] <= 0) | (df_OHLCV['Volume'] < 0)
        final_conditions = (conditions_before | conditions_after | conditions_negative) & clearance_condition
        outliers = df_OHLCV[final_conditions]
        if not outliers.empty:
            outliers = outliers['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, 가격제한폭 초과 혹은 음수: {outliers}']
            self.logger.info(outliers_list)
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\outliers_list.txt"
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt" # 임시
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장

        df_OHLCV.drop(['Pre_Close'], axis=1, inplace=True)
