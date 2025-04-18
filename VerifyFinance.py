import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from VerifyData import VerifyData
from datetime import date, datetime, timedelta
import numpy as np

class VerifyFinance(VerifyData):
    '''
    Finance data의 무결성 검증용 child class
    '''
    def __init__(self, logger, paths):
        super().__init__(logger, paths)
        self.suffix = 'financial'  # 파일 이름 저장시 사용하는 접미사
        self.path_data = paths.Financial

        self.date_prefix = 'financial_day_ref' # date reference 파일의 접미사
        self.db_columns = ['date', 'rv', 'gp', 'oi', 'np', 'ev_evitda', 'per', 'pbr', 'psr', 'pcr', 'gpa', 'roa', 'roe']

    def load_config(self):
        super().load_config()

    def check_integrity(self, code, df_b_day_ref, df_data, datemanage):
        df_data.reset_index(inplace=True)
        df_data['date'] = pd.to_datetime(df_data['date']).dt.date
        no_error = True

        # 무결성 검사 2. NaN 있는지 확인
        rows_with_nan = df_data.isna().any(axis=1)  # NaN 있는지 확인
        if rows_with_nan.any():
            NaN_exists = df_data[rows_with_nan]['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            NaN_exists_list = [f"{code}, NaN 값이 있는 날짜: {NaN_exists}"]
            self.logger.info(NaN_exists_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\NaN_exists_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\NaN_exists_list.txt" # 임시
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 3. 시간적 일관성 확인
        path = f"{self.path_data}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\time_unconsistency_list.txt" # 임시
        # ref 와 지금 받아온 finance의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['date'].isin(df_data['date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_data = df_data[
            ~df_data['date'].isin(df_b_day_ref['date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
        if not unique_to_ref.empty:
            unique_to_ref = unique_to_ref['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_ref_list = [f'{code}, df_data에 없는 날짜: {unique_to_ref}']
            self.logger.info(unique_to_ref_list)
            utils.save_list_to_file_append(unique_to_ref_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        if not unique_to_df_data.empty:
            unique_to_df_OHLCV = unique_to_df_data['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_df_OHLCV_list = [f'{code}, df_data에만 추가로 있는 날짜: {unique_to_df_OHLCV}']
            self.logger.info(unique_to_df_OHLCV_list)
            utils.save_list_to_file_append(unique_to_df_OHLCV_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        # 시간 순으로 정렬되지 않은 행 찾기. 같은 날짜가 또 있는 것도 포함
        df_data['out_of_order'] = df_data['date'] <= df_data['date'].shift(1)
        out_of_order_rows = df_data[df_data['out_of_order']]
        if not out_of_order_rows.empty:
            out_of_order_rows = out_of_order_rows['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            out_of_order_rows_list = [f'{code}, 날짜가 역순인 부분: {out_of_order_rows}']
            self.logger.info(out_of_order_rows_list)
            utils.save_list_to_file_append(out_of_order_rows_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        # df_data.drop(['Out_of_Order'], axis=1, inplace=True) #처리를 어떻게 할지는 생각해 보자

        # 무결성 검사 4. outlier 검출 - 음수 있는지 확인(RV)
        # Check for negative values in RV column
        outliers = df_data[df_data["rv"] < 0]

        if not outliers.empty:
            outliers = outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, rv 음수: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\outliers_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt" # 임시
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 5. # 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 두번 연속 같은 경우 검출 - 모든 항목에 대해서. 0인 경우는 제외
        consecutive_rows = []
        columns = ["rv", "gp", "oi", "np"]

        for col in columns:
            for i in range(1, len(df_data)):
                if df_data.loc[i, col] == df_data.loc[i - 1, col] and df_data.loc[i, col] != 0:
                    consecutive_rows.append(df_data.iloc[i])

        consecutive_same_values = pd.DataFrame(consecutive_rows)
        if not consecutive_same_values.empty:
            consecutive_same_values = consecutive_same_values['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            consecutive_same_values_list = [f'{code}, 값이 2 분기 연속 같은 경우: {consecutive_same_values}']
            self.logger.info(consecutive_same_values_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\consecutive_same_values_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\consecutive_same_values_list.txt"  # 임시
            utils.save_list_to_file_append(consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        return no_error