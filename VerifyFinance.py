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
    def __init__(self, logger):
        super().__init__(logger)
        self.suffix = 'financial'  # 파일 이름 저장시 사용하는 접미사
        self.date_prefix = 'financial_day_ref' # date reference 파일의 접미사

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_VerifyFinance.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        self.path_data = config['path']['path_data']  # 데이터 경로
        self.path_date_ref = config['path']['path_date_ref'] # 날짜 기준 정보 경로

    def check_integrity(self, code, df_b_day_ref, df_data, datemanage, listed_status):
        df_data.reset_index(inplace=True)
        df_data['Date'] = pd.to_datetime(df_data['Date']).dt.date
        no_error = True

        # 무결성 검사 2. NaN 있는지 확인
        rows_with_nan = df_data.isna().any(axis=1)  # NaN 있는지 확인
        if rows_with_nan.any():
            NaN_exists = df_data[rows_with_nan]['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            NaN_exists_list = [f"{code}, NaN 값이 있는 날짜: {NaN_exists}"]
            self.logger.info(NaN_exists_list)
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\NaN_exists_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\NaN_exists_list.txt" # 임시
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 3. 시간적 일관성 확인
        path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\time_unconsistency_list.txt" # 임시
        # ref 와 지금 받아온 finance의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['Date'].isin(df_data['Date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_data = df_data[
            ~df_data['Date'].isin(df_b_day_ref['Date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
        if not unique_to_ref.empty:
            unique_to_ref = unique_to_ref['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_ref_list = [f'{code}, df_data에 없는 날짜: {unique_to_ref}']
            self.logger.info(unique_to_ref_list)
            utils.save_list_to_file_append(unique_to_ref_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        if not unique_to_df_data.empty:
            unique_to_df_OHLCV = unique_to_df_data['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_df_OHLCV_list = [f'{code}, df_data에만 추가로 있는 날짜: {unique_to_df_OHLCV}']
            self.logger.info(unique_to_df_OHLCV_list)
            utils.save_list_to_file_append(unique_to_df_OHLCV_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        # 시간 순으로 정렬되지 않은 행 찾기. 같은 날짜가 또 있는 것도 포함
        df_data['Out_of_Order'] = df_data['Date'] <= df_data['Date'].shift(1)
        out_of_order_rows = df_data[df_data['Out_of_Order']]
        if not out_of_order_rows.empty:
            out_of_order_rows = out_of_order_rows['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            out_of_order_rows_list = [f'{code}, 날짜가 역순인 부분: {out_of_order_rows}']
            self.logger.info(out_of_order_rows_list)
            utils.save_list_to_file_append(out_of_order_rows_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        # df_data.drop(['Out_of_Order'], axis=1, inplace=True) #처리를 어떻게 할지는 생각해 보자

        # 무결성 검사 4. outlier 검출 - 음수 있는지 확인(RV)
        # Check for negative values in RV column
        outliers = df_data[df_data["RV"] < 0]

        if not outliers.empty:
            outliers = outliers['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, RV 음수: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\outliers_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt" # 임시
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 5. # 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 두번 연속 같은 경우 검출 - 모든 항목에 대해서. 0인 경우는 제외
        consecutive_rows = []
        columns = ["RV", "GP", "OI", "NP"]

        for col in columns:
            for i in range(1, len(df_data)):
                if df_data.loc[i, col] == df_data.loc[i - 1, col] and df_data.loc[i, col] != 0:
                    consecutive_rows.append(df_data.iloc[i])

        consecutive_same_values = pd.DataFrame(consecutive_rows)
        if not consecutive_same_values.empty:
            consecutive_same_values = consecutive_same_values['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            consecutive_same_values_list = [f'{code}, 값이 2일 연속 같은 경우: {consecutive_same_values}']
            self.logger.info(consecutive_same_values_list)
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\consecutive_same_values_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\consecutive_same_values_list.txt"  # 임시
            utils.save_list_to_file_append(consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        return no_error