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
        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_VerifyOHLCV.ini'
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
        # ref 와 지금 받아온 OHLCV의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['Date'].isin(df_data['Date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_OHLCV = df_data[
            ~df_data['Date'].isin(df_b_day_ref['Date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
        if not unique_to_ref.empty:
            unique_to_ref = unique_to_ref['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_ref_list = [f'{code}, df_OHLCV에 없는 날짜: {unique_to_ref}']
            self.logger.info(unique_to_ref_list)
            utils.save_list_to_file_append(unique_to_ref_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        if not unique_to_df_OHLCV.empty:
            unique_to_df_OHLCV = unique_to_df_OHLCV['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_df_OHLCV_list = [f'{code}, df_OHLCV에만 추가로 있는 날짜: {unique_to_df_OHLCV}']
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

        # 무결성 검사 4. outlier 검출 - 가격제한폭 초과 변동, 음수 있는지 확인, 이틀이상 값이 동일한지 확인
        # 거래 정지인 경우, 상한가/하한가에서 float 값 int 로 변환했을 때 값 차이나는 경우 고려할 것
        df_data['Pre_Close'] = df_data['Close'].shift(1)  # 전날의 Close 값 계산

        # 정리 매매 고려 조건: 상폐일로부터 self.clearance_days 동안은 outlier 고려 안함
        last_index = len(df_data) - 1
        clearance_start_index = max(0, last_index - self.clearance_days)

        clearance_condition = (
                (listed_status != 'Delisted') |
                (df_data.index <= clearance_start_index)
        )

        # 기준일에 따른 조건 설정
        conditions_before = (df_data['Date'] < self.limit_change_day) & (
                (df_data['Open'] > round(df_data['Pre_Close'] * 1.151 + 1)) | (
                    df_data['Open'] < np.floor(df_data['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_data['High'] > round(df_data['Pre_Close'] * 1.151 + 1)) | (
                            df_data['High'] < np.floor(df_data['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_data['Low'] > round(df_data['Pre_Close'] * 1.151 + 1)) | (
                            df_data['Low'] < np.floor(df_data['Pre_Close'] * 0.849 - 1).astype(float)) |
                (df_data['Close'] > round(df_data['Pre_Close'] * 1.151 + 1)) | (
                            df_data['Close'] < np.floor(df_data['Pre_Close'] * 0.849 - 1).astype(float))
        ) & ~(df_data['Volume'] == 0) #거래 정지인 경우는 제외
        #conditions_before = conditions_before & clearance_condition

        conditions_after = (df_data['Date'] >= self.limit_change_day) & (
                (df_data['Open'] > round(df_data['Pre_Close'] * 1.31 + 1)) | (
                    df_data['Open'] < np.floor(df_data['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_data['High'] > round(df_data['Pre_Close'] * 1.31 + 1)) | (
                            df_data['High'] < np.floor(df_data['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_data['Low'] > round(df_data['Pre_Close'] * 1.31 + 1)) | (
                            df_data['Low'] < np.floor(df_data['Pre_Close'] * 0.699 - 1).astype(float)) |
                (df_data['Close'] > round(df_data['Pre_Close'] * 1.31 + 1)) | (
                            df_data['Close'] < np.floor(df_data['Pre_Close'] * 0.699 - 1).astype(float))
        ) & ~(df_data['Volume'] == 0) #거래 정지인 경우는 제외
        #conditions_after = conditions_after & clearance_condition

        conditions_negative = (df_data['Open'] <= 0) | (df_data['High'] <= 0) | (df_data['Low'] <= 0) | (
                    df_data['Close'] <= 0) | (df_data['Volume'] < 0)

        final_conditions = (conditions_before | conditions_after | conditions_negative) & clearance_condition
        outliers = df_data[final_conditions]
        if not outliers.empty:
            outliers = outliers['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, 가격제한폭 초과 혹은 음수: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}\\outliers_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt" # 임시
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 5. # 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 이틀 연속 같은 경우 검출 - OHLC 중 이틀연속 값이 같은 컬럼이 2개 이상인 경우.
        # 거래 정지인 경우는 검출 제외
        '''
        df_data['Pre_Open'] = df_data['Open'].shift(1)  # 전날의 Open 값 계산
        df_data['Pre_High'] = df_data['High'].shift(1)  # 전날의 High 값 계산
        df_data['Pre_Low'] = df_data['Low'].shift(1)  # 전날의 Low 값 계산
        df_data['Pre_Volume'] = df_data['Volume'].shift(1)  # 전날의 Volume 값 계산 

        df_data['Pre_Open_2'] = df_data['Open'].shift(2)  # 2일전의 Open 값 계산
        df_data['Pre_High_2'] = df_data['High'].shift(2)  # 2일전의 High 값 계산
        df_data['Pre_Low_2'] = df_data['Low'].shift(2)  # 2일전의 Low 값 계산
        df_data['Pre_Close_2'] = df_data['Close'].shift(2)  # 2일전의 Close 값 계산
        df_data['Pre_Volume_2'] = df_data['Volume'].shift(2)  # 2일전의 Volume 값 계산
        df_data['Pre_Open_3'] = df_data['Open'].shift(3)  # 2일전의 Open 값 계산
        df_data['Pre_High_3'] = df_data['High'].shift(3)  # 2일전의 High 값 계산
        df_data['Pre_Low_3'] = df_data['Low'].shift(3)  # 2일전의 Low 값 계산
        df_data['Pre_Close_3'] = df_data['Close'].shift(3)  # 2일전의 Close 값 계산
        df_data['Pre_Volume_3'] = df_data['Volume'].shift(3)  # 2일전의 Volume 값 계산
        consecutive_same_values = df_data[df_data.apply(self.filter_rows, axis=1)]
        '''

        # OHLC 전체가 n일간 같은 값을 갖는지 판별
        '''
        n = 6
        compare_columns = ['Open', 'High', 'Low', 'Close']
        continuous_same_value_mask = self.check_continuous_same_value(df_data, n, compare_columns)
        consecutive_same_values = df_data[continuous_same_value_mask]

        if not consecutive_same_values.empty:
            consecutive_same_values = consecutive_same_values['Date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            consecutive_same_values_list = [f'{code}, 값이 {n}일 연속 같은 경우: {consecutive_same_values}']
            self.logger.info(consecutive_same_values_list)
            path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\consecutive_same_values_list.txt" # 임시
            utils.save_list_to_file_append(consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
        '''
        return no_error

    # n일간 연속적으로 같은 값을 가지는지 판별하는 함수
    def check_continuous_same_value(self, df, n, compare_columns):
        result = pd.Series([False] * len(df), index=df.index)
        for i in range(len(df) - n + 1):
            sub_df = df.iloc[i:i + n]
            if all(sub_df['Volume'] != 0): # 'Volume'이 0인 경우는 비교에서 제외
                is_continuous = all(sub_df[compare_columns].apply(lambda col: len(set(col)) == 1, axis=0))
                result.iloc[i + n - 1] = is_continuous
        return result

    def filter_rows(self, row): # 값이 이틀 연속 같은 경우의 행을 추출 - 3일 연속으로 바꿀까..
        if row['Volume'] == 0 or row['Pre_Volume'] == 0 or row['Pre_Volume_2'] == 0 or row['Pre_Volume_3'] == 0:
            return False
        '''
        for col in ['Open', 'High', 'Low', 'Close']:
            if row[col] == 0:
                return False
        '''
        same_value_row_count = 0

        same_value_count_1 = sum([
            row['Open'] == row['Pre_Open'],
            row['High'] == row['Pre_High'],
            row['Low'] == row['Pre_Low'],
            row['Close'] == row['Pre_Close']
        ])
        if same_value_count_1 >=4:
            same_value_row_count += 1

        same_value_count_2 = sum([
            row['Pre_Open'] == row['Pre_Open_2'],
            row['Pre_High'] == row['Pre_High_2'],
            row['Pre_Low'] == row['Pre_Low_2'],
            row['Pre_Close'] == row['Pre_Close_2']
        ])
        if same_value_count_2 >=4:
            same_value_row_count += 1

        same_value_count_3 = sum([
            row['Pre_Open_2'] == row['Pre_Open_3'],
            row['Pre_High_2'] == row['Pre_High_3'],
            row['Pre_Low_2'] == row['Pre_Low_3'],
            row['Pre_Close_2'] == row['Pre_Close_3']
        ])
        if same_value_count_3 >=4:
            same_value_row_count += 1

        return same_value_row_count >= 3
