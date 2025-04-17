import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from VerifyData import VerifyData
from datetime import date, datetime, timedelta
import numpy as np

class VerifyVolume(VerifyData):
    '''
    OHLCV data의 무결성 검증용 child class
    '''
    def __init__(self, logger, paths):
        super().__init__(logger, paths)
        self.suffix = 'volume'  # 파일 이름 저장시 사용하는 접미사
        self.path_data = paths.OHLCV_volume
        #self.limit_change_day = date(2015, 6, 15)  # 가격제한폭이 30%로 확대된 날
        #self.clearance_days = 17 # 정리매매 기간 최대 15일 + 상폐직전 마진 2일
        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사
        self.db_columns = ['date', 'volume', 'vf', 'vi', 'vr']

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_VerifyVolume.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

    def check_integrity(self, code, df_b_day_ref, df_data, datemanage):
        df_data.reset_index(inplace=True)
        df_data['date'] = pd.to_datetime(df_data['date']).dt.date
        no_error = True

        ## 무결성 검사 2. NaN 있는지 확인
        rows_with_nan = df_data.isna().any(axis=1)  # NaN 있는지 확인
        if rows_with_nan.any():
            NaN_exists = df_data[rows_with_nan]['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            NaN_exists_list = [f"{code}, NaN 값이 있는 날짜: {NaN_exists}"]
            self.logger.info(NaN_exists_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\NaN_exists_list.txt"
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        ## 무결성 검사 3. 시간적 일관성 확인
        path = f"{self.path_data}\\{datemanage.workday_str}\\time_unconsistency_list.txt"

        # ref 와 지금 받아온 volume의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['date'].isin(df_data['date'])]  # df_date_reference에만 있고 df_data에 없는 날짜.
        unique_to_df_data = df_data[
            ~df_data['date'].isin(df_b_day_ref['date'])]  # df_data에 있고 df_date_reference에만 없는 날짜.
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

        ## 무결성 검사 4. outlier 검출 - volume이 음수인 경우, volume이 0인데 다른 값들이 0이 아닌 경우,
        # F+I+R !=0인 경우 --> 이건 제외한다. 기타 법인, 국가등이 있을 수 있다.
        conditions_negative = df_data['volume'] < 0
        conditions_FIR_nonzero = (df_data['volume'] == 0) & ((df_data['vf'] != 0) | (df_data['vi'] != 0) | (df_data['vr'] != 0))
        #conditions_FIR_sum_nonzero = (df_data['VF'] + df_data['VI'] + df_data['VR']) != 0

        #final_conditions = (conditions_negative | conditions_FIR_nonzero | conditions_FIR_sum_nonzero)
        final_conditions = (conditions_negative | conditions_FIR_nonzero)
        outliers = df_data[final_conditions]
        if not outliers.empty:
            outliers = outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, volume 음수 or FIR 에러: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\outliers_list.txt"
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        ## 무결성 검사 5. # 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 이틀 연속 같은 경우 검출
        '''
        df_data['Pre_Volume'] = df_data['Volume'].shift(1)  # 전날의 Volume 값 계산
        df_data['Pre_VF'] = df_data['VF'].shift(1)  # 전날의 VF 값 계산
        df_data['Pre_VI'] = df_data['VI'].shift(1)  # 전날의 VI 값 계산
        df_data['Pre_VR'] = df_data['VR'].shift(1)  # 전날의 VR 값 계산

        conditions_same_value = ((df_data['Volume'] != 0) & ( df_data['Pre_Volume'] == df_data['Volume']) & \
        (df_data['VF'] != 0) & (df_data['Pre_VF'] == df_data['Volume']) & \
        (df_data['VI'] != 0) & (df_data['Pre_VI'] == df_data['Volume']) & \
        (df_data['VR'] != 0) & (df_data['Pre_VR'] == df_data['Volume']))

        consecutive_same_values = df_data[conditions_same_value]
        '''
        #if code == '00003A':
        # 각 행에서 [volume, vf, vi, vr] 열의 값이 2행 동안 연속적으로 동일한 값이 n개 이상인 행을 찾는 함수
        # n=3 인 경우도 많다. 네이버 증권에서 검색해도 동일하다
        def has_n_consecutive_same_values(df, n):
            result_indices = []

            # 행을 순회하며 연속된 2행의 동일한 값 체크
            for i in range(len(df) - 1):
                row1 = df.iloc[i]
                row2 = df.iloc[i + 1]
                count = sum((row1 == row2) & (row1 != 0) & (row2 != 0))
                if count >= n:
                    result_indices.append(i)
                    result_indices.append(i + 1)

            # 결과 인덱스에서 중복 제거
            result_indices = sorted(set(result_indices))
            return df.iloc[result_indices]

        # 최소 연속 값의 수
        n = 4

        # 연속된 동일한 값이 n개 이상인 행 찾기
        consecutive_same_values = has_n_consecutive_same_values(df_data, n)

        if not consecutive_same_values.empty:
            consecutive_same_values = consecutive_same_values['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            consecutive_same_values_list = [f'{code}, 값이 2일 연속 같은 경우: {consecutive_same_values}']
            self.logger.info(consecutive_same_values_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\consecutive_same_values_list.txt" # 임시
            utils.save_list_to_file_append(consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        return no_error

    ''' # 2025.3.31 이거는 verifyOHLCV 에서 쓰는건 아닌가? 여기 왜 있지?
    # n일간 연속적으로 같은 값을 가지는지 판별하는 함수
    def check_continuous_same_value(self, df, n, compare_columns):
        result = pd.Series([False] * len(df), index=df.index)
        for i in range(len(df) - n + 1):
            sub_df = df.iloc[i:i + n]
            if all(sub_df['volume'] != 0): # 'Volume'이 0인 경우는 비교에서 제외
                is_continuous = all(sub_df[compare_columns].apply(lambda col: len(set(col)) == 1, axis=0))
                result.iloc[i + n - 1] = is_continuous
        return result
    '''
