import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from VerifyData import VerifyData
from datetime import date, datetime, timedelta
import numpy as np

class VerifyCombinedData(VerifyData):
    '''
    Combined Data의 무결성 검증용 child class
    '''
    def __init__(self, logger, paths, datemanage, flag_mod=False):
        super().__init__(logger, paths, flag_mod)

        self.suffix = 'combined_ohlcv'
        self.limit_change_day = datetime(2015, 6, 15)  # 가격제한폭이 30%로 확대된 날
        self.clearance_days = 17 # 정리매매 기간 최대 15일 + 상폐직전 마진 2일
        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사
        self.db_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'vf', 'vi', 'vr', 'cap', 'share']
        self.path_data = paths.OHLCV_Combined

        codelist_path = f'{self.path_codeLists}\\Delisted\\Delisted_Ticker_{datemanage.workday_str}_modified.xlsx'
        self.df_codelist_delisted = pd.read_excel(codelist_path, index_col=0)
        self.df_codelist_delisted['Code'] = self.df_codelist_delisted['Code'].astype(str)
        self.df_codelist_delisted['Code'] = self.df_codelist_delisted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기

    def load_config(self):
        super().load_config()

    def check_integrity(self, code, df_b_day_ref, df_data, datemanage):
        df_data.reset_index(inplace=True)
        #df_data['date'] = pd.to_datetime(df_data['date']).dt.date
        df_data['date'] = pd.to_datetime(df_data['date'])
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
        # ref 와 지금 받아온 OHLCV의 date 비교
        unique_to_ref = df_b_day_ref[
            ~df_b_day_ref['date'].isin(df_data['date'])]  # df_date_reference에만 있고 df_OHLCV에 없는 날짜.
        unique_to_df_OHLCV = df_data[
            ~df_data['date'].isin(df_b_day_ref['date'])]  # df_OHLCV에 있고 df_date_reference에만 없는 날짜.
        if not unique_to_ref.empty:
            unique_to_ref = unique_to_ref['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_ref_list = [f'{code}, df_OHLCV에 없는 날짜: {unique_to_ref}']
            self.logger.info(unique_to_ref_list)
            utils.save_list_to_file_append(unique_to_ref_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        if not unique_to_df_OHLCV.empty:
            unique_to_df_OHLCV = unique_to_df_OHLCV['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            unique_to_df_OHLCV_list = [f'{code}, df_OHLCV에만 추가로 있는 날짜: {unique_to_df_OHLCV}']
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

        # 무결성 검사 4. 가격 outlier 검출 - 가격제한폭 초과 변동, 음수 있는지 확인, 이틀이상 값이 동일한지 확인
        # 거래 정지인 경우, 상한가/하한가에서 float 값 int 로 변환했을 때 값 차이나는 경우 고려할 것
        df_data['pre_close'] = df_data['close'].shift(1)  # 전날의 close 값 계산

        # 정리 매매 고려 조건: 상폐일로부터 self.clearance_days 동안은 outlier 고려 안함
        last_index = len(df_data) - 1
        clearance_start_index = max(0, last_index - self.clearance_days)

        if code not in set(self.df_codelist_delisted['Code']):
            # code가 delisted 리스트에 없다면: 모든 row가 True
            clearance_condition = pd.Series([True] * len(df_data), index=df_data.index) # 정리 매매가 아닌 시점일 때 clearance_condition = True
        else:
            # 아니라면 index 조건으로 필터링
            clearance_condition = df_data.index <= clearance_start_index

        '''
        clearance_condition = ( # 정리 매매가 아닌 시점일 때 True
                (listed_status != 'Delisted') |                
                (df_data.index <= clearance_start_index)
        )
        '''

        # 기준일에 따른 조건 설정
        conditions_before = (df_data['date'] < self.limit_change_day) & (
                (df_data['open'] > round(df_data['pre_close'] * 1.151 + 1)) | (
                    df_data['open'] < np.floor(df_data['pre_close'] * 0.849 - 1).astype(float)) |
                (df_data['high'] > round(df_data['pre_close'] * 1.151 + 1)) | (
                            df_data['high'] < np.floor(df_data['pre_close'] * 0.849 - 1).astype(float)) |
                (df_data['low'] > round(df_data['pre_close'] * 1.151 + 1)) | (
                            df_data['low'] < np.floor(df_data['pre_close'] * 0.849 - 1).astype(float)) |
                (df_data['close'] > round(df_data['pre_close'] * 1.151 + 1)) | (
                            df_data['close'] < np.floor(df_data['pre_close'] * 0.849 - 1).astype(float))
        ) & ~(df_data['volume'] == 0) #거래 정지인 경우는 제외
        #conditions_before = conditions_before & clearance_condition

        conditions_after = (df_data['date'] >= self.limit_change_day) & (
                (df_data['open'] > round(df_data['pre_close'] * 1.31 + 1)) | (
                    df_data['open'] < np.floor(df_data['pre_close'] * 0.699 - 1).astype(float)) |
                (df_data['high'] > round(df_data['pre_close'] * 1.31 + 1)) | (
                            df_data['high'] < np.floor(df_data['pre_close'] * 0.699 - 1).astype(float)) |
                (df_data['low'] > round(df_data['pre_close'] * 1.31 + 1)) | (
                            df_data['low'] < np.floor(df_data['pre_close'] * 0.699 - 1).astype(float)) |
                (df_data['close'] > round(df_data['pre_close'] * 1.31 + 1)) | (
                            df_data['close'] < np.floor(df_data['pre_close'] * 0.699 - 1).astype(float))
        ) & ~(df_data['volume'] == 0) #거래 정지인 경우는 제외
        #conditions_after = conditions_after & clearance_condition

        conditions_negative = (df_data['open'] <= 0) | (df_data['high'] <= 0) | (df_data['low'] <= 0) | (
                    df_data['close'] <= 0) | (df_data['volume'] < 0)

        final_conditions = (conditions_before | conditions_after | conditions_negative) & clearance_condition
        price_outliers = df_data[final_conditions]
        if not price_outliers.empty:
            price_outliers = price_outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            price_outliers_list = [f'{code}, 가격제한폭 초과 혹은 음수: {price_outliers}']
            self.logger.info(price_outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\price_outliers_list.txt"
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\price_outliers_list.txt" # 임시
            utils.save_list_to_file_append(price_outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        ## 무결성 검사 5. Volume outlier 검출 - volume이 음수인 경우, volume이 0인데 다른 값들이 0이 아닌 경우,
        # F+I+R !=0인 경우 --> 이건 제외한다. 기타 법인, 국가등이 있을 수 있다.
        conditions_volume_negative = df_data['volume'] < 0
        conditions_FIR_nonzero = (df_data['volume'] == 0) & (
                    (df_data['vf'] != 0) | (df_data['vi'] != 0) | (df_data['vr'] != 0))
        # conditions_FIR_sum_nonzero = (df_data['VF'] + df_data['VI'] + df_data['VR']) != 0

        # final_conditions = (conditions_negative | conditions_FIR_nonzero | conditions_FIR_sum_nonzero)
        final_volume_conditions = (conditions_volume_negative | conditions_FIR_nonzero)
        volume_outliers = df_data[final_volume_conditions]
        if not volume_outliers.empty:
            volume_outliers = volume_outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            volume_outliers_list = [f'{code}, volume 음수 or FIR 에러: {volume_outliers}']
            self.logger.info(volume_outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\volume_outliers_list.txt"
            utils.save_list_to_file_append(volume_outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 6. share outlier 검출 - 음수이거나 중간값이 0인지 확인
        condition_share_negative = df_data["share"] < 0
        condition_share_zero = (df_data["share"] == 0) & (df_data.index != 0) & (df_data.index != len(df_data) - 1) # 첫째행, 마지막 행이 아닌데 값이 0인 경우
        volume_outliers = df_data[condition_share_negative | condition_share_zero]

        if not volume_outliers.empty:
            volume_outliers = volume_outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            volume_outliers_list = [f'{code}, 음수 혹은 중간값 0: {volume_outliers}']
            self.logger.info(volume_outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\volume_outliers_list.txt"  # 임시
            # path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt"  # 임시
            utils.save_list_to_file_append(volume_outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 7. # price 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 이틀 연속 같은 경우 검출 - OHLC 중 이틀연속 값이 같은 컬럼이 2개 이상인 경우.
        # 거래 정지인 경우는 검출 제외
        '''
        df_data['pre_open'] = df_data['open'].shift(1)  # 전날의 open 값 계산
        df_data['pre_high'] = df_data['high'].shift(1)  # 전날의 high 값 계산
        df_data['pre_low'] = df_data['low'].shift(1)  # 전날의 low 값 계산
        df_data['pre_volume'] = df_data['volume'].shift(1)  # 전날의 volume 값 계산 

        df_data['pre_open_2'] = df_data['open'].shift(2)  # 2일전의 open 값 계산
        df_data['pre_high_2'] = df_data['high'].shift(2)  # 2일전의 high 값 계산
        df_data['pre_low_2'] = df_data['low'].shift(2)  # 2일전의 low 값 계산
        df_data['pre_close_2'] = df_data['close'].shift(2)  # 2일전의 close 값 계산
        df_data['pre_volume_2'] = df_data['volume'].shift(2)  # 2일전의 volume 값 계산
        df_data['pre_open_3'] = df_data['open'].shift(3)  # 2일전의 open 값 계산
        df_data['pre_high_3'] = df_data['high'].shift(3)  # 2일전의 high 값 계산
        df_data['pre_low_3'] = df_data['low'].shift(3)  # 2일전의 low 값 계산
        df_data['pre_close_3'] = df_data['close'].shift(3)  # 2일전의 close 값 계산
        df_data['pre_volume_3'] = df_data['volume'].shift(3)  # 2일전의 volume 값 계산
        consecutive_same_values = df_data[df_data.apply(self.filter_rows, axis=1)]
        '''

        # OHLC 전체가 n일간 같은 값을 갖는지 판별
        '''
        n = 6
        compare_columns = ['open', 'high', 'low', 'close']
        continuous_same_value_mask = self.check_continuous_same_value(df_data, n, compare_columns)
        price_consecutive_same_values = df_data[continuous_same_value_mask]

        if not consecutive_same_values.empty:
            price_consecutive_same_values = price_consecutive_same_values['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            price_consecutive_same_values_list = [f'{code}, 값이 {n}일 연속 같은 경우: {price_consecutive_same_values}']
            self.logger.info(price_consecutive_same_values_list)
            path = f"{self.path_data}\\{datemanage.workday_str}_merged\\price_consecutive_same_values_list.txt" # 임시
            utils.save_list_to_file_append(price_consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
        '''

        ## 무결성 검사 8. # volume 연속적으로 같은 값을 가지는지 여부를 판별
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
        volume_consecutive_same_values = has_n_consecutive_same_values(df_data, n)

        if not volume_consecutive_same_values.empty:
            volume_consecutive_same_values = volume_consecutive_same_values['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            volume_consecutive_same_values_list = [f'{code}, 값이 2일 연속 같은 경우: {volume_consecutive_same_values}']
            self.logger.info(volume_consecutive_same_values_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\volume_consecutive_same_values_list.txt"  # 임시
            utils.save_list_to_file_append(volume_consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        return no_error

    # n일간 연속적으로 같은 값을 가지는지 판별하는 함수
    def check_continuous_same_value(self, df, n, compare_columns):
        result = pd.Series([False] * len(df), index=df.index)
        for i in range(len(df) - n + 1):
            sub_df = df.iloc[i:i + n]
            if all(sub_df['volume'] != 0): # 'volume'이 0인 경우는 비교에서 제외
                is_continuous = all(sub_df[compare_columns].apply(lambda col: len(set(col)) == 1, axis=0))
                result.iloc[i + n - 1] = is_continuous
        return result

    def filter_rows(self, row): # 값이 이틀 연속 같은 경우의 행을 추출 - 3일 연속으로 바꿀까..
        if row['volume'] == 0 or row['pre_volume'] == 0 or row['pre_volume_2'] == 0 or row['pre_volume_3'] == 0:
            return False
        '''
        for col in ['open', 'high', 'low', 'close']:
            if row[col] == 0:
                return False
        '''
        same_value_row_count = 0

        same_value_count_1 = sum([
            row['open'] == row['pre_open'],
            row['high'] == row['pre_high'],
            row['low'] == row['pre_low'],
            row['close'] == row['pre_close']
        ])
        if same_value_count_1 >=4:
            same_value_row_count += 1

        same_value_count_2 = sum([
            row['pre_open'] == row['pre_open_2'],
            row['pre_high'] == row['pre_high_2'],
            row['pre_low'] == row['pre_low_2'],
            row['pre_close'] == row['pre_close_2']
        ])
        if same_value_count_2 >=4:
            same_value_row_count += 1

        same_value_count_3 = sum([
            row['pre_open_2'] == row['pre_open_3'],
            row['pre_high_2'] == row['pre_high_3'],
            row['pre_low_2'] == row['pre_low_3'],
            row['pre_close_2'] == row['pre_close_3']
        ])
        if same_value_count_3 >=4:
            same_value_row_count += 1

        return same_value_row_count >= 3
