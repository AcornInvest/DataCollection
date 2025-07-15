import os
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from VerifyData import VerifyData
from datetime import date, datetime, timedelta
import numpy as np

class VerifyCompensation(VerifyData):
    '''
    NoOfShare data의 무결성 검증용 child class
    '''
    def __init__(self, logger, paths):
        super().__init__(logger, paths)
        self.suffix = 'compensation'  # 파일 이름 저장시 사용하는 접미사
        self.path_data = paths.OHLCV_Compensation
        self.date_prefix = 'bussiness_day_ref' # date reference 파일의 접미사
        self.db_columns = ['date', 'old_share', 'new_share']

    def load_config(self):
        super().load_config()

    #def check_integrity(self, code, df_b_day_ref, df_data, datemanage, listed_status):
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
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\NaN_exists_list.txt"  # 임시
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 3. 시간적 일관성 확인
        # 거래일 목록 ref 읽어오기 --> 2025.3.24 이거 수정하자. df_b_day_ref를 쓰면 된다.
        # 애초에 이게 왜 있지? 주석처리하자.(2025.3.23)
        # df_data의 날짜가 df_b_day_ref를보다 하루씩 더 늦은 값이 나올수가 있나? intelliquant에서?
        # 근데 그건 update할 때는 상관없지 않을까?
        '''
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        df_business_days = pd.read_excel(path_date_ref)
        df_business_days['date'] = pd.to_datetime(df_business_days['date']).dt.date

        # Finding the last value in df_b_day_ref['Date']
        last_date = df_b_day_ref['date'].iloc[-1]

        # Finding the index of the row in df_business_days that matches the last_date
        index_in_business_days = df_business_days.index[df_business_days['date'] == last_date].tolist()

        # If a match is found, get the date from the next row
        if index_in_business_days:
            next_index = index_in_business_days[0] + 1
            if next_index < len(df_business_days):
                next_date = df_business_days['date'].iloc[next_index]
                # Adding the next date to df_b_day_ref
                #df_b_day_ref = df_b_day_ref.append({'Date': next_date}, ignore_index=True)
                df_b_day_ref = pd.concat([df_b_day_ref, pd.DataFrame({'date': [next_date]})], ignore_index=True)
        '''
        path = f"{self.path_data}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\time_unconsistency_list.txt"
        ## date ref 와 지금 받아온 df_data의 date 비교
        # ref와 df_data의 첫번째 날짜가 같은지 확인
        if df_b_day_ref['date'].iloc[0] != df_data['date'].iloc[0]:
            different_start_date_list = [f'{code}, 시작 날짜가 ref_date와 다름']
            self.logger.info(different_start_date_list)
            utils.save_list_to_file_append(different_start_date_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # df_data의 마지막 날짜가 df_b_day_ref의 마지막 날짜보다 더 늦는 경우
        if df_b_day_ref['date'].iloc[-1] < df_data['date'].iloc[-1]:
            end_date_error_list = [f'{code}, 마지막 날짜가 ref_date의 마지막보다 늦음']
            self.logger.info(end_date_error_list)
            utils.save_list_to_file_append(end_date_error_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 시간 순으로 정렬되지 않은 행 찾기. 같은 날짜가 또 있는 것도 포함
        df_data['Out_of_Order'] = df_data['date'] <= df_data['date'].shift(1)
        out_of_order_rows = df_data[df_data['Out_of_Order']]
        if not out_of_order_rows.empty:
            out_of_order_rows = out_of_order_rows['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            out_of_order_rows_list = [f'{code}, 날짜가 역순인 부분: {out_of_order_rows}']
            self.logger.info(out_of_order_rows_list)
            utils.save_list_to_file_append(out_of_order_rows_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 4. outlier 검출 - 음수이거나 중간값이 0인지 확인
        #outliers = df_data[(df_data["OldNoShare"] < 0) | (df_data["NewNoShare"] < 0)]
        condition_negative = (df_data["old_share"] < 0) | (df_data["new_share"] < 0)
        condition_zero = ((df_data["old_share"] == 0) | (df_data["new_share"] == 0)) & (df_data.index != 0) & (df_data.index != len(df_data) - 1)
        outliers = df_data[condition_negative | condition_zero]

        if not outliers.empty:
            outliers = outliers['date'].apply(lambda d: d.strftime('%Y-%m-%d')).tolist()
            outliers_list = [f'{code}, 음수 혹은 중간값 0: {outliers}']
            self.logger.info(outliers_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\outliers_list.txt"  # 임시
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\outliers_list.txt"  # 임시
            utils.save_list_to_file_append(outliers_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 5. # 연속적으로 같은 값을 가지는지 여부를 판별
        # 값이 두번 연속 같은 경우 검출 - 모든 항목에 대해서.
        consecutive_rows = []
        columns = ["old_share", "new_share"]

        for col in columns:
            for i in range(1, len(df_data)):
                if df_data.loc[i, col] == df_data.loc[i - 1, col] and df_data.loc[i, col] != 0:
                    consecutive_rows.append(df_data.iloc[i])

        consecutive_same_values = pd.DataFrame(consecutive_rows)
        if not consecutive_same_values.empty:
            consecutive_same_values = consecutive_same_values['date'].apply(
                lambda d: d.strftime('%Y-%m-%d')).tolist()
            consecutive_same_values_list = [f'{code}, 값이 연속으로 같은 경우: {consecutive_same_values}']
            self.logger.info(consecutive_same_values_list)
            path = f"{self.path_data}\\{datemanage.workday_str}\\consecutive_same_values_list.txt"  # 임시
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\consecutive_same_values_list.txt"  # 임시
            utils.save_list_to_file_append(consecutive_same_values_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        ''' # 2024.4.17 이거 따로 체크하지 않는다. 어차피 combine할 때 ohlc 변동을 모든 종목에 대해 체크한다.
        # 2025.3.24
        #주식수가 변경된 것을 따로 파일로 저장해야 한다. ohlcv 가져올 때 참고하도록 한다.
        flag_share_modified = False
        if no_error:
            if len(df_data) > 1: # share 가 달라지지 않으면 해당 종목은 startday 값 1줄 뿐이다. 이런 경우는 merge 할 때도 추가할 필요가 없다.
                # 상폐되는 경우는 마지막 값이 0이다.
                # ohlcv 가져올 때 갱신해야 하는 조건:
                # (1) 기존 share 값에서 변화가 있어야 함.
                # (2) 상폐되는 경우는 상폐 되기 전에 기존값과 다른 값이 1번 이상 있어야 함
                if df_data['new_share'].iloc[-1] == 0:# 상폐되는 경우. (2)에 해당
                    if len(df_data)>2: # 상폐되기 전 중간 변화값이 1회 이상 있는지 확인
                        flag_share_modified = True
                elif df_data['new_share'].iloc[-2] != df_data['new_share'].iloc[-1]: #상폐되지 않는 경우. (1)에 해당. 마지막 값과 직전값에 차이가 있는지 확인
                    flag_share_modified = True

        return no_error, flag_share_modified
        '''

        return no_error
