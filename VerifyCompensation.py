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
    def __init__(self, logger):
        super().__init__(logger)
        self.suffix = 'compensation'  # 파일 이름 저장시 사용하는 접미사
        self.date_prefix = 'bussiness_day_ref' # date reference 파일의 접미사

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_VerifyCompensation.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        self.path_data = config['path']['path_data']  # 데이터 경로
        self.path_date_ref = config['path']['path_date_ref'] # 날짜 기준 정보 경로

    #def check_integrity(self, code, df_b_day_ref, df_data, datemanage, listed_status):
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
            #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\NaN_exists_list.txt"  # 임시
            utils.save_list_to_file_append(NaN_exists_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False

        # 무결성 검사 3. 시간적 일관성 확인
        # 거래일 목록 ref 읽어오기
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
                df_b_day_ref = pd.concat([df_b_day_ref, pd.DataFrame({'Date': [next_date]})], ignore_index=True)

        path = f"{self.path_data}\\{datemanage.workday_str}\\time_unconsistency_list.txt"
        #path = f"{self.path_data}\\{listed_status}\\{datemanage.workday_str}_merged\\time_unconsistency_list.txt"
        ## ref 와 지금 받아온 df_data의 date 비교
        # ref와 df_data의 첫번째 날짜가 같은지 확인
        if df_b_day_ref['date'].iloc[0] != df_data['date'].iloc[0]:
            different_start_date_list = [f'{code}, 시작 날짜가 ref_date와 다름']
            self.logger.info(different_start_date_list)
            utils.save_list_to_file_append(different_start_date_list, path)  # 텍스트 파일에 오류 부분 저장
            no_error = False
        # df_data의 마지막 날짜가 df_b_day_ref의 마지막 날짜보다 더 늦는 경우
        if type(df_b_day_ref['date'].iloc[-1]) != type(df_data['date'].iloc[-1]):
            print('type 다름')

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

        return no_error
