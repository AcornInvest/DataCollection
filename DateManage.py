import sys
import os
import time
import logging
from datetime import datetime
from datetime import date
from dateutil.relativedelta import *
import configparser
import pandas as pd

class DateManage:
    '''
    오늘 및 구동과 관련된 날짜를 얻고, 이를 이용하여 각종 파일 이름 string을 생성, 관리하는 클래스
    '''
    def __init__(self, filename, paths):
        # day Strings
        self.today = date.today()
        self.today_string = self.today.strftime('%Y-%m-%d') # 로그파일명에 쓸 것

        # 경로
        self.cur_dir = os.getcwd()
        self.path_date_ref = paths.date_ref
        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사
        self.date_financial_prefix = 'financial_day_ref'  # financial date reference 파일의 접미사

        # 로그 파일 - 오늘
        self.file_log = '\\' + self.today_string + '_' + filename + '_log_info.log'
        self.path_log = self.cur_dir + r'\log' + self.file_log

    def SetFormerday(self, formerday: datetime):  # 수집하는 데이터의 첫번째 날짜. 작업 시작일의 데이터가 더 과거 데이터를 필요로 할 때
        self.formerday = formerday
        self.formerday_str = self.formerday.strftime('%Y-%m-%d')

    def SetStartday(self, startday: datetime):  # 작업시작일 --> 수집하는 데이터의 첫번째 날짜
        self.startday = startday
        self.startday_str = self.startday.strftime('%Y-%m-%d')

    #def SetWorkday(self, workday:date): # 작업기준일 --> 수집하는 데이터의 마지막 날짜
    def SetWorkday(self, workday: datetime):  # 작업기준일 --> 수집하는 데이터의 마지막 날짜
        self.workday = workday
        self.workday_str = self.workday.strftime('%Y-%m-%d')

    def set_ref_days(self):
        # Reference days
        path_date_ref = f"{self.path_date_ref}\\{self.date_financial_prefix}_{self.workday_str}.xlsx"
        self.df_financial_ref_days = pd.read_excel(path_date_ref)
        path_date_ref = f"{self.path_date_ref}\\{self.date_prefix}_{self.workday_str}.xlsx"
        self.df_ref_days = pd.read_excel(path_date_ref)

    def calculate_past_b_day(self, delta_days: int, base_date: datetime, flag_financial=False):
        """
        base_date가 거래일(date_ref)에 존재하면 그 행을,
        없으면 base_date보다 ‘더 미래’인 가장 이른 거래일을 기준으로 한 뒤
        delta_days만큼 이전 거래일을 반환한다.

        Parameters
        ----------
        delta_days : int
            며칠 이전의 거래일을 찾을지 지정 (음수도 허용).
        base_date : datetime
            기준이 되는 날짜. 중복은 없다고 가정.

        Returns
        -------
        datetime.date
            기준일(혹은 가장 가까운 미래 거래일)로부터 delta_days 거래일 전의 날짜

        Raises
        ------
        ValueError
            • base_date가 마지막 거래일보다 미래이면 오류
            • delta_days가 범위를 초과할 때
            범위 초과 시 마지막 행으로 보정
        """
        # 1) 거래일 목록 로드 및 기간 필터링
        if flag_financial:
            df_business_days = self.df_financial_ref_days.copy()
        else:
            df_business_days = self.df_ref_days.copy()

        df_business_days["date"] = pd.to_datetime(df_business_days["date"])

        date_ref = (
            df_business_days[
               # (df_business_days["date"] >= self.formerday) &
                (df_business_days["date"] <= self.workday)
                ].sort_values("date")
            .reset_index(drop=True)
        )

        # 2) base_date 위치 결정
        date_series = date_ref["date"]

        # searchsorted: base_date가 정확히 있으면 해당 인덱스,
        # 없으면 삽입 위치(즉, base_date보다 큰 첫 거래일) 반환
        idx_base = date_series.searchsorted(base_date, side="left")

        # base_date가 마지막 거래일보다 미래이면 오류
        if idx_base >= len(date_series):
            raise ValueError(
                f"base_date({base_date.date()}) 이후의 거래일이 없습니다."
            )

        # 3) delta_days만큼 이전 인덱스 계산
        idx_target = idx_base - delta_days
        if idx_target < 0:
            raise ValueError(
                f"delta_days({delta_days})가 범위를 초과했습니다. "
                f"가용 최대 이전 거래일 수는 {idx_base}일입니다."
            )

        # idx_target 이 마지막 df_business_days 를 넘어가는 경우.
        # load processed data 할 때 base_date 에서 unused_period 만큼 더하면 아예 쓸 수 있는 값이 없는 경우.
        if idx_target > len(date_series) - 1:
            return self.workday

        # 4) 원하는 과거 거래일 반환
        return date_series.iloc[idx_target]



