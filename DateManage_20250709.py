import sys
import os
import time
import logging
from datetime import datetime
from datetime import date
from dateutil.relativedelta import *
import configparser

class DateManage:
    '''
    오늘 및 구동과 관련된 날짜를 얻고, 이를 이용하여 각종 파일 이름 string을 생성, 관리하는 클래스
    '''
    def __init__(self, filename):
        # 설정 로드
        self.LoadConfig()

        # day Strings
        self.today = date.today()
        self.today_string = self.today.strftime('%Y-%m-%d') # 로그파일명에 쓸 것

        # 로그 파일 - 오늘
        self.file_log = '\\' + self.today_string + '_' + filename + '_log_info.log'
        self.path_log = self.cur_dir + r'\log' + self.file_log

    def LoadConfig(self): # 설정 불러오기
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_DateManage.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_data = config['path']['path_data']

    #def SetStartday(self, startday:date): # 작업시작일 --> 수집하는 데이터의 첫번째 날짜
    def SetStartday(self, startday: datetime):  # 작업시작일 --> 수집하는 데이터의 첫번째 날짜
        self.startday = startday
        self.startday_str = self.startday.strftime('%Y-%m-%d')

    #def SetWorkday(self, workday:date): # 작업기준일 --> 수집하는 데이터의 마지막 날짜
    def SetWorkday(self, workday: datetime):  # 작업기준일 --> 수집하는 데이터의 마지막 날짜
        self.workday = workday
        self.workday_str = self.workday.strftime('%Y-%m-%d')


