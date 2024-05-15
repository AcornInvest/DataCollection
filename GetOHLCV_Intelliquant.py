import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import date
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd
from pandas import Timedelta
import re

class GetOHLCV_Intelliquant(UseIntelliquant): # 인텔리퀀트에서 모든 종목 OHLCV 3일씩
    def __init__(self, logger, num_process):
        super().__init__(logger, num_process)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        self.max_batchsize = 60 # For_Intelliquant 파일 내의 code 숫자
        self.max_unit_year = 104  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24.5년 * 4종목 =  98 단위 + 마진 = 102으로 시뮬레이션 하도록 함
        #self.max_unit_year = 125  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24.5년 * 5종목 =  122.5 단위 + 마진 = 125으로 시뮬레이션 하도록 함
        #self.max_unit_year = 300  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 24년 * 12종목 =  288 단위 + 마진 12 = 300으로 시뮬레이션 하도록 함
        self.path_base_code = self.cur_dir + '\\' + 'GetOHLCV_Intelliquant_base.js'
        self.suffix = 'OHLCV_intelliquant'  # 파일 이름 저장시 사용하는 접미사

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetOHLCV_Intelliquant.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page_list = [config['intelliquant']['page_0'], config['intelliquant']['page_1'], config['intelliquant']['page_2'], config['intelliquant']['page_3']]
        self.name_list = [config['intelliquant']['name_0'], config['intelliquant']['name_1'], config['intelliquant']['name_2'], config['intelliquant']['name_3']]
        self.page = self.page_list[self.num_process]
        self.name = self.name_list[self.num_process]
        self.path_backtest_save = config['path']['path_backtest_save']
        self.path_date_ref = config['path']['path_date_ref']
