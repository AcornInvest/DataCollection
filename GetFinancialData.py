import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import datetime
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd
from pandas import Timedelta
import re

class GetFinancialData(UseIntelliquant):
    def __init__(self, logger):
        super().__init__(logger)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_batchsize = 20 # Delisted
        self.max_batchsize = 10 # Listed
        self.max_unit_year = 100 # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 100유닛만큼 끊어서 시뮬레이션 하겠다는 의미
        self.path_base_code = self.cur_dir + '\\' + 'get_financials_base.js'

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetFinancialData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_backtest_save = config['path']['path_backtest_save']

    def calculate_batch_indices(self, length_code_list, max_batchsize, listingdate_content, delistingdate_content, startday, workday): #run_backtest_rep() 에서 한번에 시뮬레이션 할 리스트 만들어서 리턴
        listingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", listingdate_content)
        delistingdate_list = re.findall(r"new Date\('(\d{4}-\d{2}-\d{2})'\)", delistingdate_content)
        listingdate_list_timestamp = pd.to_datetime(listingdate_list)
        delistingdate_list_timestamp = pd.to_datetime(delistingdate_list)
        adjusted_listingdate_list_timestamp = [ts if ts >= startday else startday for ts in listingdate_list_timestamp]
        adjusted_delistingdate_list_timestamp = [ts if ts <= workday else workday for ts in delistingdate_list_timestamp]

        unit_year_list = []
        td_year = Timedelta(days=365)
        for listingdate, delistingdate in zip(adjusted_listingdate_list_timestamp, adjusted_delistingdate_list_timestamp):
            unit_year = (delistingdate - listingdate)/td_year
            unit_year_list.append(unit_year)

        indices = []
        sum_unit_year = 0
        start_index = 0
        end_index = 0
        for index, unit_year in enumerate(unit_year_list):
            if(sum_unit_year + unit_year > self.max_unit_year):
                end_index = index - 1
                indices.append((start_index, end_index))
                start_index = index
                sum_unit_year=0
            sum_unit_year += unit_year

        if sum_unit_year > 0:
            indices.append((start_index, len(unit_year_list) - 1))

        # 인텔리퀀트 start date, end date도 계산해서 리턴할까? 근데 그건 처음에 전체 데이터 읽어올 때만 좀 유용하고 업데이트할 때는 큰 의미가 없다.
        return indices