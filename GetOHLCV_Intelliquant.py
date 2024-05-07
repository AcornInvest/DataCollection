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

class GetOHLCV_Intelliquant(UseIntelliquant): # 인텔리퀀트에서 모든 종목 OHLCV 3일씩
    def __init__(self, logger):
        super().__init__(logger)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        self.max_batchsize = 60 # For_Intelliquant 파일 내의 code 숫자
        self.path_base_code = self.cur_dir + '\\' + 'GetOHLCV_Intelliquant_base.js'
        self.suffix = 'OHLCV_intelliquant'  # 파일 이름 저장시 사용하는 접미사

    def load_config(self):
        super().load_config()

        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetOHLCV_Intelliquant.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_backtest_save = config['path']['path_backtest_save']
        self.path_OHLCV_init = config['path']['path_OHLCV_init']
        self.path_date_ref = config['path']['path_date_ref']

    def calculate_batch_indices(self, length_code_list, max_batchsize, listingdate_content, delistingdate_content, startday, workday): #run_backtest_rep() 에서 한번에 시뮬레이션 할 리스트 만들어서 리턴
        indices = []
        for start in range(0, length_code_list, max_batchsize):
            end = min(start + max_batchsize - 1, length_code_list - 1)
            indices.append((start, end))
        return indices