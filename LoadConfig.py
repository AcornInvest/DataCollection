import configparser
import os

class LoadConfig:
    def __init__(self):
        self.load_config()

    def load_config(self):
        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_DataCollection.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.StockDataSet = config['path']['StockDataSet']
        self.CodeLists = config['path']['CodeLists']
        self.OHLCV_Combined = config['path']['OHLCV_Combined']
        self.OHLCV_Intelliquant = config['path']['OHLCV_Intelliquant']
        self.OHLCV_Intelliquant_mod = config['path']['OHLCV_Intelliquant_mod']
        self.OHLCV_Compensation = config['path']['OHLCV_Compensation']
        self.OHLCV_volume = config['path']['OHLCV_volume']
        self.Financial = config['path']['Financial']
        self.date_ref = config['path']['date_ref']

