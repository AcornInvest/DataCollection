import os
from Intelliquant import Intelliquant
from DateManage import DateManage
import configparser

class UseIntelliquant:
    '''
   인텔리퀀트에서 데이터 가져오는 기능
   '''

    def __init__(self):
        self.intel = Intelliquant()

        # 설정 로드
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_UseIntelliquant.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_data = config['path']['path_data']
        self.path_codeLists = config['path']['path_codeLists']
        self.path_compensation = config['path']['path_compensation']
        self.path_financial  = config['path']['path_financial']

    '''
    def IntelliquantOn(self, page, name): # 크롬 켜고 전략 페이지 열기
        pass
        # 필요없어 보인다
    '''
    def load_base_code(self, path_base_code):
        with open(path_base_code, 'r', encoding='utf-8') as file:
            js_code_base = file.read()
        #print(js_code_base)
        return js_code_base

    def make_js_code(self, datemanage: DateManage, listed: str, file_index: int, start_num:int, end_num:int):
        path_base_code = self.cur_dir + '\\' + 'GetNoOfShares_base.js'
        js_code_base = self.load_base_code(path_base_code)
        js_code_dataset = self.load_dataset_code(datemanage, listed, file_index, start_num, end_num)
        js_code = js_code_dataset + js_code_base
        #print(js_code)
        return js_code

    def create_js_code_dataset(self, startday, workday, code, listingdate, delistingdate):
        return (
            f"//Dataset Begin\n"
            f"var StartDate = new Date('{startday}');\n"
            f"var FinalDate = new Date('{workday}');\n\n"
            f"var code = [\n{code}\n];\n"
            f"var ListingDate = [\n{listingdate}\n];\n"
            f"var DelistingDate = [\n{delistingdate}\n];\n"
            f"//Dataset End\n\n"
        )




