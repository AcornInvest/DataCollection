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
        self.LoadConfig()

    def LoadConfig(self):
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

    def IntelliquantOn(self, page, name): # 크롬 켜고 전략 페이지 열기
        pass

    def UpdateCode(self, js_code): # 이건 자식 클래스에서 할까..아니면 intelliquant class 에서 할까? 그게 나을 것 같기도..
        # 이미 인텔리퀀트 열어서 해당 전략 페이지가 열어진 상태임을 가정함.
        textedit 선택 후, ctrl+A, delete 후 js_code 값 입력
        저장 버튼, 확인 버튼 누르기
        pass




