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
        js_code_base = self.load_base_code(self.path_base_code)
        js_code_dataset = self.load_dataset_code(datemanage, listed, file_index, start_num, end_num)
        #print(js_code_dataset)
        js_code = js_code_dataset + js_code_base
        #print(js_code)
        return js_code

    def create_js_code_dataset(self, startday, workday, code, listingdate, delistingdate):
        # 상장일, 상폐일, 코드 리스트 받아서 데이터셋 코드 형식 str 리턴
        return (
            f"//Dataset Begin\n"
            f"var StartDate = new Date('{startday}');\n"
            f"var FinalDate = new Date('{workday}');\n\n"
            f"var code = [\n{code}\n];\n"
            f"var ListingDate = [\n{listingdate}\n];\n"
            f"var DelistingDate = [\n{delistingdate}\n];\n"
            f"//Dataset End\n\n"
        )

    def parse_data_to_list(self, data_str): #format_string_data() 에서 사용.
        # 줄바꿈과 콤마를 기준으로 데이터를 리스트로 변환
        return [item.strip() for item in data_str.replace('\n', '').split(',')]

    def format_string_data(self, data_str, start, end, line_length=5): # load_dataset_code 시 한 파일 내의 데이터 중 일부를 추출
        # 데이터 리스트로 변환
        data_list = self.parse_data_to_list(data_str)

        # 추출된 데이터 범위
        extracted_data = data_list[start:end + 1]

        # 5개씩 끊어서 새로운 문자열로 변환
        formatted_data = ''
        for i in range(0, len(extracted_data), line_length):
            line_data = ','.join(extracted_data[i:i + line_length])
            if i + line_length < len(extracted_data):
                line_data += ','
            formatted_data += line_data + '\n'

        # 마지막 줄의 불필요한 줄바꿈 제거
        return formatted_data.rstrip()

    def load_dataset_code(self, datemanage: DateManage, listed: str, file_index: int, start_num: int,
                          end_num: int):  # index에 해당하는 code, listing date, delisting date 합성하여 str 리턴
        # start_num: 각 txt 파일의 배열 중 참조를 시작하는 index
        # end_num: 각 txt 파일의 배열 중 마지막으로 참조하는 index
        # 각 txt 파일마다 20개씩 배열이 있으므로 기본값은 start_num=0, end_num=19

        '''
        self.path_dir = self.path_codeLists + '\\' + listed + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'
        self.path_code = self.path_dir + 'A_Code_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        self.path_listingdate = self.path_dir + 'A_ListingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        self.path_delistingdate = self.path_dir + 'A_DelistingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        '''

        with open(self.path_code, 'r') as file:
            code_content = file.read()
        # print(code_content)
        with open(self.path_listingdate, 'r') as file:
            listingdate_content = file.read()
        # print(listingdate_content)
        with open(self.path_delistingdate, 'r') as file:
            delistingdate_content = file.read()
        # print(delistingdate_content)

        format_code_content = self.format_string_data(code_content, start_num, end_num)
        format_listingdate_content = self.format_string_data(listingdate_content, start_num, end_num)
        format_delistingdate_content = self.format_string_data(delistingdate_content, start_num, end_num)

        js_code_dataset = self.create_js_code_dataset(datemanage.startday_str, datemanage.workday_str,
                                                      format_code_content, format_listingdate_content,
                                                      format_delistingdate_content)
        return js_code_dataset



