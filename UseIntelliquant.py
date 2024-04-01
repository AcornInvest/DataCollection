import os
from Intelliquant import Intelliquant
from DateManage import DateManage
import configparser

class UseIntelliquant:
    '''
   인텔리퀀트에서 데이터 가져오는 기능
   '''

    def __init__(self, logger):
        self.intel = Intelliquant()
        self.logger = logger

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

    def load_base_code(self, path_base_code):
        with open(path_base_code, 'r', encoding='utf-8') as file:
            js_code_base = file.read()
        #print(js_code_base)
        return js_code_base

    def create_js_code_dataset(self, startday, workday, code, listingdate, delistingdate, start_num, end_num):
        # 상장일, 상폐일, 코드 리스트 받아서 데이터셋 코드 형식 str 리턴
        format_code_content = self.format_string_data(code, start_num, end_num)
        format_listingdate_content = self.format_string_data(listingdate, start_num, end_num)
        format_delistingdate_content = self.format_string_data(delistingdate, start_num, end_num)

        return (
            f"//Dataset Begin\n"
            f"var StartDate = new Date('{startday}');\n"
            f"var FinalDate = new Date('{workday}');\n\n"
            f"var code = [\n{format_code_content}\n];\n"
            f"var ListingDate = [\n{format_listingdate_content}\n];\n"
            f"var DelistingDate = [\n{format_delistingdate_content}\n];\n"
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

    def load_dataset_code(self, datemanage: DateManage, file_index: int):
        # index에 해당하는 code, listing date, delisting date 값 불러와서 리턴
        path_code = self.path_for_intelliquant_dir + 'A_Code_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_listingdate = self.path_for_intelliquant_dir + 'A_ListingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'
        path_delistingdate = self.path_for_intelliquant_dir + 'A_DelistingDate_' + datemanage.workday_str + '_' + str(file_index) + '.txt'


        with open(path_code, 'r') as file:
            code_content = file.read()
            items = code_content.split(',')
            length_code_list = len(items)
        # print(code_content)
        with open(path_listingdate, 'r') as file:
            listingdate_content = file.read()
            items = listingdate_content.split(',')
            length_listingdate_list = len(items)
        # print(listingdate_content)
        with open(path_delistingdate, 'r') as file:
            delistingdate_content = file.read()
            items = delistingdate_content.split(',')
            length_delistingdate_list = len(items)
        # print(delistingdate_content)

        if (length_code_list == length_listingdate_list == length_delistingdate_list):
            print("length_code_list: ", length_code_list)
        else:
            raise ValueError(datemanage.workday_str, "_", file_index, "의 파일간 데이터 리스트 수가 다름")

        return length_code_list, code_content, listingdate_content, delistingdate_content

    def calculate_batch_indices(self, length_code_list, batchsize):
        indices = []
        for start in range(0, length_code_list, batchsize):
            end = min(start + batchsize - 1, length_code_list - 1)
            indices.append((start, end))
        return indices

    def run_backtest_rep(self, datemanage):
        # 데이터를 하나씩 추출해서 인텔리퀀트의 코드 수정해가면서 백테스트 수행.
        #chrome_on()은 되어 있는 상태에서 호출

        #category = ['Delisted', 'Listed']
        #category = ['Delisted']
        category = ['Listed']
        for type in category:
            self.path_for_intelliquant_dir = self.path_codeLists + '\\' + type + '\\For_Intelliquant\\' + datemanage.workday_str + '\\'
            # max_file_index(폴더 내 데이터 파일 수) 계산
            # 파일 무리별 카운터 초기화
            count_code = 0
            count_delisting = 0
            count_listing = 0

            # 폴더 내의 모든 파일에 대해 반복
            for filename in os.listdir(self.path_for_intelliquant_dir):
                if filename.startswith('A_Code_') and datemanage.workday_str in filename:
                    count_code += 1
                elif filename.startswith('A_DelistingDate_') and datemanage.workday_str in filename:
                    count_delisting += 1
                elif filename.startswith('A_ListingDate_') and datemanage.workday_str in filename:
                    count_listing += 1

            # 카운터 값이 모두 같은지 확인하고, 아니면 에러 발생
            if count_code == count_delisting == count_listing:
                max_file_index = count_code
                print("모든 파일 무리의 개수가 동일합니다. ", max_file_index)
            else:
                raise ValueError("파일 무리의 개수가 서로 다릅니다.")

            for file_index in range(74, 94 + 1):  # 테스트용. 파일 2개만 실행
            #for file_index in range(1, max_file_index+1): #폴더 내의 파일 갯수만큼 반복
                length_code_list, code_content, listingdate_content, delistingdate_content = self.load_dataset_code(datemanage, file_index)

                # 수정할 것
                # 파일에서 listing date의 최소(가장 과거), delisting date 의 최대(가장 최근) 날짜를 보고 startday, workday 및 batchsize 선정하기
                data_indices = self.calculate_batch_indices(length_code_list, self.batchsize)

                #num_data_index = 1
                for idx, k in enumerate(data_indices): # 한 파일 내에서의 인덱스
                    js_code_dataset = self.create_js_code_dataset(datemanage.startday_str, datemanage.workday_str,
                                                      code_content, listingdate_content,delistingdate_content, k[0], k[1])
                    #print(js_code_dataset)
                    js_code_base = self.load_base_code(self.path_base_code)
                    js_code = js_code_dataset + js_code_base
                    self.intel.update_code(js_code) #인텔리퀀트 코드창 수정
                    backtest_list = self.intel.backtest(datemanage.startday_str, datemanage.workday_str, '10000000', self.logger)
                    self.save_backtest_result(backtest_list, type, datemanage, file_index, idx)

    def save_backtest_result(self, backtest_list, type, datemanage, file_index, idx):
        self.path_backtest_result = self.path_compensation + '\\' + type + '\\From_Intelliquant\\' + datemanage.workday_str + '\\' + 'backtest_result_' + datemanage.workday_str + '_' + str(file_index) + '_' + str(idx) + '.txt'
        folder = os.path.dirname(self.path_backtest_result)
        # 폴더가 존재하지 않으면 생성
        if not os.path.exists(folder):
            os.makedirs(folder)

        self.path_load_failure_list = folder + '\\' + 'load_failure_list_' + datemanage.workday_str + '.txt'
        self.path_delisting_date_error_list = folder + '\\' + 'delisting_date_error_list_' + datemanage.workday_str + '.txt'

        # 각 백테스트 결과 파일 txt로 저장
        f = open(self.path_backtest_result, 'w', encoding='utf-8')
        f.writelines(backtest_list)
        f.close()
        self.logger.info("Backtest 결과 저장 완료: %s" % self.path_backtest_result)

        #load_failure_list, delisting_date_error_list 저장
        load_failure_list, delisting_date_error_list = self.parse_list_data(backtest_list)
        if load_failure_list:
            self.save_list_to_file_append(load_failure_list, self.path_load_failure_list)
            self.logger.info("load_failure_list 결과 저장 완료: %s" % self.path_load_failure_list)
        if delisting_date_error_list:
            self.save_list_to_file_append(delisting_date_error_list, self.path_delisting_date_error_list)
            self.logger.info("delisting_date_error_list 결과 저장 완료: %s" % self.path_delisting_date_error_list)

    def parse_list_data(self, lines):
        load_failure_list = []
        delisting_date_error_list = []

        for line in lines:
            if "load_failure_list:" in line:
                extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                load_failure_list = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
            elif "DelistingDate_Error_list:" in line:
                extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                delisting_date_error_list = [x.strip() for x in extracted_data if x.strip()]

        return load_failure_list, delisting_date_error_list

    # 파일에 데이터를 추가하는 함수로 save_list_to_file 수정하기
    def save_list_to_file_append(self, data_list, filename):
        with open(filename, 'a') as file:  # 'a' 모드는 파일에 내용을 추가합니다
            for item in data_list:
                file.write(f"{item}\n")

    # 특정 폴더 내에서 특정 문자열로 시작하는 파일들의 이름을 리스트로 반환하는 함수
    def get_files_starting_with(self, folder_path, start_string):
        files_starting_with = []
        for filename in os.listdir(folder_path):
            if filename.startswith(start_string):
                files_starting_with.append(filename)
        return files_starting_with