import os
from Intelliquant import Intelliquant
from DateManage import DateManage
import configparser
import pandas as pd
import utils
from pandas import Timedelta
import re

class GetDateRef:
    '''
   인텔리퀀트에서 빈 코드 돌려서 영업일 정보 가져오는 기능
   '''

    def __init__(self, logger, num_process):
        self.num_process = num_process  # 멀티 프로세스 번호
        self.intel = Intelliquant(self.num_process)
        self.logger = logger

        # 설정 로드
        self.load_config()
        #self.suffix = 'data'  # 파일 이름 저장시 사용하는 접미사

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_GetDateRef.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']
        self.path_data = config['path']['path_data']
        self.path_backtest_save = config['path']['path_backtest_save']

    def run_backtest(self, datemanage):
        # Date ref 를 얻기위한 backtest 1회 수행
        #chrome_on()은 되어 있는 상태에서 호출

        start_date_str = datemanage.startday_str
        end_date_str = datemanage.workday_str
        backtest_list = self.intel.backtest(start_date_str, end_date_str, '10000000', self.logger)
        self.save_backtest_result(self.path_backtest_save, backtest_list, datemanage)

    def save_backtest_result(self, path_backtest_save, backtest_list, datemanage):
        # backtest 파일 저장 경로
        self.path_backtest_result = path_backtest_save + '\\' + 'backtest_result_' + datemanage.workday_str + '.txt'
        folder = os.path.dirname(self.path_backtest_result)
        # 폴더가 존재하지 않으면 생성
        if not os.path.exists(folder):
            os.makedirs(folder)

        # 파일에 저장
        with open(self.path_backtest_result, 'w', encoding='utf-8') as file:
            '''
            for text in backtest_list:
                file.write(text + '\n')
            '''
            file.write('\n'.join(backtest_list) + '\n')
        self.logger.info("Backtest 결과 저장 완료: %s" % self.path_backtest_result)

    def process_backtest_result(self, path_file):  # backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        # OHLCV backtest 일반 데이터 패턴: 숫자가 5개 또는 6개 연속으로 있고, 그 뒤에 옵셔널하게 알파벳 문자가 1개 있는 것
        data_pattern = r'\[\d{4}-\d{2}-\d{2}\]\s\d{5,6}[A-Za-z]?,'
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'
        code_pattern = r'\] (\d{5}[A-Za-z]?|\d{6}),'
        open_pattern = r'O: (\d+(\.\d+)?),'
        high_pattern = r'H: (\d+(\.\d+)?),'
        low_pattern = r'L: (\d+(\.\d+)?),'
        close_pattern = r'C: (\d+(\.\d+)?),'
        volume_pattern = r'V: (\d+),'
        cap_pattern = r'cap: (\d+)'
        num_codes = 0
        num_stocks = 0
        num_load_failure_stocks = 0
        num_delisting_data_error_stocks = 0
        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                if re.search(data_pattern, line):  # 일반 데이터 처리
                    date = re.search(date_pattern, line).group(1)
                    code = re.search(code_pattern, line).group(1)
                    Open = re.search(open_pattern, line).group(1)
                    high = re.search(high_pattern, line).group(1)
                    low = re.search(low_pattern, line).group(1)
                    close = re.search(close_pattern, line).group(1)
                    volume = re.search(volume_pattern, line).group(1)
                    cap = re.search(cap_pattern, line).group(1)

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date, Open, high, low, close, volume, cap))
                elif 'list_index:' in line:
                    num_codes = int(line.split('list_index:')[1].strip())
                elif 'NumOfStocks:' in line:
                    num_stocks = int(line.split('NumOfStocks:')[1].strip())
                elif 'load_failure_list:' in line:
                    extracted_data = line.split("load_failure_list: [")[1].split("]")[0].split(",")
                    load_failure_stocks = [x.strip() for x in extracted_data if x.strip()]  # 비어있지 않은 요소만 추가
                    num_load_failure_stocks = len(load_failure_stocks)
                elif 'DelistingDate_Error_list:' in line:
                    extracted_data = line.split("DelistingDate_Error_list: [")[1].split("]")[0].split(",")
                    delisting_data_error_stocks = [x.strip() for x in extracted_data if x.strip()]
                    num_delisting_data_error_stocks = len(delisting_data_error_stocks)

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print(path_file, ': backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info(
                'Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d' % (
                path_file, num_codes, num_stocks, num_load_failure_stocks, num_delisting_data_error_stocks))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Cap'])
            # 날짜순으로 정렬
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df.sort_values('Date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes
