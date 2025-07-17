import configparser
from UseIntelliquant import UseIntelliquant
import pandas as pd
import re

class GetCombinedData(UseIntelliquant): # 인텔리퀀트에서 ohlcv, num of share 가져오기
    def __init__(self, logger, paths, num_process, datemanage, flag_mod=False):
        super().__init__(logger, paths, num_process, datemanage, flag_mod)

        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_unit_year = 33 # 합치기 전 ohlcv, volume 에서 사용하던 수.
        self.max_unit_year = 16
        self.path_base_code = self.cur_dir + '\\' + 'GetCombinedData_Intelliquant_base.js'
        self.suffix = 'combined_ohlcv' # 파일 이름 저장시 사용하는 접미사
        self.path_backtest_save = paths.OHLCV_Combined

        # 테이블 생성 쿼리. volume 은 저장하지 않음. volume 데이터가 따로 있으니까
        self.create_table_query = f'''
           CREATE TABLE IF NOT EXISTS {self.suffix} (
               stock_code TEXT,
               date TEXT,
               open REAL,
               high REAL,
               low REAL,
               close REAL,            
               volume INTEGER,
               vf INTEGER,
               vi INTEGER,
               vr INTEGER,
               cap INTEGER,
               share INTEGER,
               PRIMARY KEY (stock_code, date)
           );
           '''

    def load_config(self):
        super().load_config()
        #self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetCombinedData_Intelliquant.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page_0']
        self.name = config['intelliquant']['name_0']

    def process_backtest_result(self, path_file):  # backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        # ▣ 날짜 + 종목코드(4가지 케이스) + 쉼표 를 한 번에 잡는 패턴
        data_pattern = (
            r"\["  # 여는 대괄호 [
            r"\d{4}-\d{2}-\d{2}"  # YYYY-MM-DD
            r"\]\s"  # 닫는 대괄호 ] + 공백
            r"(?:"  # ▼ 종목코드 4가지 형태
            r"\d{6}"  # ① 숫자 6개        (예: 005930)
            r"|"  # ────────
            r"\d{5}[A-Za-z]"  # ② 숫자 5개 + 알파  (예: 12345A)
            r"|"  # ────────
            r"\d{4}[A-Za-z]\d"  # ③ 숫자 4 + 알파 + 숫자 (예: 0030R0)
            r"|"  # ────────
            r"\d{4}[A-Za-z]{2}"  # ④ 숫자 4 + 알파 2   (예: 0030RT)
            r")"
            r","  # 끝의 쉼표
        )
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'

        # code_pattern = r'\] (\d{5}[A-Za-z]?|\d{6}),'
        # 날짜 뒤에 나오는 종목코드 4가지 형태(6·5+1·4+1+1·4+2)를 매칭
        code_pattern = (
            r"\]\s+"  # ']' 다음 한 개 이상 공백
            r"("
            r"\d{6}"
            r"|"
            r"\d{5}[A-Za-z]"
            r"|"
            r"\d{4}[A-Za-z]\d"
            r"|"
            r"\d{4}[A-Za-z]{2}"
            r")"
            r","  # ← 쉼표까지 반드시 포함
        )

        open_pattern = r'O: (\d+(\.\d+)?),'
        high_pattern = r'H: (\d+(\.\d+)?),'
        low_pattern = r'L: (\d+(\.\d+)?),'
        close_pattern = r'C: (\d+(\.\d+)?),'
        volume_pattern = r'V: (\d+),'
        volume_forn_pattern = r'F: (-?\d+)'
        volume_inst_pattern = r'I: (-?\d+)'
        volume_retail_pattern = r'R: (-?\d+)'
        cap_pattern = r'cap: (\d+)'
        share_pattern = r'sh: (\d+)'

        num_codes = 0
        num_stocks = 0
        num_load_failure_stocks = 0
        num_delisting_data_error_stocks = 0

        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                if re.search(data_pattern, line):  # 일반 데이터 처리
                    date = re.search(date_pattern, line).group(1)
                    code = re.search(code_pattern, line).group(1)
                    open = re.search(open_pattern, line).group(1)
                    high = re.search(high_pattern, line).group(1)
                    low = re.search(low_pattern, line).group(1)
                    close = re.search(close_pattern, line).group(1)
                    volume = re.search(volume_pattern, line).group(1)
                    volume_forn = re.search(volume_forn_pattern, line).group(1)
                    volume_inst = re.search(volume_inst_pattern, line).group(1)
                    volume_retail = re.search(volume_retail_pattern, line).group(1)
                    cap = re.search(cap_pattern, line).group(1)
                    share = re.search(share_pattern, line).group(1)

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    # volume 은 get volume 에서도 구하지만 verify ohlcv 할 때 거래 정지 여부를 판단하는데 필요해서 넣는다.
                    data_by_code[code].append((date, open, high, low, close, volume, volume_forn, volume_inst, volume_retail, cap, share))

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
            print(path_file,
                  ': backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info(
                'Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d' % (
                    path_file, num_codes, num_stocks, num_load_failure_stocks, num_delisting_data_error_stocks))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume',  'vf', 'vi', 'vr', 'cap', 'share'])

            # 날짜순으로 정렬
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df.sort_values('date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes
