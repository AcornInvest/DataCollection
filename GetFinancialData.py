import os
import logging
from LogStringHandler import LogStringHandler
from Intelliquant import Intelliquant
from DateManage import DateManage
from datetime import date
from datetime import datetime
import configparser
from UseIntelliquant import UseIntelliquant
import json
import math
import pandas as pd
import re
import sqlite3
import utils
import sys

class GetFinancialData(UseIntelliquant):
    def __init__(self, logger, paths, num_process, datemanage):
        super().__init__(logger, paths, num_process, datemanage)
        # 인텔리퀀트 시뮬레이션 종목수 조회시 한번에 돌리는 종목 수.
        #self.max_unit_year = 1500  # 한 종목, 1년을 시뮬레이션할 때가 1 유닛. 100유닛만큼 끊어서 시뮬레이션 하겠다는 의미. 특성 4가지 할 때의 값
        self.max_unit_year = 500 # 특성 12가지일 때.
        self.path_base_code = self.cur_dir + '\\' + 'get_financials_base.js'
        self.suffix = 'financial'  # 파일 이름 저장시 사용하는 접미사
        self.date_prefix = 'financial_day_ref'  # date reference 파일의 접미사

        #self.path_data = config['path']['path_data']
        self.path_backtest_save = paths.Financial

        # financial date용 거래일 목록 ref 읽어오기
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        self.df_business_days = pd.read_excel(path_date_ref)
        self.df_business_days['date'] = pd.to_datetime(self.df_business_days['date']).dt.date
        self.df_business_days = self.df_business_days[(self.df_business_days['date'] >= datemanage.startday) & (self.df_business_days['date'] <= datemanage.workday)]

        # 테이블 생성 쿼리.
        self.create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.suffix} (
            stock_code TEXT,
            date TEXT,
            rv REAL,
            gp REAL,
            oi REAL,
            np REAL,            
            ev_evitda REAL,
            per REAL,
            pbr REAL,            
            psr REAL,
            pcr REAL,
            gpa REAL,
            roa REAL,
            roe REAL,
            PRIMARY KEY (stock_code, date)                  
        );
        '''

        # 코드리스트 읽어오기
        codelist_path = f'{self.path_codeLists}\\Listed\\Listed_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist_listed = pd.read_excel(codelist_path, index_col=0)
        df_codelist_listed['Code'] = df_codelist_listed['Code'].astype(str)
        df_codelist_listed['Code'] = df_codelist_listed['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist_listed['ListingDate'] = pd.to_datetime(df_codelist_listed['ListingDate']).dt.date
        df_codelist_listed['DelistingDate'] = pd.to_datetime(df_codelist_listed['DelistingDate']).dt.date

        codelist_path = f'{self.path_codeLists}\\Delisted\\Delisted_Ticker_{datemanage.workday_str}_modified.xlsx'
        df_codelist_delisted = pd.read_excel(codelist_path, index_col=0)
        df_codelist_delisted['Code'] = df_codelist_delisted['Code'].astype(str)
        df_codelist_delisted['Code'] = df_codelist_delisted['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
        df_codelist_delisted['ListingDate'] = pd.to_datetime(df_codelist_delisted['ListingDate']).dt.date
        df_codelist_delisted['DelistingDate'] = pd.to_datetime(df_codelist_delisted['DelistingDate']).dt.date

        df_codelist = pd.concat([df_codelist_listed, df_codelist_delisted], ignore_index=True)

        # 걸러낼 code 들의 set 만들기
        df_codelist_filtered = df_codelist[
            ~(
            (df_codelist['DelistingDate'] >= datemanage.startday) &  # 상폐가 startday 이후
            (df_codelist['ListingDate'] <= datemanage.workday) &  # 상장이 workday 이전
            (df_codelist['DelistingDate'] >= self.df_business_days['date'].iloc[0]) &  # 상폐가 첫번째 business day 이후
            (df_codelist['ListingDate'] <= self.df_business_days['date'].iloc[-1])  # 상장이 마지막 business day 이전
            )
            ]
        self.codes_exclusion = set(df_codelist_filtered['Code'])  # Ticker 파일에서 가져온 Code column

    def load_config(self):
        super().load_config()

        # self.cur_dir = os.getcwd() # 부모 클래스에서 선언됨
        path = self.cur_dir + '\\' + 'config_GetFinancialData.ini'
        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')
        self.page = config['intelliquant']['page']
        self.name = config['intelliquant']['name']

    def process_backtest_result(self, path_file): #backtest result 를 처리하여 df로 반환
        # 각 코드별 데이터를 저장할 딕셔너리
        data_by_code = {}

        # financial backtest 일반 데이터 패턴: 숫자가 5개 또는 6개 연속으로 있고, 그 뒤에 옵셔널하게 알파벳 문자가 1개 있는 것
        data_pattern = r'\[\d{4}-\d{2}-\d{2}\]\s\d{5,6}[A-Za-z]?,'
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'
        code_pattern = r'\] (\d{5}[A-Za-z]?|\d{6}),'
        rv_pattern = r'rv: (-?\d+),'
        gp_pattern = r'gp: (-?\d+),'
        oi_pattern = r'oi: (-?\d+),'
        np_pattern = r'np: (-?\d+),'
        ev_evitda_pattern = r'ev_evitda: (-?\d+(\.\d+)?),'
        per_pattern = r'per: (-?\d+(\.\d+)?),'
        pbr_pattern = r'pbr: (-?\d+(\.\d+)?),'
        psr_pattern = r'psr: (-?\d+(\.\d+)?),'
        pcr_pattern = r'pcr: (-?\d+(\.\d+)?),'
        gpa_pattern = r'gpa: (-?\d+(\.\d+)?),'
        roa_pattern = r'roa: (-?\d+(\.\d+)?),'
        roe_pattern = r'roe: (-?\d+(\.\d+)?)'

        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                if 'list_index:' in line:
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
                elif re.search(data_pattern, line): # 일반 데이터 처리
                    date = re.search(date_pattern, line).group(1)
                    code = re.search(code_pattern, line).group(1)
                    rv = re.search(rv_pattern, line).group(1)
                    gp = re.search(gp_pattern, line).group(1)
                    oi = re.search(oi_pattern, line).group(1)
                    np = re.search(np_pattern, line).group(1)
                    ev_evitda = re.search(ev_evitda_pattern, line).group(1)
                    per = re.search(per_pattern, line).group(1)
                    pbr = re.search(pbr_pattern, line).group(1)
                    psr = re.search(psr_pattern, line).group(1)
                    pcr = re.search(pcr_pattern, line).group(1)
                    gpa = re.search(gpa_pattern, line).group(1)
                    roa = re.search(roa_pattern, line).group(1)
                    roe = re.search(roe_pattern, line).group(1)

                    # 코드에 따라 데이터 묶기
                    if code not in data_by_code:
                        data_by_code[code] = []
                    data_by_code[code].append((date, rv, gp, oi, np, ev_evitda, per, pbr, psr, pcr, gpa, roa, roe))

        if num_codes != (num_stocks + num_load_failure_stocks + num_delisting_data_error_stocks):
            print('backtest 결과 이상. num_code != num_stock + num_load_failure_stocks + num_delisting_data_error_stocks')
            self.logger.info("Backtest 결과 이상: %s, num_code = %d, num_stocks = %d, num_load_failure_stocks = %d, num_delisting_data_error_stocks = %d" % (path_file,num_codes,num_stocks,num_load_failure_stocks,num_delisting_data_error_stocks ))

        # 각 코드별로 DataFrame 객체 생성
        dataframes = {}
        for code, data in data_by_code.items():
            df = pd.DataFrame(data, columns=['date', 'rv', 'gp', 'oi', 'np', 'ev_evitda', 'per', 'pbr', 'psr', 'pcr', 'gpa', 'roa', 'roe'])

            # 재무 정보가 1개도 없는 종목 골라내기
            # 상장일이 마지막 financial data update 날보다 뒤인 경우, 상폐일이 처음 financial ref day보다 빠를 때
            # 해당 코드의 financial data 가 1행 밖에 없으며 그 날짜가 financial ref date가 아닌 경우를 찾음

            date_obj = datetime.strptime(df['date'].iloc[0], "%Y-%m-%d").date() # 첫번째 행의 데이터가 ref day인지 확인
            if date_obj not in set(self.df_business_days['date']):
                if len(df) == 1: # 한줄만 있는 경우 재무 정보 없는 경우로 봄
                    continue
                else: # 여러줄 있는 경우: 날짜를 최근의 ref day로 변경함
                    business_dates = self.df_business_days['date']  # Series
                    mask_past = business_dates < date_obj  # Boolean mask

                    if mask_past.any():  # 과거 영업일 존재
                        closest_prev_business_day = business_dates[mask_past].max()
                    else:  # 존재하지 않으면 None
                        closest_prev_business_day = None

                    if closest_prev_business_day is not None:
                        df.at[df.index[0], 'date'] = closest_prev_business_day

            # 날짜순으로 정렬
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df.sort_values('date', inplace=True)
            # Reset index
            df.reset_index(drop=True, inplace=True)
            dataframes[code] = df

        return dataframes

    ''' # 2024.3.31 과거에 사용하던 코드. 이제는 UseIntelliquant 로 통합함
    def make_sql(self, datemanage):
        # 처리된 excel 데이터를 모아서 SQL로 만듦.
        # 2024.7.24 일회성 작업에 필요. 앞으로는 intelliquant backtest results를 바로 sql로 저장함
        category = ['Delisted', 'Listed']

        # 테이블 생성 쿼리
        create_table_query = '''
#        CREATE TABLE IF NOT EXISTS financial (
#            stock_code TEXT,
#            date TEXT,
#            rv REAL,
#            gp REAL,
#            oi REAL,
#            np REAL,
#            ev_evitda REAL,
#            per REAL,
#            pbr REAL,
#            psr REAL,
#            pcr REAL,
#            gpa REAL,
#            roa REAL,
#            roe REAL
#        );
'''

        # sql 데이터 저장할 폴더가 존재하지 않으면 생성
        #savedata_folder = f'{self.path_savedata}\\{listed_status}\\{datemanage.workday_str}\\'
        savedata_folder = f'{self.path_savedata}\\{datemanage.workday_str}\\'
        if not os.path.exists(savedata_folder):
            os.makedirs(savedata_folder)

        # SQLite 데이터베이스 파일 연결 (없으면 새로 생성)
        #filename_db = f'{self.suffix}_{listed_status}_{datemanage.workday}.db'
        filename_db = f'{self.suffix}_{datemanage.workday}.db'
        file_path_db = savedata_folder + filename_db
        conn = sqlite3.connect(file_path_db)
        conn.execute(create_table_query)  # 테이블 생성
        conn.commit()

        def check_files(codes, folder, suffix):
            file_names = utils.find_files_with_keyword(folder, suffix)  # 데이터 처리 결과 파일 목록. 특정 suffix가 포함된 파일만 골라냄
            file_prefixes = set([name[:6] for name in file_names])  # 각 파일명의 처음 6글자 추출

            if file_prefixes != codes or len(codes) != len(codes):
                missing_in_files = codes - file_prefixes  # codelist에는 있는데 파일 이름에 없는 값 목록
                extra_in_files = file_prefixes - codes  # 파일 이름에는 있는데 codelist에 없는 값 목록
                # 결과를 텍스트 파일로 저장
                missing_in_files_path = savedata_folder + 'missing_in_files_' + suffix + '_' + listed_status + '_' + datemanage.workday_str + '.txt'
                with open(missing_in_files_path, 'w') as f:
                    for item in missing_in_files:
                        f.write("%s\n" % item)

                extra_in_files_path = savedata_folder + 'extra_in_files_' + suffix + '_' + listed_status + '_' + datemanage.workday_str + '.txt'
                with open(extra_in_files_path, 'w') as f:
                    for item in extra_in_files:
                        f.write("%s\n" % item)

                print(
                    f'{suffix} 데이터 에러. 결과가 {listed_status}_{suffix}_missing_in_files.txt와 {listed_status}_{suffix}_extra_in_files.txt에 저장되었습니다.')
                sys.exit(1)
            else:
                print(f'모든 codelist의 목록이 {listed_status}_{suffix} 데이터에 포함되어 있습니다.')

        for listed_status in category:
            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            codes = set(codelist['Code'])  # Ticker 파일에서 가져온 Code column

            # OHLCV 확인
            #files_path = self.path_data + f'\\OHLCV\\Intelliquant\\{listed_status}\\{datemanage.workday_str}_merged'
            folder_financial = os.path.join(self.path_data, 'Financial', listed_status, datemanage.workday_str) # 엑셀 파일 폴더 경로
            suffix_financial = 'financial'
            check_files(codes, folder_financial, suffix_financial)

            for code in codes:
                file_path_financial = f'{folder_financial}\\{code}_{suffix_financial}_{datemanage.workday_str}.xlsx'
                df_financial = pd.read_excel(file_path_financial, index_col=None)
                df_financial['stock_code'] = code  # 종목코드 열 추가
                df_financial.to_sql('financial', conn, if_exists='append', index=False)

        # 인덱스 생성 (쿼리 성능 향상)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON financial (stock_code);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON financial (date);')
        conn.commit()
        # 데이터베이스 연결 종료
        conn.close()
        '''