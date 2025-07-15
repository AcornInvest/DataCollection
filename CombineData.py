import os
from DateManage import DateManage
import configparser
import pandas as pd
import sys
import utils
import sqlite3

# 20250408 에 새로 만듦
# db 파일들을 모아서 combine 함.
# OHLCV 관련 데이터를 모음

class CombineData:
    '''
   OHLCV, NoOfshare, volume 합쳐서 Combined 폴더에 저장하는 기능
   2024.8.8 파일을 sql에 저장하도록 함
   '''

    def __init__(self, logger, paths, datemanage):
        self.logger = logger
        self.paths = paths

        # 설정 로드
        self.load_config()

        self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사

        # 거래일 목록 ref 읽어오기. 기본. 전체 영업일.
        path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
        self.df_business_days = pd.read_excel(path_date_ref)
        #self.df_business_days['date'] = pd.to_datetime(self.df_business_days['date']).dt.date
        self.df_business_days['date'] = pd.to_datetime(self.df_business_days['date'])
        self.df_business_days = self.df_business_days[(self.df_business_days['date'] >= datemanage.startday) & (
                    self.df_business_days['date'] <= datemanage.workday)]

    def load_config(self):
        # 설정
        self.path_data = self.paths.StockDataSet
        self.path_codeLists = self.paths.CodeLists
        self.path_savedata = self.paths.OHLCV_Combined
        self.path_date_ref = self.paths.date_ref
        self.suffix = 'combined_ohlcv'

    def combine_data(self, datemanage): # 각 폴더의 데이터가 codelist의 모든 목록을 포함하는지 확인
        category = ['Delisted', 'Listed']

        # 테이블 생성 쿼리
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS combined_ohlcv (
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

        all_codes = set()
        for listed_status in category:
            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            #codelist['ListingDate'] = pd.to_datetime(codelist['ListingDate']).apply(lambda x: x.date())
            #codelist['DelistingDate'] = pd.to_datetime(codelist['DelistingDate']).apply(lambda x: x.date())
            codelist['ListingDate'] = pd.to_datetime(codelist['ListingDate'])
            codelist['DelistingDate'] = pd.to_datetime(codelist['DelistingDate'])
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기

            # 상폐일이 작업시작일(startday)보다 늦고, 상장일이 작업마지막날(workday)보다 빠른 것만 남기기
            codelist_filtered = codelist[
                (codelist['DelistingDate'] >= datemanage.startday) &  # 상폐가 startday 이후
                (codelist['ListingDate'] <= datemanage.workday) &  # 상장이 workday 이전
                (codelist['ListingDate'] <= self.df_business_days['date'].iloc[-1])  # 상장이 마지막 business day 이전
            ]

            codes = set(codelist_filtered['Code'])  # Ticker 파일에서 가져온 Code column
            all_codes.update(codes)  # 합집합에 추가

        # Combined 데이터 저장할 폴더가 존재하지 않으면 생성
        savedata_folder = f'{self.path_savedata}\\{datemanage.workday_str}\\'
        if not os.path.exists(savedata_folder):
            os.makedirs(savedata_folder)

        '''
        def check_files(codes, folder, suffix):
            file_names = utils.find_files_with_keyword(folder, suffix)  # 데이터 처리 결과 파일 목록. 특정 suffix가 포함된 파일만 골라냄
            file_prefixes = set([name[:6] for name in file_names])  # 각 파일명의 처음 6글자 추출

            if file_prefixes != codes or len(codes) != len(codes):
                missing_in_files = codes - file_prefixes # codelist에는 있는데 파일 이름에 없는 값 목록
                extra_in_files = file_prefixes - codes # 파일 이름에는 있는데 codelist에 없는 값 목록
                # 결과를 텍스트 파일로 저장
                missing_in_files_path = savedata_folder + 'missing_in_files_' + suffix + '_' + listed_status + '_' + datemanage.workday_str + '.txt'
                with open(missing_in_files_path, 'w') as f:
                    for item in missing_in_files:
                        f.write("%s\n" % item)

                extra_in_files_path = savedata_folder + 'extra_in_files_' + suffix + '_' + listed_status + '_' + datemanage.workday_str + '.txt'
                with open(extra_in_files_path, 'w') as f:
                    for item in extra_in_files:
                        f.write("%s\n" % item)

                print(f'{suffix} 데이터 에러. 결과가 {listed_status}_{suffix}_missing_in_files.txt와 {listed_status}_{suffix}_extra_in_files.txt에 저장되었습니다.')
                sys.exit(1)
            else:
                print(f'모든 codelist의 목록이 {listed_status}_{suffix} 데이터에 포함되어 있습니다.')
        '''

        # OHLCV
        folder_OHLCV = os.path.join(self.path_data, 'OHLCV', 'Intelliquant', f'{datemanage.workday_str}') # 폴더 경로
        suffix_OHLCV = 'OHLCV_intelliquant'
        file_OHLCV = f'{suffix_OHLCV}_{datemanage.workday_str}.db'
        path_OHLCV = folder_OHLCV + '\\' + file_OHLCV

        #check_files(codes, folder_OHLCV, suffix_OHLCV)

        # volume
        folder_volume = os.path.join(self.path_data, 'OHLCV', 'volume', f'{datemanage.workday_str}') # 폴더 경로
        suffix_volume = 'volume'
        file_volume = f'{suffix_volume}_{datemanage.workday_str}.db'
        path_volume = folder_volume + '\\' + file_volume

        # NoOfShare
        folder_compensation = os.path.join(self.path_data, 'OHLCV', 'Compensation', f'{datemanage.workday_str}') # 폴더 경로
        suffix_compensation = 'compensation'
        file_compensation = f'{suffix_compensation}_{datemanage.workday_str}.db'
        path_compensation = folder_compensation + '\\' + file_compensation

        db_table_pairs = [
            (path_OHLCV, suffix_OHLCV),
            (path_volume, suffix_volume),
            (path_compensation, suffix_compensation),
        ]

        dfs = []
        for db_file, table_name in db_table_pairs:
            conn = sqlite3.connect(db_file)

            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            conn.close()

            # stock_code, date 인덱스 설정
            df.set_index(['stock_code', 'date'], inplace=True)

            # 검증: stock_code 집합 비교
            stock_codes_in_table = set(df.index.get_level_values('stock_code'))

            if stock_codes_in_table != all_codes:
                missing_in_table = all_codes - stock_codes_in_table
                extra_in_table = stock_codes_in_table - all_codes

                raise ValueError(f"❌ 테이블 '{table_name}'의 stock_code가 all_codes 와 일치하지 않습니다.\n"
                                 f" - 누락된 코드: {missing_in_table}\n"
                                 f" - 추가된 코드: {extra_in_table}")

            # 조건 처리
            if table_name == 'OHLCV_intelliquant':
                df = df.drop(columns=['volume'], errors='ignore')

            elif table_name == 'compensation':
                df = df[['new_share']].rename(columns={'new_share': 'share'})

            dfs.append(df)

        # df 수형 병합
        merged_df = dfs[0].join(dfs[1:], how='outer')

        # share 빈곳 채우기
        merged_df['share'] = merged_df.groupby(level='stock_code')['share'].ffill()

        # 병합된 df에 존재하는 열만 유지하면서 순서 재정렬
        desired_order = [
            'open', 'high', 'low', 'close',
            'volume', 'vf', 'vi', 'vr',
            'cap', 'share'
        ]
        merged_df = merged_df[[col for col in desired_order if col in merged_df.columns]]

        # 인덱스를 컬럼으로 되돌림 (stock_code, date)
        merged_df_reset = merged_df.reset_index()

        # SQLite 데이터베이스 파일 연결 (없으면 새로 생성)
        filename_db = f'{self.suffix}_{datemanage.workday_str}.db'
        file_path_db = savedata_folder + filename_db

        with sqlite3.connect(file_path_db) as conn:
            # 1단계: 데이터 저장 (인덱스 없이 저장)
            merged_df_reset.to_sql(self.suffix, conn, if_exists='replace', index=False)

            # 2단계: 복합 인덱스 생성 (이미 존재하면 예외 발생하므로 IF NOT EXISTS 사용)
            cursor = conn.cursor()
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_stock_code ON {self.suffix} (stock_code);')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_date ON {self.suffix} (date);')
            conn.commit()

        print(f"✅ '{file_path_db}'에 {self.suffix} 테이블 저장 완료 + stock_code/date 인덱스 생성 완료.")
