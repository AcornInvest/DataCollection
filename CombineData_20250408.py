import os
from DateManage import DateManage
import configparser
import pandas as pd
import sys
import utils
import sqlite3

# 엑셀 파일들을 모아서 combine 하던 방식.

class CombineData:
    '''
   OHLCV, NoOfshare, volume 합쳐서 Combined 폴더에 저장하는 기능
   2024.8.8 파일을 sql에 저장하도록 함
   '''

    def __init__(self, logger):
        self.logger = logger
        # 설정 로드
        self.load_config()

    def load_config(self):
        self.cur_dir = os.getcwd()
        path = self.cur_dir + '\\' + 'config_CombineData.ini'

        # 설정파일 읽기
        config = configparser.ConfigParser()
        config.read(path, encoding='utf-8')

        # 설정값 읽기
        self.path_data = config['path']['path_data']
        self.path_codeLists = config['path']['path_codelists']
        self.path_savedata = config['path']['path_savedata'] # combined 결과물 저장할 폴더
        self.suffix = 'combined'
        #self.path_date_ref = config['path']['path_date_ref']
        #self.date_prefix = 'bussiness_day_ref'  # date reference 파일의 접미사

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
            share INTEGER
        );
        '''

        for listed_status in category:
            codelist_path = self.path_codeLists + '\\' + listed_status + '\\' + listed_status + '_Ticker_' + datemanage.workday_str + '_modified.xlsx'
            codelist = pd.read_excel(codelist_path, index_col=0)
            codelist['Code'] = codelist['Code'].astype(str)
            codelist['Code'] = codelist['Code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            codes = set(codelist['Code'])  # Ticker 파일에서 가져온 Code column

            # Combined 데이터 저장할 폴더가 존재하지 않으면 생성
            savedata_folder = f'{self.path_savedata}\\{listed_status}\\{datemanage.workday_str}\\'
            if not os.path.exists(savedata_folder):
                os.makedirs(savedata_folder)

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

            # OHLCV 확인
            #files_path = self.path_data + f'\\OHLCV\\Intelliquant\\{listed_status}\\{datemanage.workday_str}_merged'
            folder_OHLCV = os.path.join(self.path_data, 'OHLCV', 'Intelliquant', listed_status, f'{datemanage.workday_str}_merged') # 폴더 경로
            suffix_OHLCV = 'OHLCV_intelliquant'
            check_files(codes, folder_OHLCV, suffix_OHLCV)

            # NoOfShare 확인
            #files_path = self.path_data + f'\\OHLCV\\Compensation\\{listed_status}\\{datemanage.workday_str}_merged'
            folder_compensation = os.path.join(self.path_data, 'OHLCV', 'Compensation', listed_status, f'{datemanage.workday_str}_merged') # 폴더 경로
            suffix_compensation = 'compensation'
            check_files(codes, folder_compensation, suffix_compensation)

            # volume 확인
            #files_path = self.path_data + f'\\OHLCV\\volume\\{listed_status}\\{datemanage.workday_str}_merged'
            folder_volume = os.path.join(self.path_data, 'OHLCV', 'volume', listed_status, f'{datemanage.workday_str}_merged') # 폴더 경로
            suffix_volume = 'volume'
            check_files(codes, folder_volume, suffix_volume)

            '''
            # 거래일 목록 ref 읽어오기
            path_date_ref = f'{self.path_date_ref}\\{self.date_prefix}_{datemanage.workday_str}.xlsx'
            df_business_days = pd.read_excel(path_date_ref)
            df_business_days['Date'] = pd.to_datetime(df_business_days['Date']).dt.date            
            '''

            def arrange_no_of_share(df_combined_0, df_compensation, df_business_days):
                # df_combined_0 의 ['Date'] 열의 시작부터 끝에 해당하도록 'Date' 열 만들기
                # df_compensation 데이터 확장
                pass

            # SQLite 데이터베이스 파일 연결 (없으면 새로 생성)
            filename_db = f'{self.suffix}_{listed_status}_{datemanage.workday}.db'
            file_path_db = savedata_folder + filename_db
            conn = sqlite3.connect(file_path_db)
            conn.execute(create_table_query) # 테이블 생성
            conn.commit()


            for code in codes:
                file_path_OHLCV = f'{folder_OHLCV}\\{code}_{suffix_OHLCV}_{datemanage.workday_str}.xlsx'
                file_path_compensation = f'{folder_compensation}\\{code}_{suffix_compensation}_{datemanage.workday_str}.xlsx'
                file_path_volume = f'{folder_volume}\\{code}_{suffix_volume}_{datemanage.workday_str}.xlsx'
                df_OHLCV = pd.read_excel(file_path_OHLCV, index_col=0)
                df_compensation = pd.read_excel(file_path_compensation, index_col=0)
                df_volume = pd.read_excel(file_path_volume, index_col=0)

                col_OHLCV = ['Open', 'High', 'Low', 'Close', 'Cap']
                col_volume = ['Volume', 'VF', 'VI', 'VR']
                col_compenston = ['NewNoShare']

                # 필요한 열만 선택
                df_OHLCV_filtered = df_OHLCV[col_OHLCV]
                df_volume_filtered = df_volume[col_volume]
                df_compensation_filtered = df_compensation[col_compenston]

                df_combined = df_OHLCV_filtered.join(df_volume_filtered, how='outer')
                df_combined = df_combined.join(df_compensation_filtered, how='outer')
                df_combined.rename(columns={'NewNoShare': 'Share'}, inplace=True)
                #df_combined['Share'] = df_combined['Share'].fillna(method='ffill')
                df_combined['Share'] = df_combined['Share'].ffill()
                #df_combined = pd.merge(df_OHLCV_filtered, df_volume_filtered, on='Date', how='outer')
                # 2024.7.4 utils의 save_df_to_excel 바뀌면서 추가됨
                df_combined.reset_index(inplace=True)  # 인덱스를 'date' 열로 변환
                df_combined.rename(columns={'index': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', \
                                            'Volume': 'volume', 'VF': 'vf', 'VI': 'vi', 'VR': 'vr', 'Cap': 'cap', \
                                            'Share': 'share'}, inplace=True)  # 'index' 열 이름을 'date'로 변경, 다른 열 이름도 변경
                df_combined['stock_code'] = code  # 종목코드 열 추가
                df_combined.to_sql('combined_ohlcv', conn, if_exists='append', index=False)

                #utils.save_df_to_excel(df_combined, code, ('_' + self.suffix + '_' + datemanage.workday_str), savedata_folder)

            # 인덱스 생성 (쿼리 성능 향상)
            conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON combined_ohlcv (stock_code);')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON combined_ohlcv (date);')
            conn.commit()
            # 데이터베이스 연결 종료
            conn.close()
