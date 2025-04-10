import os
from DateManage import DateManage
import configparser
import numpy as np
import pandas as pd
import shutil
import sqlite3

# 이걸 parent 로 하고, ohlcv_combined, financial, processed, technical 로 자식 class 를 만든다.
# listed_stock_data, chancelist 는 1년/6개월에 한번만 추가하면 된다.


# verify compensation 에서 주식수 변동있는 종목들을 xlsx 로 만드는 것 필요없다.
# 여기서만 판단한다. ohlc 외에 값이 다르면 error 로 판단하고,
# ohlc 만 다르면 목록을 저장하여 ohlcv_mod로 새로 backtest 하도록 한다.

class MergeData:
    '''
   지난 데이터와 현재 데이터를 합치는 기능의 parent class
   '''

    def __init__(self, logger, flag_mod=False):
        self.logger = logger
        self.flag_mod = flag_mod  # ohlcv share 변경된 데이터 대상 확인 용도

        # 설정 로드
        self.load_config() # 자식클래스에서 정의할 것
        # self.suffix = 'data' # 파일 이름 저장시 사용하는 접미사. 자식 클래스에서 정의할 것

    def check_continuity(self, datemanage):
        folder1 = f'{self.path_data}\\'
        file1 = f'{self.suffix}_merged.db'
        path1 = folder1 + file1
        folder2 = f'{self.path_data}\\{datemanage.workday_str}\\'
        file2 = f'{self.suffix}_{datemanage.workday_str}.db'
        path2 = folder2 + file2

        # share 변경된 종목 목록 가져오기
        if self.flag_mod:
            path = self.path_compensation_data + f'\\{datemanage.workday_str}\\share_modified_codes_{datemanage.workday_str}.xlsx'
            stocks_mod = pd.read_excel(path, index_col=None)
            stocks_mod['stock_code'] = stocks_mod['stock_code'].astype(str)
            stocks_mod['stock_code'] = stocks_mod['stock_code'].str.zfill(6)  # 코드가 6자리에 못 미치면 앞에 0 채워넣기
            check_columns = ['open', 'high', 'low', 'close']

        # DB 연결
        conn1 = sqlite3.connect(path1)
        conn2 = sqlite3.connect(path2)

        # 쿼리문
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE date = ?
        """

        # 날짜 기준 행만 불러오기
        df1 = pd.read_sql_query(query, conn1, params=(datemanage.startday_str,))
        df2 = pd.read_sql_query(query, conn2, params=(datemanage.startday_str,))

        conn1.close()
        conn2.close()

        def compare_stock_codes(df1, df2):
            set1 = set(df1['stock_code'])
            set2 = set(df2['stock_code'])

            only_in_df1 = set1 - set2
            only_in_df2 = set2 - set1

            if not only_in_df1 and not only_in_df2:
                print("✅ 두 DataFrame의 stock_code 값들이 완전히 동일합니다.")
            else:
                if only_in_df1:
                    print(f"❌ df1에만 있고 df2에는 없는 stock_code들 ({len(only_in_df1)}개):")
                    print(sorted(only_in_df1))

                if only_in_df2:
                    print(f"❌ df2에만 있고 df1에는 없는 stock_code들 ({len(only_in_df2)}개):")
                    print(sorted(only_in_df2))

        compare_stock_codes(df1, df2)

        flag_error = False
        flag_mod_stocks = False

        # 병합
        merged = pd.merge(df1, df2, on=["stock_code", "date"], suffixes=('_1', '_2'))

        # 비교할 열 목록
        compare_columns = [col for col in df1.columns if col not in ['stock_code', 'date']]

        # diff 컬럼 확인
        def diff_cols(row):
            diffs = []
            for col in compare_columns:
                val1 = row[f"{col}_1"]
                val2 = row[f"{col}_2"]

                # 둘 다 NaN이면 같다고 판단
                if pd.isna(val1) and pd.isna(val2):
                    continue

                # 숫자형인 경우 np.isclose()로 비교
                elif pd.api.types.is_numeric_dtype(type(val1)) and pd.api.types.is_numeric_dtype(type(val2)):
                    if not np.isclose(val1, val2, equal_nan=True):
                        diffs.append(col)

                # 그 외 타입은 일반 비교
                else:
                    if val1 != val2:
                        diffs.append(col)
            return diffs

        merged['diff_cols'] = merged.apply(diff_cols, axis=1)

        # 차이 있는 행만 필터링
        diff_merged = merged[merged['diff_cols'].apply(lambda x: len(x) > 0)]

        if diff_merged.empty:
            print(f"[{file2}] 모든 데이터가 일치합니다.")
            return flag_error, flag_mod_stocks

        # 차이가 있는 경우
        print(f"[{file2}] 차이 발생!")

        if self.flag_mod and stocks_mod is not None:
            # share 차이가 생긴 종목들 중 수정종가 변경이 생긴 종목들 찾는 조건
            # (1) 차이가 발생한 행의 stock_code 가 stocks_mod['stock_code'] 안에 있는지
            # (2) 차이가 발생한 열이 check_columns = ['open', 'high', 'low', 'close'] 에 속하면서 다른 열들에서는 차이가 없는지
            is_in_mod_stock = diff_merged['stock_code'].isin(stocks_mod['stock_code'])
            is_only_check_cols = diff_merged['diff_cols'].apply(lambda cols: set(cols).issubset(check_columns))


            # 조건 만족하는 행
            valid_rows = diff_merged[is_in_mod_stock & is_only_check_cols]
            mod_stock_codes = set(valid_rows['stock_code'])

            if mod_stock_codes:
                output_df = pd.DataFrame({'stock_code': sorted(mod_stock_codes)})
                path = folder2 + f"mod_stock_codes_{datemanage.workday_str}.xlsx"
                output_df.to_excel(path, index=False)
                print(f"\n종가 변경 stock_code를 Excel로 저장했습니다: mod_stock_codes_{datemanage.workday_str}.xlsx")
                flag_mod_stocks = True

            # 조건을 만족하지 않는 행
            invalid_rows = diff_merged[~(is_in_mod_stock & is_only_check_cols)]

            if not invalid_rows.empty:
                print(f"\n조건을 만족하지 않는 차이 행:")
                display_cols = ['stock_code', 'date', 'diff_cols'] + [f"{col}_{sfx}" for col in compare_columns for sfx
                                                                      in ['1', '2']]
                print(invalid_rows[display_cols])
                flag_error = True
        else:
            # flag_mod False일 때 모든 차이 출력
            for col in compare_columns:
                col1 = f"{col}_1"
                col2 = f"{col}_2"
                diff_df = merged[merged[col1] != merged[col2]]
                if not diff_df.empty:
                    print(f"\n[열: {col}] 값이 다른 행:")
                    print(diff_df[['stock_code', 'date', col1, col2]])

            flag_error = True

        return flag_error, flag_mod_stocks

    def merge_dbs(self):
        pass

    # 2025.4.10 예전에 엑셀 합치던 함수
    '''
    def merge_files(self, listed_status, date_before, date_recent, path_data, suffix):
        folder_data_before = f'{path_data}\\{listed_status}\\{date_before}_merged'
        folder_data_recent = f'{path_data}\\{listed_status}\\{date_recent}'
        folder_data_result = f'{path_data}\\{listed_status}\\{date_recent}_merged'

        # Ensure result folder exists
        os.makedirs(folder_data_result, exist_ok=True)

        file_names_data_before = [f for f in os.listdir(folder_data_before) if suffix in f]
        file_names_data_recent = [f for f in os.listdir(folder_data_recent) if suffix in f]

        codes_before = {f.split('_')[0]: f for f in file_names_data_before}
        codes_recent = {f.split('_')[0]: f for f in file_names_data_recent}

        only_in_before = set(codes_before.keys()) - set(codes_recent.keys())
        only_in_recent = set(codes_recent.keys()) - set(codes_before.keys())
        in_both = set(codes_before.keys()) & set(codes_recent.keys())

        # Save only_in_before list to a text file
        with open(os.path.join(folder_data_result, f'only_existed_in_before_{date_before}.txt'), 'w') as file:
            for code in only_in_before:
                file.write(f'{code}\n')

        # Save only_in_recent list to a text file
        with open(os.path.join(folder_data_result, f'only_existed_in_recent_{date_recent}.txt'), 'w') as file:
            for code in only_in_recent:
                file.write(f'{code}\n')

        # Process only_in_before files
        for code in only_in_before:
            src_file = os.path.join(folder_data_before, codes_before[code])
            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            shutil.copy2(src_file, dest_file)

        # Process only_in_recent files
        for code in only_in_recent:
            src_file = os.path.join(folder_data_recent, codes_recent[code])
            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            shutil.copy2(src_file, dest_file)

        # Process files in both
        for code in in_both:
            file_before = os.path.join(folder_data_before, codes_before[code])
            file_recent = os.path.join(folder_data_recent, codes_recent[code])

            df_before = pd.read_excel(file_before)
            df_recent = pd.read_excel(file_recent)

            # Combine dataframes and drop duplicates
            combined_df = pd.concat([df_before, df_recent]).drop_duplicates(subset='Date').reset_index(drop=True)

            dest_file = os.path.join(folder_data_result, f'{code}_{suffix}_{date_recent}_merged.xlsx')
            combined_df.to_excel(dest_file, index=False)

        print(f'{folder_data_before}, {folder_data_recent} 파일 merge 완료')
        self.logger.info(f'{folder_data_before}, {folder_data_recent} 파일 merge 완료')
    '''