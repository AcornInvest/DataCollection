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
        self.flag_mod = flag_mod  # ohlcv share 변경된 데이터 다시 받은 거 합칠 때 사용

        # 설정 로드
        self.load_config() # 자식클래스에서 정의할 것
        # self.suffix = 'data' # 파일 이름 저장시 사용하는 접미사. 자식 클래스에서 정의할 것

    def check_continuity(self, datemanage):
        # DB 연결
        conn1 = sqlite3.connect(self.path_merged)
        conn2 = sqlite3.connect(self.path_new)

        # 쿼리문
        query = f"""
            SELECT * FROM {self.table_merged}
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

        # diff 컬럼 확인 함수
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
            print(f"[{self.file_new}] 공통된 stock codes 의 startday에서의 모든 데이터가 일치합니다.")
            return flag_error, flag_mod_stocks

        # 데이터에 차이가 있는 경우
        print(f"[{self.file_new}] 차이 발생!")

        check_columns = ['open', 'high', 'low', 'close']
        # share 차이가 생긴 종목들 중 수정종가 변경이 생긴 종목들 찾는 조건
        # 차이가 발생한 열이 check_columns = ['open', 'high', 'low', 'close'] 에 속하면서 다른 열들에서는 차이가 없는지
        is_only_check_cols = diff_merged['diff_cols'].apply(lambda cols: set(cols).issubset(check_columns))

        # 조건 만족하는 행
        valid_rows = diff_merged[is_only_check_cols]

        # 조건을 만족하지 않는 행
        invalid_rows = diff_merged[~is_only_check_cols]

        if not valid_rows.empty: # 수정종가에 차이가 있는 종목이 하나라도 있을 경우
            mod_stock_codes = set(valid_rows['stock_code'])

            if mod_stock_codes:
                output_df = pd.DataFrame({'stock_code': sorted(mod_stock_codes)})
                path = os.path.join(self.folder_new, f"mod_stock_codes_{datemanage.workday_str}.xlsx")
                #path = self.folder_new + f"mod_stock_codes_{datemanage.workday_str}.xlsx"
                output_df.to_excel(path, index=False)
                print(f"\n종가 변경 stock_code를 Excel로 저장했습니다: mod_stock_codes_{datemanage.workday_str}.xlsx")
                flag_mod_stocks = True

        if not invalid_rows.empty: # 수정종가 외에 다른 열에 차이가 있는 종목이 하나라도 있는 경우
            print(f"\n조건을 만족하지 않는 차이 행:")
            display_cols = ['stock_code', 'date', 'diff_cols'] + [f"{col}_{sfx}" for col in compare_columns for sfx
                                                                  in ['1', '2']]
            print(invalid_rows[display_cols])
            flag_error = True

        return flag_error, flag_mod_stocks

    def validate_exact_date_match_per_stock(self):
        """
        new_table에 있는 모든 stock_code에 대해,
        merged_table에서 해당 종목의 date 리스트가 정확히 일치하는지 확인
        """
        # 데이터 불러오기
        conn_merged = sqlite3.connect(self.path_merged)
        df_merged = pd.read_sql_query(f"SELECT stock_code, date FROM {self.table_merged}", conn_merged)
        conn_merged.close()

        conn_new = sqlite3.connect(self.path_new)
        df_new = pd.read_sql_query(f"SELECT stock_code, date FROM {self.table_new}", conn_new)
        conn_new.close()

        # 결과 누적용 리스트
        mismatch_list = []

        # 비교 대상 stock_code만 추출
        stock_codes = df_new['stock_code'].unique()

        for code in stock_codes:
            dates_in_new = set(df_new[df_new['stock_code'] == code]['date'])
            dates_in_merged = set(df_merged[df_merged['stock_code'] == code]['date'])

            if dates_in_new != dates_in_merged:
                mismatch_list.append({
                    'stock_code': code,
                    'only_in_new': sorted(dates_in_new - dates_in_merged),
                    'only_in_merged': sorted(dates_in_merged - dates_in_new)
                })

        if mismatch_list:
            sample = mismatch_list[:3]  # 예시 3개만 출력
            raise ValueError(
                f"intelliquant_mod와 기존 merged_db 비교: stock_code별 date 불일치가 감지되었습니다 (총 {len(mismatch_list)}개 종목).\n"
                f"예시:\n" +
                '\n'.join([
                    f"- {item['stock_code']}: new_only={item['only_in_new']}, merged_only={item['only_in_merged']}"
                    for item in sample
                ])
            )
        else:
            print("intelliquant_mod와 기존 merged_db 비교: 모든 stock_code의 date 목록이 정확히 일치합니다.")


    def merge_dbs(self):
        # 기존 merged 파일 연결
        conn = sqlite3.connect(self.path_merged)
        cursor = conn.cursor()

        # new file attach
        cursor.execute(f"ATTACH DATABASE '{self.path_new}' AS new_db")

        if self.flag_mod: #수정 주가들의 ohlc 를 통째로 교체하는 경우
            check_columns = ['open', 'high', 'low', 'close']

            # 이 부분 코드를 만들어줘
            # new_db 에 있는 테이블 {self.table_new}의 열들 중  check_columns에 해당하는 열들의 데이터를,
            # {self.path_merged}파일의 테이블 {self.table_merged}의 같은 열들에 넣어줘.
            # new_db의 {self.table_new}열은 항상 {self.path_merged}파일의 테이블 {self.table_merged} 열과 같은 (stock_code, date) 쌍을 가진다고 전제해

            set_clause = ', '.join([
                f"{col} = new_values.{col}" for col in check_columns
            ])

            # FROM 절을 이용해 new_values 테이블과 직접 매칭
            cursor.execute(f"""
                   UPDATE {self.table_merged}
                   SET {set_clause}
                   FROM new_db.{self.table_new} AS new_values
                   WHERE {self.table_merged}.stock_code = new_values.stock_code
                     AND {self.table_merged}.date = new_values.date;
               """)

        else: # 원래 merge file에 new 파일을 update 하는 경우
            # INSERT OR IGNORE: 동일한 PRIMARY KEY/UNIQUE KEY가 있다면 무시됨
            cursor.execute(f"""
                INSERT OR IGNORE INTO {self.table_merged}
                SELECT * FROM new_db.{self.table_new}
            """)

        # 마무리
        conn.commit()
        cursor.execute("DETACH DATABASE new_db")
        conn.close()

