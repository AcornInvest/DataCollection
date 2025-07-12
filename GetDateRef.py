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
        #self.cur_dir = os.getcwd()
        self.cur_dir = os.path.dirname(os.path.abspath(__file__))
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

        return self.path_backtest_result

    def process_backtest_result(self, path_file):  # backtest result 를 처리하여 df로 반환
        # 결과를 저장할 리스트
        ref_date = []

        # date 패턴: [YYYY-MM-DD]. 해당 줄에 이외의 다른 문자는 없는 경우
        date_pattern = r'\[(\d{4}-\d{2}-\d{2})\]'
        date_pattern = r'^\[(\d{4}-\d{2}-\d{2})\]\s*$'

        with open(path_file, 'r', encoding='utf-8') as file:
            for line in file:
                # 패턴이 날짜만 포함된 줄과 일치하는 경우
                if re.search(date_pattern, line.strip()):
                    # 날짜 추출
                    date = re.search(date_pattern, line.strip()).group(1)
                    # ref_date 리스트에 추가
                    ref_date.append(date)

        # DataFrame 생성
        #df_date_ref = pd.DataFrame(ref_date, columns=["date"], index=True)
        #df_date_ref = pd.DataFrame(ref_date, columns=["date"], index=range(len(ref_date)))
        df_date_ref = pd.DataFrame({
            "index": range(len(ref_date)),  # 0부터 시작하는 인덱스 열 생성
            "date": ref_date
        })

        return df_date_ref

    def merge_date_ref(self, datemanage, df_date_ref_new, date_prefix):
        # 기존 거래일 목록 ref 읽어오기
        filename_old = f'{date_prefix}_{datemanage.startday_str}.xlsx'
        path_date_ref_old = f'{self.path_data}\\{filename_old}'
        df_date_ref_old = pd.read_excel(path_date_ref_old)
        df_date_ref_old = df_date_ref_old.reset_index()

        # 조건 확인 및 합치기
        if df_date_ref_old.iloc[-1]['date'] == df_date_ref_new.iloc[0]['date']:
            # 행 방향으로 합치기
            df_date_ref_combined = pd.concat([df_date_ref_old, df_date_ref_new.iloc[1:]], ignore_index=True)
            #df_date_ref_combined = df_date_ref_combined.reset_index()

            # 'date' 열만 선택
            df_date_ref_combined = df_date_ref_combined[['date']]

            #print("DataFrames successfully combined:")
            filename_combined = f'{date_prefix}_{datemanage.workday_str}.xlsx'
            path_date_ref_combined = f'{self.path_data}\\{filename_combined}'
            df_date_ref_combined.to_excel(path_date_ref_combined, index=False)
            print(f"Combined DataFrame saved to {filename_combined}")
            self.logger.info(f"Combined DataFrame saved to {filename_combined}")

            return df_date_ref_combined

        else:
            print(f"Error: {filename_old}의 마지막 행 값이 새로운 df_date_ref의 첫번째 값과 다름")

    def merge_financial_date_ref(self, datemanage, df_financial_date_ref_new, date_prefix):
        # 기존 거래일 목록 ref 읽어오기
        filename_old = f'{date_prefix}_{datemanage.startday_str}.xlsx'
        path_financial_date_ref_old = f'{self.path_data}\\{filename_old}'
        df_financial_date_ref_old = pd.read_excel(path_financial_date_ref_old)
        df_financial_date_ref_old['date'] = pd.to_datetime(df_financial_date_ref_old['date']).dt.date

        # 조건 1: old의 마지막 값과 new의 첫 번째 값이 동일한지 확인
        last_date_old = df_financial_date_ref_old['date'].iloc[-1]
        first_date_new = df_financial_date_ref_new['date'].iloc[0]


        old 의 마지막 값과 new 의 마지막 값이 동일한지 확인 필요

        # 조건 2: 다음 분기 순서인지 확인
        def is_next_quarter(last_date, first_date):
            # 현재 분기에서 다음 분기로 이동 확인
            quarters = [4, 6, 9, 12]
            last_month = last_date.month
            first_month = first_date.month
            year_diff = first_date.year - last_date.year

            if last_month == 12:
                # 12월 -> 다음해 4월
                return year_diff == 1 and first_month == 4
            else:
                # 현재 분기 -> 다음 분기
                next_month = quarters[quarters.index(last_month) + 1]
                return year_diff == 0 and first_month == next_month

        # 조건 확인 및 DataFrame 병합
        if last_date_old == first_date_new:
            # 조건 1: 첫 번째 행 생략
            df_financial_date_ref_new = df_financial_date_ref_new.iloc[1:]
            df_combined = pd.concat([df_financial_date_ref_old, df_financial_date_ref_new], ignore_index=True)
            print(f"{filename_old}의 마지막 행 값과 새로운 df_financial_date_ref의 첫번째 값이 동일함")
        elif is_next_quarter(last_date_old, first_date_new):
            # 조건 2: 그대로 합침
            df_combined = pd.concat([df_financial_date_ref_old, df_financial_date_ref_new], ignore_index=True)
            print(f"{filename_old}의 마지막 행 값과 새로운 df_financial_date_ref의 첫번째 값이 동일하지 않으며 연속적임")
        else:
            print(f"Error: {filename_old}의 마지막 행 값과 새로운 df_financial_date_ref의 첫번째 값이 같지 않으며 연속성이 없음")
            df_combined = None

        return df_combined

    def make_financial_date_ref(self, df_date_ref):
        # 'date' 열을 datetime 형식으로 변환 후 datetime.date로 변환
        df_date_ref['date'] = pd.to_datetime(df_date_ref['date'])

        # 특정 월(4월, 6월, 9월, 12월) 필터링
        target_months = [4, 6, 9, 12]
        filtered_date_ref = df_date_ref[df_date_ref['date'].dt.month.isin(target_months)]

        # 각 월의 첫 번째 날짜만 선택
        filtered_date_ref = filtered_date_ref.groupby(filtered_date_ref['date'].dt.month).first().drop(columns=['index'])

        # 'date' 열을 datetime.date 형식으로 변환
        filtered_date_ref['date'] = filtered_date_ref['date'].dt.date

        return filtered_date_ref

    def count_num_days_financial_date(self, datemanage, df_financial_date_ref_combined, df_date_ref_combined, date_prefix):
        df_date_ref_combined['date'] = pd.to_datetime(df_date_ref_combined['date']).dt.date

        # 'num_days'가 NaN인 행들만 계산
        for i in df_financial_date_ref_combined[df_financial_date_ref_combined['num_days'].isna()].index:
            # 현재 행의 날짜와 다음 행의 날짜를 가져옴
            current_date = df_financial_date_ref_combined.loc[i, 'date']
            next_date = (
                df_financial_date_ref_combined.loc[i + 1, 'date']
                if i + 1 < len(df_financial_date_ref_combined)
                else None  # 마지막 행의 경우
            )

            # df_date_ref_combined에서 날짜 범위에 해당하는 행 수 계산
            if next_date:
                # 현재 행의 날짜부터 다음 행의 날짜보다 작은 값까지
                count = df_date_ref_combined[
                    (df_date_ref_combined['date'] >= current_date) &
                    (df_date_ref_combined['date'] < next_date)
                    ].shape[0]
            else:
                # 마지막 행: 현재 행의 날짜와 같거나 이후인 모든 값
                count = df_date_ref_combined[
                    (df_date_ref_combined['date'] >= current_date)
                ].shape[0]

            # 'num_days' 열에 값 채우기
            df_financial_date_ref_combined.loc[i, 'num_days'] = count

        filename_combined = f'{date_prefix}_{datemanage.workday_str}.xlsx'
        path_date_ref_combined = f'{self.path_data}\\{filename_combined}'
        df_financial_date_ref_combined.to_excel(path_date_ref_combined, index=False)
        print(f"Combined DataFrame saved to {filename_combined}")
        self.logger.info(f"Combined DataFrame saved to {filename_combined}")
        pass
