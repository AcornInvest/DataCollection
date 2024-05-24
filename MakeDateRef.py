import pandas as pd
from datetime import datetime
import re

start_day = datetime(2000, 1, 4)
end_day = datetime(2024, 3, 29)

bussiness_days = pd.bdate_range(start=start_day, end=end_day)
df_bussiness_days = pd.DataFrame(bussiness_dates, columns=['Date'])
df_bussiness_days['Date'] = pd.to_datetime(df_bussiness_days['Date']).dt.strftime('%Y-%m-%d')

path_file = 'C:\\Work_Dotori\\StockDataset\\OHLCV\\date_reference\\business_day_intelliquant_2024-03-29.txt'
dates = []
with open(path_file, 'r') as file:
    for line in file:
        date = re.search(r'\[(\d{4}-\d{2}-\d{2})\]', line).group(1)
        dates.append(date)

df_business_days_intelliquant = pd.DataFrame(dates, columns=['Date'])
df_business_days_intelliquant['Date'] = pd.to_datetime(df_business_days_intelliquant['Date']).dt.strftime('%Y-%m-%d')

# df_bussiness_days에 있는데 df_business_days_intelliquant에 없는 날짜 찾기
missing_holidays = df_bussiness_days[~df_bussiness_days['Date'].isin(df_business_days_intelliquant['Date'])]
df_holidays = pd.DataFrame(missing_holidays)  # 새로운 데이터프레임 df_holidays 생성

df_holidays.set_index('Date', inplace=True)
df_holidays.to_excel("C:\\Work_Dotori\\StockDataset\\OHLCV\\date_reference\\holiday_ref_2024-03-29.xlsx", index=True)

df_business_days_intelliquant.set_index('Date', inplace=True)
df_business_days_intelliquant.to_excel(
    "C:\\Work_Dotori\\StockDataset\\OHLCV\\date_reference\\bussiness_day_ref_2024-03-29.xlsx", index=True)

class MakeDateRef:
    def __init__(self):
        pass

    def make_bussiness_day_ref(self):
        pass

    def make_financial_update_day_ref(self):
        pass


