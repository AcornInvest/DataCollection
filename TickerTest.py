import pandas as pd
import GetTicker


test = GetTicker.GetAllTicker()
stocks = test.GetListingStocks("20240313")
print(stocks)


