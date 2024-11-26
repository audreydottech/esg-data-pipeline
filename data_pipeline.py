from functions import *
import time
import datetime

tickers = ['BXBLY', 'VWDRY', 'SMTGY']

print("Starting data pipeline at ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("----------------------------------------------")

# Step 1: extract stock prices for the day
t0 = time.time()
getStockPrices(ticker_list=tickers)
t1 = time.time()
print("Step 1: Done")
print("---> Stock prices downloaded in", str(t1-t0), "seconds", "\n")


# Step 2: extract stock news for the day
t0 = time.time()
getStockNews(ticker_list=tickers)
t1 = time.time()
print("Step 1: Done")
print("---> Stock news downloaded in", str(t1-t0), "seconds", "\n")






