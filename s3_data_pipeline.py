from backup_in_s3 import *
import time
import datetime


print("Starting S3 data pipeline at ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("----------------------------------------------")

# Step 1: extract stock prices for the day
t0 = time.time()
grab_latest_news_from_Github()
t1 = time.time()
print("Step 1: Done")
print("---> Stock news uploaded to S3 in", str(t1-t0), "seconds", "\n")


# Step 2: extract stock news for the day
t0 = time.time()
grab_latest_prices_from_Github()
t1 = time.time()
print("Step 1: Done")
print("---> Stock prices uploaded to S3 in", str(t1-t0), "seconds", "\n")