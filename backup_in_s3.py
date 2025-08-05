import datetime
import os
import polars as pl
import s3fs
import boto3

my_aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
my_aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')
fs = s3fs.S3FileSystem(key=my_aws_access_key_id, secret=my_aws_access_secret_key)


def grab_latest_news_from_Github():
    """
    Function to grab the latest 30 days of news data from json files
    """
    end_date = datetime.datetime.now().date()
    start_date = end_date - datetime.timedelta(days=30)
    df = pl.read_json('data/esg-stock-news3.json')
    # df = df.with_columns(pl.col('providerPublishTime').str.to_date('%Y-%m-%d'))
    filtered_df = df.filter(pl.col("datetime").is_between(start_date, end_date))

    destination = "s3://environmental-stock-data-bucket/esg-stock-news_" + str(
        datetime.datetime.now().strftime('%Y_%m_%d')) + '.json'

    with fs.open(destination, mode='wb') as f:
        filtered_df.write_json(f)


def grab_latest_prices_from_Github():
    """
    Function to grab the latest 30 days of stock price data from csv files
    """
    end_date = datetime.datetime.now().date()
    start_date = end_date - datetime.timedelta(days=30)
    df = pl.read_csv('data/esg-stock-prices.csv')
    df = df.with_columns(pl.col('timestamp').str.to_date('%Y-%m-%d %H:%M:%S'))
    filtered_df = df.filter(pl.col("timestamp").is_between(start_date, end_date))

    destination = "s3://environmental-stock-data-bucket/esg-stock-prices_" + str(
        datetime.datetime.now().strftime('%Y_%m_%d')) + '.json'

    print(destination)

    with fs.open(destination, mode='wb') as f:
        filtered_df.write_json(f)


def upload_file_to_s3():
    """
    Function to manually upload whole files
    """
    s3 = boto3.client('s3', aws_access_key_id=my_aws_access_key_id,
                      aws_secret_access_key=my_aws_access_secret_key)
    s3.upload_file('data/esg-stock-news3.json', 'environmental-stock-data-bucket', 'esg-stock-news3.json')

    s3.upload_file('data/esg-stock-prices.csv', 'environmental-stock-data-bucket', 'esg-stock-prices.csv')