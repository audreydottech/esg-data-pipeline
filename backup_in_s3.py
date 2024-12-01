import datetime
import polars as pl
import s3fs

import boto3

# TO DO: Need to add secret to Github and schedule a job to write a new file to S3 once a month
# TO DO: Configure IAM

s3 = boto3.client('s3', aws_access_key_id='AKIA37K7GYJPJIHAE4W2',
                  aws_secret_access_key='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')


def grab_latest_news_from_Github():
    """
    Function to grab the latest 30 days of news data from json files
    """
    end_date = datetime.datetime.now().date()
    start_date = end_date - datetime.timedelta(days=30)
    df = pl.read_json('data/esg-stock-news.json')
    df = df.with_columns(pl.col('providerPublishTime').str.to_date('%Y-%m-%d'))
    filtered_df = df.filter(pl.col("providerPublishTime").is_between(start_date, end_date))

    fs = s3fs.S3FileSystem()

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

    fs = s3fs.S3FileSystem()

    destination = "s3://environmental-stock-data-bucket/esg-stock-prices_" + str(
        datetime.datetime.now().strftime('%Y_%m_%d')) + '.json'

    with fs.open(destination, mode='wb') as f:
        filtered_df.write_json(f)


def upload_file_to_s3():
    """
    Function to manually upload whole files
    """
    s3.upload_file('data/esg-stock-news.json', 'environmental-stock-data-bucket', 'esg-stock-news.json')

    s3.upload_file('data/esg-stock-prices.csv', 'environmental-stock-data-bucket', 'esg-stock-prices.csv')


# TO DO: Snowflake job to grab the latest files from S3
# TO DO: Can dbt just grab the files from S3 and transform them before loading them into Snowflake?
