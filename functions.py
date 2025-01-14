import datetime
import json
import os
import time
import logging

import requests
from bs4 import BeautifulSoup
import yfinance as yf
import finnhub
import polars as pl

tickers = ['SMSMY', 'BXBLY', 'VWDRY', 'SMTGY', 'TPE', 'NDX1', 'BDORY', 'SBGSY', 'CHRH', 'STN']


def update_json_keys(data, old_keys, new_keys):
    """
    Function to normalize column names

    """
    for old_key, new_key in zip(old_keys, new_keys):
        if old_key in data:
            data[new_key] = data.pop(old_key)


def get_stock_price(ticker):
    """
    Function to extract latest stock prices for top 100 ESG stocks

    """
    my_key = os.getenv('FH_API_KEY')
    finnhub_client = finnhub.Client(api_key=my_key)

    stock_data = finnhub_client.quote(ticker)

    update_json_keys(stock_data, ['c', 'd', 'dp', 'h', 'l', 'o', 'pc', 't'],
                     ['current_price', 'change', 'percent_change', 'high', 'low', 'open', 'previous_close',
                      'timestamp'])
    # add a stock ticker column
    stock_data['ticker'] = ticker

    # format date
    ts = int(stock_data['timestamp'])
    stock_data['timestamp'] = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    return stock_data


def getStockPrices(ticker_list):
    stock_price_list = []

    for ticker in ticker_list:
        res = get_stock_price(ticker)
        stock_price_list.append(res)

    # pl.DataFrame(stock_price_list).write_csv('data/esg-stock-prices.csv')

    df2 = pl.DataFrame(stock_price_list)
    with open("data/esg-stock-prices.csv", mode="a") as f:
        df2.write_csv(f, include_header=False)


def get_article_text(url: str) -> str:
    """
    Function to scrape ESG stock news

    """
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_text = ' '.join([p.get_text() for p in soup.find_all('p')])
        return article_text
    except:
        return "Error retrieving article text."


def get_stock_news(ticker):
    """
    Function to only retrieve relevant stock news information for a single stock

    """
    # retrieve stock news for a single stock
    stock = yf.Ticker(ticker)
    news = stock.news

    # reformat timestamp to string to avoid JSON serialization issue at write time

    for article in stock.news:
        # remove less critical data elements
        article.pop('thumbnail', None)
        article.pop('type', None)
        article.pop('uuid', None)

    return news


def getStockNews(ticker_list):
    """
    Function to write/append the latest stock news to a JSON file

    """

    end_ts = time.time()
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=2)
    start_ts = start_date.timestamp()

    for ticker in ticker_list:

        entry = get_stock_news(ticker)

        # If the JSON file does not exist, create it
        if not os.path.isfile('data/esg-stock-news.json'):
            with open('data/esg-stock-news.json', mode='w') as f:
                f.write(json.dumps(entry, indent=2))

        else:
            # safety measure to make sure there is no duplicate content
            for article in entry:
                try:
                    ts = article.get('providerPublishTime')
                    if start_ts < ts < end_ts:

                        # This removes the final ']' to avoid JSON syntax errors after a new write
                        with open('data/esg-stock-news.json', 'rb+') as f:
                            f.seek(-1, os.SEEK_END)
                            f.truncate()

                        # This adds a comma before appending and closes the bracket to avoid JSON syntax errors
                        with open('data/esg-stock-news.json', mode='a') as f:
                            f.write(',')
                            f.write(json.dumps(article, indent=2))
                            f.write(']')
                except (TypeError, AttributeError, ValueError) as e:
                    logging.error(e)


destination = "s3://environmental-stock-data-bucket/esg-stock-news_" + str(
    datetime.datetime.now().strftime('%Y_%m_%d')) + '.json'


