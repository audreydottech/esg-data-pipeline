import datetime
from datetime import date, timezone
from dateutil import parser
import json
import os

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


def write_news_to_json(news_to_print):
    # If the JSON file does not exist, create it
    if not os.path.isfile('data/esg-stock-news.json'):
        with open('data/esg-stock-news.json', mode='w') as f:
            f.write(json.dumps(news_to_print, indent=2))
    else:
        # This removes the final ']' to avoid JSON syntax errors after a new write
        with open('data/esg-stock-news.json', 'rb+') as f:
            f.seek(-1, os.SEEK_END)
            f.truncate()

        # This adds a comma before appending and closes the bracket to avoid JSON syntax errors
        with open('data/esg-stock-news.json', mode='a') as f:
            f.write(',')
            f.write(json.dumps(news_to_print, indent=2))
            f.write(']')


def getStockNews(ticker_list):
    """
    Function to write/append the latest stock news to a JSON file

    """
    end_date = date.today()

    start_date = end_date - datetime.timedelta(days=1)

    for ticker in ticker_list:

        stock = yf.Ticker(ticker)
        stock_news = stock.news

        # safety measure to make sure there is no duplicate content
        if stock_news is not None:

            for item in stock_news:

                ts = item['content']['pubDate']
                article_date = parser.isoparse(ts[:-1]).astimezone(timezone.utc).date()

                if start_date < article_date < end_date:
                    content = item['content']

                    desired_keys = {'id', 'title', 'canonicalUrl', 'pubDate'}
                    desired_output = [{k: v for (k, v) in content.items() if k in desired_keys}]
                    desired_output[0]['ticker'] = ticker
                    desired_output[0]['pubDate'] = article_date.strftime('%Y-%m-%d')
                    desired_output[0]['canonicalUrl'] = desired_output[0]['canonicalUrl']['url']

                    write_news_to_json(desired_output[0])
