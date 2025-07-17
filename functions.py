import datetime
from collections import Counter, defaultdict
from datetime import date, timezone
from pprint import pprint

import pandas
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
    if not os.path.isfile('data/esg-stock-news3.json'):
        with open('data/esg-stock-news3.json', mode='w') as f:
            f.write('[')
            f.write(json.dumps(news_to_print, indent=2))
            f.write(']')

    else:
        # This removes the final ']' to avoid JSON syntax errors after a new write
        with open('data/esg-stock-news3.json', 'rb+') as f:
            f.seek(-1, os.SEEK_END)
            f.truncate()

        # This adds a comma before appending and closes the bracket to avoid JSON syntax errors
        with open('data/esg-stock-news3.json', mode='a') as f:
            f.write(',')
            f.write(json.dumps(news_to_print, indent=2))
            f.write(']')


def count_duplicates_in_file():
    if os.path.exists('data/esg-stock-news3.json'):
        with open('data/esg-stock-news3.json', 'r+') as file:
            data = json.load(file)
            temp_ids = []

            for row in data:
                if isinstance(row, dict):
                    temp_ids.append(row['id'])
                if isinstance(row, list):
                    for i in row:
                        temp_ids.append(i['id'])
            return temp_ids
    else:
        pass
        # example duplicate id: 135468074
        # c = Counter(temp_ids)
        # print(c)


def get_historical_news(tickers):
    my_key = os.getenv('FH_API_KEY')
    finnhub_client = finnhub.Client(api_key=my_key)

    end_date = date.today()
    # poll monthly news
    start_date = end_date - datetime.timedelta(days=1)

    temp_ids = count_duplicates_in_file()

    for ticker in tickers:  # for each ticker, get the company's latest news
        original_list = finnhub_client.company_news(ticker, _from=start_date, to=end_date)

        if original_list:  # if the list of results for a specific company is not empty, store it
            filtered_list = [x for x in original_list]

            for article in filtered_list:  # I'm only interested in these values
                desired_keys = {'id', 'datetime', 'headline', 'related', 'source', 'summary', 'url'}
                desired_output = {k: v for (k, v) in article.items() if k in desired_keys}
                desired_output['ticker'] = desired_output['related']  # rename a field
                del desired_output['related']
                desired_output['datetime'] = datetime.datetime.fromtimestamp(desired_output['datetime']).strftime(
                    '%Y-%m-%d')  # date formatting

                # check for duplicate article ids
                if temp_ids is None:
                    write_news_to_json(desired_output)
                else:
                    if desired_output['id'] not in temp_ids:
                        write_news_to_json(desired_output)

