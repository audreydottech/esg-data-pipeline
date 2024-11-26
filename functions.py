import datetime
import os

import requests
from bs4 import BeautifulSoup
import yfinance as yf
import finnhub
import polars as pl

tickers = ['BXBLY', 'VWDRY', 'SMTGY']


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

    pl.DataFrame(stock_price_list).write_parquet('data/esg-stock-prices.parquet')


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

    # add a key to the original dictionary
    K = "text"

    # scrape text from link and add to results
    for article in stock.news:
        article_text = get_article_text(article['link'])
        article[K] = article_text
        # reformat timestamp
        article['providerPublishTime'] = (datetime.datetime.fromtimestamp(article['providerPublishTime']).date())

        article.pop('thumbnail', None)
        article.pop('type', None)
        article.pop('uuid', None)

    return news


def getStockNews(ticker_list):
    """
    Function to write all stock news

    """
    end_date = datetime.datetime.now().date()
    start_date = end_date - datetime.timedelta(days=15)

    stock_news_list = []
    for ticker in ticker_list:
        res = get_stock_news(ticker)
        stock_news_list.append(res)

    for item in stock_news_list:
        df = pl.DataFrame(item)
        df2 = df.filter(
            pl.col("providerPublishTime").is_between(start_date, end_date),
        )
        df2.write_parquet('data/esg-stock-news.parquet')


# def get_top100():
#     df1 = pd.read_csv('2024-Global-100-full-dataset.csv')
#     top_100_2024 = df1[' Name']
#
#     return top_100_2024


# def match_top100_to_ticker_symbol():
