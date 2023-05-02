import os
import json
import time
import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer
from yahoofinancials import YahooFinancials


load_dotenv()

tech_stock_symbols = ['AAPL', 'MSFT', 'GOOGL', 'FB', 'NVDA', 'ADBE', 'CRM', 'TSM', 'PYPL', 'INTC', 'CSCO', 'ASML', 'AVGO', 'TXN', 'QCOM', 'NFLX', 'AMAT', 'AMD', 'BIDU', 'JD']

crypto_ticker_symbols = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'ADA-USD', 'DOGE-USD', 'XRP-USD', 'DOT-USD', 'UNI-USD', 'LTC-USD', 'LINK-USD', 'BCH-USD', 'THETA-USD', 'XLM-USD', 'USDT-USD', 'VET-USD', 'SOL-USD', 'MATIC-USD', 'EOS-USD', 'TRX-USD', 'FIL-USD']


producer_conf = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(producer_conf)


def get_stock_price(stocks):
    try:
        stock_list = YahooFinancials(stocks)
        current_price = stock_list.get_current_price()

        return current_price
    
    except Exception as e:
        print(f'Error getting stock price for list: {e}')
        return None


while True:
    stock_price = get_stock_price(crypto_ticker_symbols)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for stock, price in stock_price.items():
        if price == 'None':
            pass
        else:
            if price is not None:
                message = f"{stock},{price},{current_time}".encode()
                try:
                    producer.produce('stock_prices', message)
                    producer.flush()
                except Exception as e:
                    print(f'Error producing message for {stock}: {e}')
        time.sleep(5)