from datetime import datetime
import yfinance as yf
from django.core.management.base import BaseCommand
from ...models import CryptoDataset, Wallet, Purchase
import time
import pandas as pd
import sqlite3

class Command(BaseCommand):
    help = "A command to add data from dataframe to the database"

    def handle(self, *args, **options):
        while 1:
            data = yf.download(tickers='BTC-USD', period='2y', interval='1h')
            data.drop('Adj Close', axis=1, inplace=True)
            d1 = data.iloc[-1]
            fortune = CryptoDataset(date=data.index[-1], open=d1['Open'],
                                    close=d1['Close'], volume=d1['Volume'],
                                    high=d1['High'], low=d1['Low'])
            fortune.save()
            conn = sqlite3.connect("db.sqlite3")
            df = pd.read_sql_query("select * from RuntimeGraphs_cryptodataset;", conn)
            pa = (((df.close.iloc[-2] - df.close.iloc[-1]) / df.close.iloc[-1]) * 100).round(2)
            wal = Wallet.objects.get(id=2)
            btc = wal.btc
            cash = wal.wallet
            print(pa, cash, btc)
            if pa >= 2:
                if pa >= 3:
                    if (btc - (100 / data.Close.iloc[-1])) >= 0:
                        amount = (100 / data.Close.iloc[-1])
                        btc -= amount
                        cash += 100
                        Wallet.objects.update(id=2,wallet=cash, btc=btc)
                        print("purchased")
            elif pa <= -3:
                if cash >= 100:
                    cash -= 100
                    amount = (1 / data.Close.iloc[-1]) * 100
                    btc += amount
                    Wallet.objects.update(id=2,wallet=cash, btc=btc)
                    print("sold")

            time.sleep(3600)
