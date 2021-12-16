import datetime
import sqlite3
import pandas as pd
import yfinance as yf
from django.core.management.base import BaseCommand
import numpy as np
from matplotlib import pyplot as plt

from ...models import CryptoDataset, Purchase, Wallet
import yfinance as yf
import matplotlib.dates as mpl_dates



class Command(BaseCommand):
    help = "A command to add data from dataframe to the database"

    def handle(self, *args, **options):
        conn = sqlite3.connect("db.sqlite3")
        df = pd.read_sql_query("select * from RuntimeGraphs_cryptodataset;", conn)
        df_date = df.copy()
        df_date['date'] = pd.to_datetime(df_date['date'])
        df_date.set_index('date', drop=True, inplace=True)

        def SMA(data, period=24, column='close'):
            return data[column].rolling(window=period).mean()

        def strategy(df):
            buy = []
            sell = []
            flag = 0
            buy_price = 0

            for i in range(0, len(df)):
                if df['SMA24'][i] > df['close'][i] and flag == 0:
                    buy.append(df['close'][i])
                    sell.append(np.nan)
                    buy_price = df['close'][i]
                    flag = 1
                elif df['SMA24'][i] < df['close'][i] and flag == 1 and buy_price < df['close'][i]:
                    sell.append(df['close'][i])
                    buy.append(np.nan)
                    buy_price = 0
                    flag = 0
                else:
                    sell.append(np.nan)
                    buy.append(np.nan)

            return buy, sell
        df_date = df_date.tail(168)
        df_date['SMA24'] = SMA(df_date)
        strat = strategy(df_date)
        df_date['Buy'] = strat[0]
        df_date['Sell'] = strat[1]
        print(df_date['Sell'].isnull().sum())
