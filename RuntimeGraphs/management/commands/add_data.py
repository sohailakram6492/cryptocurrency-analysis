import sys
sys.path.append("...")

import yfinance as yf
from django.core.management.base import BaseCommand
from RuntimeGraphs.models import CryptoDataset, ETHDataset


class Command(BaseCommand):
    help = "A command to add data from dataframe to the database"

    def handle(self, *args, **options):
        data = yf.download(tickers='ETH-USD', period='2y', interval='1h')
        data.drop('Adj Close', axis=1, inplace=True)
        print(data.columns)



        for agency in data.itertuples():
            fortune = ETHDataset(date=getattr(agency, 'Index'), open=getattr(agency, 'Open'),
                                    close=getattr(agency, 'Close'), volume=getattr(agency, 'Volume'),
                                    high=getattr(agency, 'High'), low=getattr(agency, 'Low'))
            fortune.save()
            
            
    # def handle2(self, *args, **options):
    #     data = yf.download(tickers='ETH-USD', period='2y', interval='1h')
    #     data.drop('Adj Close', axis=1, inplace=True)
    #     print(data.columns)



    #     for agency in data.itertuples():
    #         fortune = ETHDataset(date=getattr(agency, 'Index'), open=getattr(agency, 'Open'),
    #                                 close=getattr(agency, 'Close'), volume=getattr(agency, 'Volume'),
    #                                 high=getattr(agency, 'High'), low=getattr(agency, 'Low'))
    #         fortune.save()
    #     print(data.columns)
