from pusher import Pusher
from django.shortcuts import render, redirect
import requests
import json
from datetime import datetime
import time
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt_sal
from datetime import date, timedelta
import requests, json, atexit, time, plotly, plotly.graph_objs as go
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger





pusher = Pusher(
    app_id = "1314831",
    key = "9679e7e81e540fc2574f",
    secret = "02359172c078f068c8a2",
    cluster = "us3",
    ssl=True
)
times = []
currencies = ["BTC"]
prices = {"BTC": []}



def bitcoinrealtime(request):
    return render(request, 'bitcoinrealtime.html', {})


def BTC_data():
    current_prices = {}
    for currency in currencies:
        current_prices[currency] = []

    times.append(time.strftime('%H:%M:%S'))

    api_url = "https://min-api.cryptocompare.com/data/pricemulti?fsyms={}&tsyms=USD".format(",".join(currencies))
    response = json.loads(requests.get(api_url).content)

    for currency in currencies:
        price = response[currency]['USD']
        current_prices[currency] = price
        prices[currency].append(price)
        
    graph_data = [go.Scatter(
        x=times,
        y=prices.get(currency),
        name="{} Prices".format(currency)
        ) for currency in currencies]

    bar_chart_data = [go.Bar(
        x=currencies,
        y=list(current_prices.values())
        )]

    data = {
        'graph': json.dumps(list(graph_data), cls=plotly.utils.PlotlyJSONEncoder),
        'bar_chart': json.dumps(list(bar_chart_data), cls=plotly.utils.PlotlyJSONEncoder)
    }

 
    pusher.trigger("crypto", "data-updated", data)
    
    









scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(
    func=BTC_data,
    trigger=IntervalTrigger(seconds=10),
    id='prices_retrieval_job',
    name='Retrieve prices every 10 seconds',
    replace_existing=True)

# atexit.register(lambda: scheduler.shutdown())