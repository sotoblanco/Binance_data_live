# to be schedule in a cron job with GitHub actions

import requests                    # for "get" request to API
import json                        # parse json into a list
import pandas as pd                # working with data frames
import datetime as dt              # working with dates
import os
import pytz
# import time
# from threading import Thread

# this function allow to get the data from binance on EST time

def get_binance_bars(symbol, interval, startTime, endTime):
 
    url = "https://api.binance.com/api/v3/klines"
 
    startTime = str(int(startTime.timestamp() * 1000))
    endTime = str(int(endTime.timestamp() * 1000))
    limit = '1000'
 
    req_params = {"symbol" : symbol, 'interval' : interval, 'startTime' : startTime, 'endTime' : endTime, 'limit' : limit}
 
    df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text))
 
    if (len(df.index) == 0):
        return None
     
    df = df.iloc[:, 0:6]
    df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
 
    df.open      = df.open.astype("float")
    df.high      = df.high.astype("float")
    df.low       = df.low.astype("float")
    df.close     = df.close.astype("float")
    df.volume    = df.volume.astype("float")
     
    df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df.datetime]

    # EST TIME
    df.index = df.index - pd.to_timedelta(4, unit="h")
 
    return df

if not os.path.isfile('BTCUSDT_historical_1m.parquet'):
    print ("File not exist")
    start_time = dt.datetime(2017, 6, 17)
    df = pd.DataFrame()

else:
    print ("File exist")
    # read the data previously store in your directory
    df = pd.read_parquet("BTCUSDT_historical_1m.parquet")
    # get the last index value
    start_time = df.index[-5]

now_time = dt.datetime.now(pytz.timezone('America/New_York'))

while True:
  print(start_time)
  btc = get_binance_bars("BTCUSDT", "1m", start_time, now_time)
  print(btc.index[-1])
  df = pd.concat([df, btc])
  
  if btc.index[-1].strftime("%Y-%m-%d %H:%M") == now_time.strftime("%Y-%m-%d %H:%M"):
    break
  start_time = btc.index[-1]

df.sort_index(inplace=True)
df = df[~df.index.duplicated(keep='last')]
df.to_parquet("BTCUSDT_historical_1m.parquet")
print(df.index[-1])
