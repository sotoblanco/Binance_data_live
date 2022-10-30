import requests                    # for "get" request to API
import json                        # parse json into a list
import pandas as pd                # working with data frames
import datetime as dt              # working with dates
import os
import logging
import logging.handlers

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
    
    df['adj_close'] = df['close']
     
    df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df.datetime]
 
    return df

def get_latest_update(btc_historical):
    # check if the file exists
    # update the latest bar
    df_list = []
    last_datetime = start_time
    while True:
        print(last_datetime)
        new_df = get_binance_bars('BTCUSDT', '1h', last_datetime, dt.datetime.now())
        if new_df is None:
            break
        df_list.append(new_df)
        last_datetime = max(new_df.index) + dt.timedelta(0, 1)
        print(last_datetime)

    df = pd.concat([btc_historical, df_list])

    return df

# for github actions and to store the logs
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

if __name__ == "__main__":

    if not os.path.isfile('BTCUSDT_historical_1h.parquet'):
        print ("File not exist")
        start_time = dt.datetime(2019, 1, 1)
        btc_historical = pd.DataFrame()

    else:
        print ("File exist")
        # read the data previously store in your directory
        btc_historical = pd.read_parquet("BTCUSDT_historical_1h.parquet")
        # get the last index value
        start_time = btc_historical.index[-1]
    try:
        full_btc_data = get_latest_update(btc_historical)
        full_btc_data.to_parquet("BTCUSDT_historical_1h.parquet")

        logger.info(f'Last data at: {full_btc_data.index[-1]} UTC Time')
    except:
        logger.info("No data available")


