# run in a cron job after get_binance with github actions
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import datetime 

# %%
df = pd.read_csv("BTCUSDT_historical.csv", index_col="Unnamed: 0", parse_dates=True)

df_day = df[(df.index.time >= datetime.time(9))
   & (df.index.time <= datetime.time(16))]

df_night = df[(df.index.time <= datetime.time(8)) |
              (df.index.time >= datetime.time(17))]

# %%
df_day["date"] = pd.to_datetime(df_day.index.date)

# %%
df_night["date"] = pd.to_datetime(df_night.index.date)

df_night["date"] = df_night.index - pd.to_timedelta(9, unit="h")

# %%
df_night.index = df_night.date

# %%
ohlc_dict = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum"}

day_sum = df_day.resample('1d').apply(ohlc_dict).dropna()
night_sum = df_night.resample('1d').apply(ohlc_dict).dropna()

# %%
#night_sum = night_sum.shift().dropna()

# %%
day_night = day_sum.merge(night_sum, left_index=True, right_index=True, suffixes=('_day', '_night'))


# %%
features = ["high_day", "low_day", "high_night", "low_night"]
categorical_features = []
numerical_features = []
for i in features:
    day_night[f"p{i}_touch"] = np.where(
        (day_night[i].shift() <= day_night["high_day"]) & (day_night[i].shift() >= day_night["low_day"]), 1, 0)
        
    day_night[f"ret_distance_p{i}_open"] = (day_night["open_day"] - day_night[i].shift())/day_night[i].shift()
    categorical_features.append(f"p{i}_touch")
    numerical_features.append(f"ret_distance_p{i}_open")

export_data = day_night[categorical_features + numerical_features]
export_data.to_csv("BTC_feature_data.csv")


