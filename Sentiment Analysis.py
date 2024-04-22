import os
import sys
import pandas as pd
import numpy as np
import statsmodels.api as sm

cwd = os.getcwd()
# Find and import config file
config_path = os.getcwd()

sys.path.append(config_path)
import config

database = config.database
central_banks = config.central_banks
training_data = os.path.join(database, "Training Data")
fed_docs = config.fed_docs
ecb_docs = config.ecb_docs


url_map = pd.read_csv(os.path.join(cwd, "url_map.csv"))

for i in range(len(url_map)):
    if url_map["central bank"][i] == "fed" and url_map["document"][i] == "minutes":
        minutes = pd.read_csv(url_map["finbert_url"][i])

sp500 = pd.read_csv(
    "/Users/kylenabors/Documents/Database/Market Data/SP500/SP500 Returns Daily.csv"
)

sp500 = sp500.rename(columns={"caldt": "date", "vwretd": "sp500_return"})
sp500["date"] = pd.to_datetime(sp500["date"])
minutes = minutes[["date", "sentiment"]]
minutes = minutes.rename(columns={"sentiment": "minute_sentiment"})
minutes["date"] = pd.to_datetime(minutes["date"])

minutes = minutes.groupby("date").mean().reset_index()

filter_df = minutes.copy(deep=True)
filter_df = filter_df[["date", "minute_sentiment"]]

cycle, trend = sm.tsa.filters.hpfilter(
    filter_df["minute_sentiment"], 1600 * ((8 / 4) ** 4)
)

filter_df["minute_sentiment_cycle"] = cycle
filter_df["minute_sentiment_trend"] = trend

filter_df = filter_df[["date", "minute_sentiment_cycle"]]
minutes = minutes.drop(columns=["minute_sentiment"])
filter_df = filter_df.rename(columns={"minute_sentiment_cycle": "minute_sentiment"})
minutes = pd.merge(minutes, filter_df, on="date", how="left")
minutes = minutes.groupby("date").mean().reset_index()

event = range(len(minutes))
minutes["event"] = event
minutes = pd.merge(minutes, sp500, how="outer", on="date")

temp = minutes.copy(deep=True)
temp = temp[["event", "sp500_return"]]
temp["log_returns"] = np.log(temp["sp500_return"] + 1)
temp = temp[["event", "log_returns"]]
temp = temp.groupby("event").sum().reset_index()
minutes = minutes.groupby(minutes["event"]).mean().reset_index()

minutes = pd.merge(minutes, temp, how="left", left_on="event", right_on="event")

print(minutes.head())
