from pandas import read_csv, DataFrame
from config import DATA_DIR, BACKTEST_DIR

N: int = 1

df_btc: DataFrame = read_csv(f"{DATA_DIR}/BTC-USDT.csv")
df_eth: DataFrame = read_csv(f"{DATA_DIR}/ETH-USDT.csv")

df_btc["last_n_days_change"] = df_btc["close"].pct_change(N)
df_eth["last_n_days_change"] = df_eth["close"].pct_change(N)

df: DataFrame = df_btc.join(df_eth, lsuffix="_btc", rsuffix="_eth")
# create new column naming select if btc's pct_change is greater than eth's show 'btc', else show 'eth'
df["select"] = df.apply(
    lambda x: "BTC" if x["last_n_days_change_btc"] > x["last_n_days_change_eth"] else "ETH", axis=1
)
# remain time and select
df = df[["time_btc", "select"]]  # type: ignore
# rename time_btc to time
df.rename(columns={"time_btc": "time"}, inplace=True)
# save to csv
df.to_csv(f"{BACKTEST_DIR}/BTC-ETH.csv", index=False)
