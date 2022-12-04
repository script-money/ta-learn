import os
import time
from zoneinfo import ZoneInfo
from pandas import DataFrame, read_csv
from datetime import date, datetime
import ccxt
from config import DATA_DIR, BACKTEST_DIR
from types_ import OHLVC
from matplotlib.dates import MonthLocator
import matplotlib.pyplot as plt
import numpy as np
from math import exp


def fetch_ohlvc_data_list(
    symbols: list[str],
    from_date: date,
    to_date: date = date.today(),
    interval: str = "1d",
    load_from_file: bool = True,
) -> list[DataFrame]:
    binance = ccxt.binance(
        {
            "proxies": {
                "http": "http://127.0.0.1:7890",
                "https": "http://127.0.0.1:7890",
            },
        }
    )
    ohlcv_data_list: list[DataFrame] = []
    for pair in symbols:
        # convert since to timestamp
        pair_str: str = pair.replace("-", "/")
        since: int = int(time.mktime(from_date.timetuple()) * 1000)
        to: int = int(time.mktime(to_date.timetuple()) * 1000)

        # load from local if exists and load_from_file is True
        if not load_from_file or not os.path.exists(
            os.path.join(DATA_DIR, f"{pair}.csv")
        ):
            # calculate days from to to
            days: int = (to_date - from_date).days
            data = []
            # fetch ohlcv data every 1000 days
            for i in range(0, days, 1000):
                # fetch ohlcv data
                ohlcv_data: list[OHLVC] = binance.fetch_ohlcv(
                    pair_str, interval, since + i * 86400000, 1000
                )
                data += ohlcv_data

            # download data to csv
            result = False
            while not result:
                try:
                    with open(os.path.join(DATA_DIR, f"{pair}.csv"), "w") as f:
                        f.write("time,open,high,low,close,volume")
                        f.write("\n")
                        for row in data:
                            readable_time: datetime = datetime.fromtimestamp(  # type: ignore
                                row[0] / 1000, tz=ZoneInfo("Asia/Shanghai")
                            )
                            line = f"{readable_time},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}"
                            f.write(line)
                            f.write("\n")
                        print(
                            f"fetch {pair_str} from {datetime.fromtimestamp(since / 1000)} to {datetime.fromtimestamp(to / 1000)} {interval} success"
                        )
                    result = True
                except Exception as e:
                    print(f"Error: fetch {pair_str} failed, error is {e}, retrying...")
                    time.sleep(1)

        # read data from csv
        df: DataFrame = read_csv(os.path.join(DATA_DIR, f"{pair}.csv"))
        ohlcv_data_list.append(df)

    if len(ohlcv_data_list) == 0:
        raise Exception("Error: get_ohlvc_data_list failed")
    return ohlcv_data_list


def backtest(
    ohlcv_data_list: list[DataFrame], symbols: list[str], N: int = 1, draw: bool = False
) -> None:
    if len(ohlcv_data_list) != 2:
        raise Exception("Error: apply_strategy failed, ohlcv_data_list length is not 2")
    if len(symbols) != len(ohlcv_data_list):
        raise Exception(
            "Error: apply_strategy failed, symbols length is not equal to ohlcv_data_list length"
        )
    df1, df2 = ohlcv_data_list
    df1_symbol, df2_symbol = symbols

    df1["last_n_days_change"] = np.log(df1["close"] / df1["close"].shift(N))
    df1["last_1_day_change"] = np.log(df1["close"] / df1["close"].shift(1))
    df2["last_n_days_change"] = np.log(df2["close"] / df2["close"].shift(N))
    df2["last_1_day_change"] = np.log(df2["close"] / df2["close"].shift(1))
    df: DataFrame = df1.join(df2, lsuffix=f"_{df1_symbol}", rsuffix=f"_{df2_symbol}")

    def get_signal(row):
        if (
            row[f"last_n_days_change_{df1_symbol}"]
            >= row[f"last_n_days_change_{df2_symbol}"]
            and row[f"last_n_days_change_{df1_symbol}"] > 0
        ):
            return df1_symbol
        elif (
            row[f"last_n_days_change_{df1_symbol}"]
            < row[f"last_n_days_change_{df2_symbol}"]
            and row[f"last_n_days_change_{df2_symbol}"] > 0
        ):
            return df2_symbol
        else:
            return None

    df["select"] = df.apply(get_signal, axis=1)  # type: ignore

    df["time"] = df[f"time_{df1_symbol}"]
    # remain time and select
    df: DataFrame = df[
        [
            f"time",
            f"close_{df1_symbol}",
            f"close_{df2_symbol}",
            f"last_1_day_change_{df1_symbol}",
            f"last_1_day_change_{df2_symbol}",
            "select",
            f"last_n_days_change_{df1_symbol}",
            f"last_n_days_change_{df2_symbol}",
        ]
    ]
    df["last_day_select"] = df["select"].shift(1)  # 按上一日的选择进行持仓

    df["every_day_change"] = df.apply(
        lambda row: row[f"last_1_day_change_{str(row['last_day_select'])}"]
        if row["last_day_select"] is not None
        else 0,
        axis=1,
    )
    df["cum_profile"] = df["every_day_change"].cumsum()
    df[f"cum_profile_{df1_symbol}"] = (
        df[f"last_1_day_change_{df1_symbol}"].fillna(0).cumsum()
    )
    df[f"cum_profile_{df2_symbol}"] = (
        df[f"last_1_day_change_{df2_symbol}"].fillna(0).cumsum()
    )
    df["previous_max"] = df["cum_profile"].cummax()
    df["drawdown"] = df.apply(
        lambda row: (exp(row["cum_profile"]) - exp(row["previous_max"]))
        / exp(row["previous_max"]),
        axis=1,
    )
    max_loss, max_loss_at = (
        df["drawdown"].min(),
        df["drawdown"].idxmin(),
    )

    df["date_str"] = df["time"].apply(lambda x: x.split(" ")[0])
    if draw:
        plt.plot(df["date_str"], df["cum_profile"], color="green", label=f"N={N}")
        # draw an arrow to show max loss
        plt.annotate(
            f"max loss: {max_loss:.2%}",
            xy=(df["date_str"][max_loss_at], df["cum_profile"][max_loss_at]),
            xytext=(df["date_str"][max_loss_at], df["cum_profile"][max_loss_at] - 0.1),
            arrowprops=dict(facecolor="red", shrink=0.05),
        )
        plt.plot(
            df["date_str"],
            df[f"cum_profile_{df1_symbol}"],
            color="orange",
            label=df1_symbol,
        )
        plt.plot(
            df["date_str"],
            df[f"cum_profile_{df2_symbol}"],
            color="blue",
            label=df2_symbol,
        )
        plt.legend(loc="upper left")
        ax = plt.gca()
        ax.grid(True)
        ax.xaxis.set_major_locator(
            MonthLocator(interval=3, tz=ZoneInfo("Asia/Shanghai"))
        )
        # set rotate for axis x
        for label in ax.get_xticklabels(which="major"):
            label.set(rotation=30, horizontalalignment="right")
        # set graph title
        plt.title(
            f"Hold {df1_symbol}/{df2_symbol}/USDT according to last {N} days change"
        )
        plt.xlabel("Date")
        plt.ylabel("Log Return")
        plt.show()

    df = df[
        [
            "time",
            f"close_{df1_symbol}",
            f"close_{df2_symbol}",
            f"last_n_days_change_{df1_symbol}",
            f"last_n_days_change_{df2_symbol}",
            "select",
            "cum_profile",
        ]
    ]
    # save to csv
    df.to_csv(f"{BACKTEST_DIR}/{df1_symbol}-{df2_symbol}.csv", index=False)
    return df.tail(-1)[["cum_profile"]].values[-1][0]


if __name__ == "__main__":
    ohlcv_data_list: list[DataFrame] = fetch_ohlvc_data_list(
        ["BTC-USDT", "ETH-USDT"], from_date=date(2019, 1, 1), load_from_file=True
    )

    n_2_profit = {}
    for n in range(1, 20):
        profit = backtest(ohlcv_data_list, ["BTC", "ETH"], N=n)
        n_2_profit[n] = profit
    max_profit = max(n_2_profit.values())
    N = [k for k, v in n_2_profit.items() if v == max_profit]
    print(f"根据N天变化买BTC或ETH或空仓策略最大盈利: {exp(max_profit):.2%}, {N=}")

    backtest(ohlcv_data_list, ["BTC", "ETH"], N=14, draw=True)
