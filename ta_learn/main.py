import ccxt
import os
from datetime import datetime, timezone, timedelta
import time

# define type ohlvc
OHLVC = list[int | float]
DATA_DIR = "./data"


def utc_offset(offset):
    return timezone(timedelta(seconds=offset))


binance = ccxt.binance(
    {
        "proxies": {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        },
    }
)

# get all pair from binance
binance.load_markets()
all_pairs = binance.symbols
if all_pairs is None:
    print("Error: No pairs found")
    exit(1)
filter_pairs = filter(lambda x: x.endswith("USDT") or x.endswith("BUSD"), all_pairs)
time_interval = "1d"
# pair:str = next(filter_pairs)
for pair in filter_pairs:
    data: list[OHLVC] = binance.fetch_ohlcv(pair, time_interval)
    pair_str: str = pair.replace("/", "-")
    result = False
    while not result:
        try:
            with open(os.path.join(DATA_DIR, f"{pair_str}.csv"), "w") as f:
                f.write("time,open,high,low,close,volume")
                f.write("\n")
                for row in data:
                    readable_time = datetime.fromtimestamp(
                        row[0] / 1000, tz=utc_offset(8 * 60 * 60)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    line = (
                        f"{readable_time},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}"
                    )
                    f.write(line)
                    f.write("\n")
            print(f"fetch {pair_str} {time_interval} success")
            result = True
        except Exception as e:
            print(f"Error: fetch {pair_str} failed, error is {e}, retrying...")
            time.sleep(1)
