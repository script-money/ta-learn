import ccxt
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from config import DATA_DIR, PAIRS_TO_FETCH

# define type ohlvc
OHLVC = list[int | float]

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
all_pairs: list[str] | None = binance.symbols
if all_pairs is None:
    print("Error: No pairs found")
    exit(1)
if len(PAIRS_TO_FETCH) != 0:
    filter_pairs = filter(lambda x: x in PAIRS_TO_FETCH, all_pairs)
else:
    filter_pairs = filter(lambda x: x.endswith("USDT") or x.endswith("BUSD"), all_pairs)

time_interval = "1d"

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
                    readable_time = datetime.fromtimestamp(  # type: ignore
                        row[0] / 1000, tz=ZoneInfo("Asia/Shanghai")
                    )
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
