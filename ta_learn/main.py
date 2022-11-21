import ccxt
import os
import time

# define type ohlvc
OHLVC =list[int | float]
DATA_DIR = './data'

binance = ccxt.binance({
    'proxies': {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890',
    },
})

# get all pair from binance
binance.load_markets()
all_pairs = binance.symbols
if all_pairs is None:
    print("Error: No pairs found")
    exit(1)
filter_pairs = filter(lambda x: x.endswith('USDT') or x.endswith('BUSD'), all_pairs)
time_interval = '1d'
# pair:str = next(filter_pairs)
for pair in filter_pairs:
    data: OHLVC = binance.fetch_ohlcv(pair, time_interval)
    pair_str = pair.replace('/','-')
    result = 0
    while result is 0:
        try:
            with open(os.path.join(DATA_DIR, f'{pair_str}.csv'), 'w') as f:
                f.write('time,open,high,low,close,volume')
                f.write('\n')
                for row in data:
                    f.write(','.join(map(str, row)) + '')  # type: ignore
                    f.write('\n')
            print(f'fetch {pair_str} {time_interval} success')
            result = 1
        except:
            print(f"Error: fetch {pair_str} failed, retrying...")
            time.sleep(1)

    

