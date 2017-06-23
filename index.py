from datetime import datetime, timedelta
import time, pytz
import numpy as np
import pandas as pd
import schedule
import requests
import json

import writer

baseUrl = 'https://min-api.cryptocompare.com/data'
markets = [
    'Coinbase',
    'Poloniex'
]
coins = {
    'BTC': [ 'USD', 'ETH' ]
}

def today_ts():
    now = datetime.now(pytz.utc)
    now = now.replace(hour=23, minute=59, second=59)
    return int(now.timestamp())

def yesterday_ts():
    return today_ts() - (60 * 60 * 24)

def fetch_prices(fromSym, toSyms, ts):
    action = 'pricehistorical'
    data = {
        'fsym': fromSym,
        'tsyms': ','.join(toSyms),
        'markets': ','.join(markets),
        'ts': ts
    }
    res = requests.get('%s/%s' % (baseUrl, action), data).text
    prices = json.loads(res)[fromSym]

    return prices

def backtrace_missing_data():
    for base in coins:
        fetch = lambda ts, qs: fetch_prices(base, np.array(qs).flatten(), ts)

        ts = today_ts()
        prices = fetch(ts, coins[base])

        quotes_to_fetch = []
        complete_col = 'completed_loop'
        for currency, price in prices.items():
            file_name = '%s_%s.json' % (currency, base)
            try:
                with open(file_name, 'r') as file:
                    data = json.load(file)

                    if not data.get(complete_col):
                        quotes_to_fetch.append(currency)
                    elif str(ts) not in data['prices']:
                        y_ts = yesterday_ts()
                        price = fetch(y_ts, [currency])[currency]
                        writer.queue(file_name, { 'prices': { y_ts: price } })

            except FileNotFoundError:
                quotes_to_fetch.append(currency)

            writer.queue(file_name, { 'prices': { ts: price } })

        # Determine earliest timestamp to start at
        ts = 0
        for quote in quotes_to_fetch:
            try:
                with open('%s_%s.json' % (quote, base)) as file:
                    earliest = min(list(map(int, json.load(file)['prices'].keys())))
                    ts = earliest if earliest > ts else ts
            except FileNotFoundError:
                ts = today_ts()
                break

        while len(quotes_to_fetch) > 0:
            ts_string = datetime.fromtimestamp(ts, pytz.utc).ctime()
            print(f'{ts_string} - {base}:{",".join(quotes_to_fetch)}')
            prices = fetch(ts, quotes_to_fetch)

            for quote, price in prices.items():
                file_name = '%s_%s.json' % (quote, base)

                if price == 0:
                    writer.queue(file_name, { complete_col: True })
                    quotes_to_fetch.remove(quote)
                else:
                    writer.queue(file_name, { 'prices': { ts: price }, complete_col: False })

            # API only allows about 1 call a second
            time.sleep(1)
            # Set timestamp to the day prior
            ts = ts - (60 * 60 * 24)

        print('\nFetching complete!')

if __name__ == '__main__':
    # Begin by fetching any missing historical data
    backtrace_missing_data()

    # Start CRON to fetch data regularly
    schedule.every().hour.do(backtrace_missing_data)
    while True:
        schedule.run_pending()
        time.sleep(10)
