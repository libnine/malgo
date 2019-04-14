import os, requests, json, datetime as dt, numpy as np, operator
from multiprocessing.dummy import Pool as ThreadPool
from pymongo import MongoClient
from app_s import Spoos


class Marketcap:
    def __init__(self):
        self.mcap_data = []
        self.ma_data = []

        if not os.path.exists('spoos.csv'):
            Spoos().spoos_scrape()
        
        with open('spoos.csv', 'r') as f:
            self.spoos = f.read().split('\n')
            self.spoos.remove('symbol')
        
    # get market cap of spx holdings
    def mcap(self, symbol):
        try:
            r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{symbol}/stats').text)
            self.mcap_data.append({"symbol": symbol, "marketcap": r["marketcap"]})
        
        except json.JSONDecodeError as je:
            print(f"Passing on {symbol}. (Market Cap)")
            pass

    # get 10m moving avg of all names
    def ma(self, symbol):
        try:
            r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{symbol}/chart/1y').text)
            prices = [r[n-1]['close'] for n in range(len(r)) if dt.datetime.strftime(dt.datetime.strptime(r[n - 1]['date'], '%Y-%m-%d'), '%m') != dt.datetime.strftime(dt.datetime.strptime(r[n]['date'], '%Y-%m-%d'), '%m')]

            ten_mo = round(np.mean(prices), 2)
            self.ma_data.append({"symbol": symbol, "ten_mo_ma": ten_mo})

        except json.JSONDecodeError as je:
            print(f"Passing on {symbol}. (10 Month MA)\n{je}")
            pass
        
        except Exception as e:
            print(f"Passed on {symbol}. (10 Month MA)\n{e}")
            pass


if __name__ == '__main__':
    with open('config.json', 'r') as cfg:
        uri = json.loads(cfg.read())['brigman']

    client = MongoClient(uri)
    db = client['spoos']
    db_mc = db['marketcap']

    mc = Marketcap()

    pool_mc = ThreadPool(5)
    pool_ma = ThreadPool(5)

    p_mc = pool_mc.map(mc.mcap, mc.spoos)
    p_ma = pool_ma.map(mc.ma, mc.spoos)
    
    pool_mc.close()
    pool_mc.join()
    
    pool_ma.close()
    pool_ma.join()
    try:
        db.drop_collection('marketcap')
        db['marketcap'].create_index("symbol", unique=True)
    
    except:
        db['marketcap'].create_index("symbol", unique=True)
        pass
    
    # merge market cap & moving average data on symbol
    sorting_key = operator.itemgetter("symbol")
    dump = [i.update(j) for i, j in zip(sorted(mc.mcap_data, key=sorting_key), sorted(mc.ma_data, key=sorting_key))]

    mongodump = db['marketcap'].insert_many(mc.mcap_data)
    print(mongodump.acknowledged)

