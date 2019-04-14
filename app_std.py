from multiprocessing.dummy import Pool as ThreadPool
import requests, json, numpy as np
from pymongo import MongoClient


mongodump = []

def std_run(s):
    try:
        # 1 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1m').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        one_mo = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        # 3 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/3m').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        three_mo = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        # 6 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/6m').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        six_mo = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        # ytd
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/ytd').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        ytd = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        # 1y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1y').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        one_yr = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        # 2y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/2y').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        two_yr = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)
        
        # 5y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/5y').text)
        std_arr = []

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                std_arr.append(rolling - 1)
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            std_arr.append(rolling - 1)
        
        std_arr = np.array(std_arr, dtype=np.float64)
        five_yr = "{:.4f}".format(np.std(std_arr, ddof=1) * 100)

        mongodump.append({"symbol": s, "one_mo": one_mo, "three_mo": three_mo, "ytd": ytd, "one_yr": one_yr, "two_yr": two_yr, "five_yr": five_yr})


    except json.JSONDecodeError as je:
        print(f"JSON Error on {s}: {je}\n")
        pass


    except Exception as e:
        print(f"Passing on {s}\n{e}\n")
        pass
            

if __name__ == '__main__':
    with open('config.json', 'r') as cfg:
        uri = json.loads(cfg.read())['brigman']

    client = MongoClient(uri)
    db = client['spoos']
    symbols =  db['symbols'].find({}, {"_id": 0}).sort("symbol", 1)
    symbols = [mdb["symbol"] for mdb in json.loads(json.dumps([s for s in symbols]))]
    
    try:
        p = ThreadPool(5)
        pool = p.map(std_run, symbols)
        pool.close()
        pool.join()
    
    except Exception as _:
        pass
    
    try:
        db.drop_collection('stddev')
        db['stddev'].create_index("symbol", unique=True)
    
    except Exception as _:
        db['stddev'].create_index("symbol", unique=True)
        pass

    stddev = db['stddev'].insert_many([m for m in mongodump])
    print(f'\n{stddev.acknowledged}\n{stddev.inserted_ids}\n\n')
