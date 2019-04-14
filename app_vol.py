from multiprocessing.dummy import Pool as ThreadPool
import requests, json, numpy as np
from pymongo import MongoClient


mongodump = []

def vol_run(s):
    try:
        # 1 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1m').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        one_mo = "{:.4f}".format(vol_arr.mean())


        # 3 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/3m').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        three_mo = "{:.4f}".format(vol_arr.mean())

        # 6 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/6m').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        six_mo = "{:.4f}".format(vol_arr.mean())

        # ytd
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/ytd').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        ytd = "{:.4f}".format(vol_arr.mean())

        # 1y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1y').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        one_yr = "{:.4f}".format(vol_arr.mean())

        # 2y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/2y').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        two_yr = "{:.4f}".format(vol_arr.mean())

        # 5y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/2y').text)
        vol_arr = [abs(float(r[i]["changePercent"])) for i in range(len(r))]

        vol_arr = np.array(vol_arr, dtype=np.float64)
        five_yr = "{:.4f}".format(vol_arr.mean())

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
        pool = p.map(vol_run, symbols)
        pool.close()
        pool.join()
    
    except Exception as _:
        pass
    
    try:
        db.drop_collection('vol')
        db['vol'].create_index("symbol", unique=True)
    
    except Exception as _:
        db['vol'].create_index("symbol", unique=True)
        pass

    vol = db['vol'].insert_many([m for m in mongodump])
    print(f'\n{vol.acknowledged}\n{vol.inserted_ids}\n\n')
