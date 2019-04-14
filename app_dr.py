from multiprocessing.dummy import Pool as ThreadPool
from pymongo import MongoClient
import requests, json


mongodump = []

def daily_return_dump(s):
    try:
        # 1 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1m').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))


        avg_dr_1m = round(((rolling - 1) * 100), 2)    

        # 3 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/3m').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))

        avg_dr_3m = round(((rolling - 1) * 100), 2)

        # 6 month
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/6m').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))
            
        avg_dr_6m = round(((rolling - 1) * 100), 2)

        # ytd
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/ytd').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))

        avg_dr_ytd = round(((rolling - 1) * 100), 2)

        # 1y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/1y').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))

        avg_dr_1y = round(((rolling - 1) * 100), 2)

        # 2y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/2y').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))

        avg_dr_2y = round(((rolling - 1) * 100), 2)
        
        # 5y
        r = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{s}/chart/5y').text)

        for i in range(len(r)):
            if i == 0:
                initial = float(r[i]["close"])
                rolling = float(r[i]["close"]) / initial
                continue

            rolling = rolling - (1 - (r[i]["close"] / r[i-1]["close"]))

        avg_dr_5y = round(((rolling - 1) * 100), 2)

        mongodump.append(f'{{"symbol": "{s}", "one_month": "{avg_dr_1m:.2f}", "three_month": "{avg_dr_3m:.2f}", "six_month": "{avg_dr_6m:.2f}", "ytd": "{avg_dr_ytd:.2f}", "one_year": "{avg_dr_1y:.2f}", "two_year": "{avg_dr_2y:.2f}", "five_year": "{avg_dr_5y:.2f}"}}')

        print(s, avg_dr_1m, avg_dr_3m)


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
        p = ThreadPool(10)
        pool = p.map(daily_return_dump, symbols)
        pool.close()
        pool.join()
    
    except:
        pass
    
    try:
        db.drop_collection('dailyreturns')
        db['dailyreturns'].create_index("symbol", unique=True)
    
    except Exception as _:
        db['dailyreturns'].create_index("symbol", unique=True)
        pass

    
    dailyreturns = db['dailyreturns'].insert_many([json.loads(m) for m in mongodump])
    print(f'\n{dailyreturns.acknowledged}\n{dailyreturns.inserted_ids}\n\n')
