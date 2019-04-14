import requests, re, json
from pymongo import MongoClient


# scrape wikipedia for all companies in the s&p 500
class Spoos:
    def __init__(self):
        self.spoos_list = []

    def spoos_scrape(self):
        r = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies').text

        nyse = re.findall('nyse.com/(.+?)/a>', r, re.S)
        nasdaq = re.findall('nasdaq.com/(.+?)/a>', r, re.S)

        self.spoos_list.extend(re.findall('>(.+?)<', ", ".join(nyse), re.S) + re.findall('>(.+?)<', ", ".join(nasdaq), re.S))

        with open('spoos.csv', 'w') as f:
            f.write('symbol\n')
            f.write('\n'.join(self.spoos_list))

        return self.spoos_list

if __name__ == '__main__':
    symbols = [{"symbol": s} for s in Spoos().spoos_scrape()]
    
    with open('config.json', 'r') as cfg:
        uri = json.loads(cfg.read())['brigman']

    client = MongoClient(uri)
    db = client['spoos']
    
    try:
        db.drop_collection('symbols')
        db['symbols'].create_index("symbol", unique=True)
    
    except:
        db['symbols'].create_index("symbol", unique=True)
        pass


    spx = db['symbols'].insert_many(symbols)
    
    try:
        db['symbols'].update_one({'symbol': 'BRK-B'}, {'$set': {'symbol': 'BRK.B'}}, upsert=True)
        db['symbols'].update_one({'symbol': 'BF-B'}, {'$set': {'symbol': 'BF.B'}}, upsert=True)

    except Exception as mongo_err:
        print(f'\nError on symbols update.\n{mongo_err}\n')
        pass

    print(f'\n{spx.acknowledged}\n{spx.inserted_ids}\n\n')
    