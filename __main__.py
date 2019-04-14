from pymongo import MongoClient
from bson import Decimal128, json_util
import json, requests


if __name__ == '__main__':
    # connect to mongo
    with open('config.json', 'r') as cfg:
        uri = json.loads(cfg.read())['brigman']

    client = MongoClient(uri)
    db = client['spoos']

    # spoos in abc order asc
    spoos_asc = db['symbols'].find({}, {"_id": 0}).sort("symbol", 1)
    print(json.dumps([s for s in spoos_asc], indent=4))
    
    # # spoos sorted by market cap desc
    # spoos_mc_desc = db['marketcap'].find({}, {"_id": 0}).sort("marketcap", -1).limit(25)
    # print(json.dumps([c for c in spoos_mc_desc], indent=4))

    # # average daily return
    # spoos_avg_dr = db['dailyreturns'].find({}, {"_id": 0}).sort("six_month", -1).limit(10)
    # print(json.dumps([d for d in spoos_avg_dr], indent=4))

    # # spoos filtered by names above 10mo ma
    # ten_mo = [t for t in  spoos_mc_desc]
    # above_ten_mo = []
    # for t in ten_mo:
    #     current = json.loads(requests.get(f'https://api.iextrading.com/1.0/stock/{t["symbol"]}/quote').text)['latestPrice']

    #     try:
    #         if float(current) > t['ten_mo_ma']:
    #             above_ten_mo.append('{} ${} - 10 Month Moving Average: ${}'.format(t['symbol'], float(current), t['ten_mo_ma']))
        
    #     except:
    #         pass

    # print(json.dumps([a for a in above_ten_mo], indent=4))

    # # spoos ranked by vol weighted rel strength
    # pipeline = [
    #     {
    #         "$unwind": "$dailyreturns",
    #         "$unwind": {
    #             "path": "$dailyreturns",
    #             "preserveNullAndEmptyArrays": True
    #         }
    #     },
    #     {
    #         "$lookup":
    #         {
    #             "from": "dailyreturns",
    #             "localField": "symbol",
    #             "foreignField": "symbol",
    #             "as": "dailyreturns"
    #         }
    #     },
    #     {
    #         "$unwind": "$dailyreturns"
    #     },
    #     {
    #         "$project": {
    #             "symbol": 1,
    #             "weighted_avg": { "$divide": [ { "$convert": { "input": "$dailyreturns.ytd", "to": "decimal" }}, { "$convert": { "input": "$ytd", "to": "decimal" }}] }
    #         }
    #     },
    #     {
    #         "$project": {
    #             "symbol": 1,
    #             "weighted_avg": 1
    #         }
    #     },
    #     {
    #         "$out": "weighted"
    #     }
    # ]

    # db['weighted'].create_index("symbol", unique=True)
    # vol_agg = db['vol'].aggregate(pipeline)
    # weighted = db['weighted'].find({}, {"_id": 0}).sort("weighted_avg", -1).limit(25)

    # print(json.dumps([w for w in weighted], indent=4, default=json_util.default))