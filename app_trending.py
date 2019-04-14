import requests, json, re


class Trend:
    def __init__(self):
        self.st_trending = []
        self.yh_trending = []

    def trending_load(self):
        del self.st_trending[:]
        del self.yh_trending[:]

        # load stocktwits
        r = requests.get('https://api.stocktwits.com/api/2/streams/trending.json')
        j = json.loads(r.text)

        for i in j['messages']:
            if not i['symbols'][0]['symbol'] in self.st_trending:
                self.st_trending.append(i['symbols'][0]['symbol'])
        
        # load yahoo
        r = requests.get('https://finance.yahoo.com/trending-tickers/')
        html = r.text
        to_parse = re.compile('{"trending_tickers":{"positions":(.+?),"name"')
        parsed_text = re.findall(to_parse, html)
        parsed_text = json.loads(parsed_text[0])

        for i in parsed_text:
            self.yh_trending.append(i['symbol'])

        
    def trending_print(self):
        self.trending_load()

        # stocktwits print
        print('\nStocktwits')
        for i in zip(*[iter(self.st_trending)]*3):
            print('\t'.join(i))

        # yahoo finance print
        print('\nYahoo Finance')
        for i in zip(*[iter(self.yh_trending)]*4):
            print('\t'.join(i))
        
        print('')

if __name__ == '__main__':
    t = Trend().trending_print()
