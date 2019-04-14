import requests, json, re, itertools, pandas as pd
from multiprocessing.dummy import Pool as ThreadPool


pool = ThreadPool(4)

# place multiline re compiling before class
short = re.compile("""
		<font size="-1">
		Short Interest: (.+?)
		</font>
""", re.MULTILINE)

implied = re.compile("""
		<td>
			Implied Move Weekly:
			
		</td>
		<td>
			(.+?)
		&nbsp;
""", re.MULTILINE)


class Er:
    def __init__(self):
        self.er_list = []
        global implied_move
        global short_int


    def er_print(self):
        if not len(self.er_list) == 0:
            df = pd.DataFrame(self.er_list, columns=['symbol', 'er', 'short_int', 'implied_move'])
            df = df.rename(columns={'symbol': 'Symbol', 'er': 'Earnings Date', 'short_int': 'Short Interest', 'implied_move': 'Implied Move'})
            df = df.sort_values('Symbol')
            
            print("")
            print(df.to_string(index=False))
            print("")

    def er_scrape(self, s):
        try:
            # earnings date
            url = 'https://finviz.com/quote.ashx?t={}'.format(s)
            r = requests.get(url)

            html = r.text
            earnings = re.compile('Earnings</td><td width="8%" class="snapshot-td2" align="left"><b>(.+?)</b>')
            earnings = re.findall(earnings, html)


            # implied move & short interest
            url = 'https://www.optionslam.com/earnings/stocks/{}'.format(s.upper())
            r = requests.get(url)        

            html = r.text
            short_int = re.findall(short, html)
            implied_move = re.findall(implied, html)
            
            if not implied_move:
                implied_move.append("N/a")
            
            if short_int[0] == "None" or short_int[0] == None:
                self.er_list.append({"symbol": "{}".format(s.upper()), "er": "{}".format(earnings[0]), "short_int": "N/a", "implied_move": "{}".format(implied_move[0])})
            else:
                self.er_list.append({"symbol": "{}".format(s.upper()), "er": "{}".format(earnings[0]), "short_int": "{}%".format(short_int[0]), "implied_move": "{}".format(implied_move[0])})
        
        except:
            self.er_list.append({"symbol": "{}".format(s.upper()), "er": "N/a", "short_int": "N/a", "implied_move": "N/a"})


    def er_multi(self, *s):
        if type(s) == tuple:
            s = list(*s)
        
        if len(s) <= 1:
            self.er_scrape(s[0])
            self.er_print()

        else:
            p = pool.map(self.er_scrape, s)
            pool.close()
            pool.join()
            self.er_print()

if __name__ == '__main__':
    with open('er.json', 'r') as f:
        tickers = json.loads(f.read())['symbols']

    s = Er().er_multi(tickers)
    