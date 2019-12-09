import pandas as pd
import json

res = []
house = pd.read_csv('communitites.csv', encoding='utf-8')
loc = pd.read_csv('locations.csv', encoding='utf-8')['location']
prices = house['unitprice']
community = []

for i in range(len(loc)):
    if loc[i] != '0, 0':
        data = {'lng': loc[i].split(',')[0].strip(),
                'lat': loc[i].split(',')[1].strip(), 'count': str(int(prices[i]) // 100)}
        res.append(data)
    i += 1
print(len(res))
js = json.dumps(res)
with open('hotmap.json', 'w', encoding='utf-8') as f:
    f.write(js)
