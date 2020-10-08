import json

COUNTRIES_FILE = "cc.txt"
OUTPUT = 'cc.json'
COUNTRY = 'country'
CAPITAL = 'capital'

with open(COUNTRIES_FILE, 'r') as f:
    lines = f.readlines()
    cc = []
    for line in lines:
        split = line.replace('\n', '').split(',')
        split = list(map(str.capitalize, split))
        country = split[0].replace(' ', '_')
        capitals = split[1]
        cc.append({COUNTRY: country, CAPITAL: capitals})


with open(OUTPUT, 'w', encoding="utf8") as f:
    json.dump(cc, f, indent=4, ensure_ascii=False)


        
