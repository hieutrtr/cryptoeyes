from coinmarketcap import Market
from kafka import KafkaProducer
import json,datetime,time
import os
dir_path = os.path.dirname(os.path.realpath(__file__))
rose_host = os.environ['ROSE_HOST']
producer = KafkaProducer(bootstrap_servers=rose_host)
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y')
coins = coinmarketcap.ticker(limit=500)
coinList = []
for coin in coins:
    topic = 'marketcap.' + coin["id"] + '.' + partition
    producer.send(topic, json.dumps(coin).encode())
    coinList.append(coin["id"])

print("there're " + str(len(coinList)) + " of coins are tracking.")
print(coinList)

with open(dir_path+"/../config/coin_list.json", "w") as coinListFile:
    coinListFile.write(json.dumps(coinList))
    coinListFile.close()

#ROSE_HOST=35.197.152.169:9092 python scrape/scrape_marketcap.py
