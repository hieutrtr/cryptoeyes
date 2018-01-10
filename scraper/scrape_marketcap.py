from coinmarketcap import Market
from kafka import KafkaProducer
import json,datetime,time
import os
rose_host = os.environ['ROSE_HOST']
producer = KafkaProducer(bootstrap_servers=rose_host)
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
coins = coinmarketcap.ticker(limit=200)
coinList = []
for coin in coins:
    topic = 'marketcap.' + coin["id"] + '.' + partition
    # producer.send(topic, bytes(coin))
    print(topic,coin)
    coinList.append(coin["id"])

print("there're " + str(len(coinList)) + " of coins are tracking.")
print(coinList)

with open("config/coin_list.json", "w") as coinListFile:
    coinListFile.write(json.dumps(coinList))
    coinListFile.close()
