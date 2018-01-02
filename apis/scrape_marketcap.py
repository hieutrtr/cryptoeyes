from coinmarketcap import Market
from kafka import KafkaProducer
import json,datetime,time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
coins = coinmarketcap.ticker(limit=200)
coinList = []
for coin in coins:
    topic = 'marketcap.' + coin["id"] + '.' + partition
    print topic
    print coin
    producer.send(topic, bytes(coin))
    coinList.append(coin["id"])
print "there're " + str(len(coins)) + " of coins are tracking."

with open("config/coin_list.json", "w") as coinListFile:
    coinListFile.write(json.dumps(coinList))
    coinListFile.close()
