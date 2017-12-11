from coinmarketcap import Market
from kafka import KafkaProducer
import json
producer = KafkaProducer(bootstrap_servers='localhost:9092')
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']

coins = coinmarketcap.ticker(limit=100)
coinList = []
for coin in coins:
    print 'marketcap.' + coin["id"]
    print coin
    producer.send('marketcap.' + coin["id"], bytes(coin))
    coinList.append(coin["id"])
print "there're " + str(len(coins)) + " of coins are tracking."

with open("config/coin_list.json", "w") as coinListFile:
    coinListFile.write(json.dumps(coinList))
    coinListFile.close()
