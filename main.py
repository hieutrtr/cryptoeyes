from coinmarketcap import Market
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
coinmarketcap = Market()
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']

for coin in coins:
    print 'marketcap.' + coin
    data = coinmarketcap.ticker(coin)
    print data
    producer.send('marketcap.' + coin, bytes(data[0]))
