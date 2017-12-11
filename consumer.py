from kafka import KafkaConsumer,TopicPartition
import json, ast
import os.path
consumer = KafkaConsumer(bootstrap_servers='104.196.140.94:9092',group_id='analyzer21',auto_offset_reset='earliest')

exchanges = ['marketcap']
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
with open("config/coin_list.json") as coinListFile:
    coins = json.load(coinListFile)
    coinListFile.close()

def Predict(id,bottom,top):
    if os.path.exists('config/fibo.json'):
        with open("config/fibo.json") as fiboFile:
            ids = json.load(fiboFile)
            fibo = ids.get(id,{})
            top, bottom = fibo.get('top',top), fibo.get('bottom',bottom)
            fiboFile.close()
    preDict = {}
    preDict['0'] = bottom
    preDict['100'] = top
    preDict['0.618'] = top - ((top - bottom)*0.618)
    preDict['0.382'] = top - ((top - bottom)*0.382)
    preDict['1.618'] = top + ((top - bottom)*0.618)
    preDict['1.382'] = top + ((top - bottom)*0.382)
    preDict['-0.618'] = bottom - ((top - bottom)*0.618)
    preDict['-0.382'] = bottom - ((top - bottom)*0.382)
    return preDict


topics = tuple(ex + '.' + coin for ex in exchanges for coin in coins)
print (topics)
data = {}
for coin in coins:
    data[coin] = {'bottom': 9223372036854775807.0, 'top': 0.0, 'last': 0.0, 'trend': 1}
consumer.subscribe(topics=topics)
for msg in consumer:
    value = ast.literal_eval(msg.value)
    coin = data[value['id']]
    if coin['trend'] == 1 and float(value['price_usd']) < coin['last']:
        coin['trend'] = 0
        if coin['last'] > coin['top']:
            coin['top'] = coin['last']
    elif coin['trend'] == 0 and float(value['price_usd']) > coin['last']:
        coin['trend'] = 1
        if coin['last'] < coin['bottom']:
            coin['bottom'] = coin['last']
    coin['last'] = float(value['price_usd'])
    coin['percent_change_1h'] = value['percent_change_1h']
    coin['percent_change_24h'] = value['percent_change_24h']
    coin['percent_change_7d'] = value['percent_change_7d']
    coin['24h_volume_usd'] = value['24h_volume_usd']
    coin['market_cap_usd'] = value['market_cap_usd']
    coin['prediction'] = Predict(value['id'],coin['bottom'],coin['top'])
    data[value['id']] = coin
    prices = {}
    with open("price.json") as priceFile:
        prices = json.load(priceFile)
        prices[value['id']] = coin
        priceFile.close()
    print prices
    with open("price.json", "w") as priceFile:
        priceFile.write(json.dumps(prices))
        priceFile.close()
    print (value['id'],coin)
