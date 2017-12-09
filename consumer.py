from kafka import KafkaConsumer,TopicPartition
import json
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',group_id='analyzer4',auto_offset_reset='earliest')

exchanges = ['marketcap']
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']

topics = tuple(ex + '.' + coin for ex in exchanges for coin in coins)
print topics
data = {}
for coin in coins:
    data[coin] = {'bottom': 9999, 'top': 0}
consumer.subscribe(topics=topics)
for msg in consumer:
    # value = json.loads(str(msg.value))
    value = json.loads(msg.value.decode("utf-8"))
    print value['price_usd']
    # if value['price_usd'] < data[value.id]['bottom']:
    #     data[value.id]['bottom'] = value['price_usd']
    # if value['price_usd'] > data[value.id]['top']:
    #     data[value.id]['top'] = value['price_usd']
    # print (msg)

print data
