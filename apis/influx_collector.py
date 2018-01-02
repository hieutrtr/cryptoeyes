from kafka import KafkaConsumer,TopicPartition
import json, ast
import os.path,sys
import datetime,time
import requests


exchanges = ['marketcap']
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
with open("config/coin_list.json") as coinListFile:
    coins = json.load(coinListFile)
    coinListFile.close()
partition = sys.argv[1] if sys.argv[1] else datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
topics = tuple(ex + '.' + coin + '.' + partition for ex in exchanges for coin in coins)
print topics
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',group_id='influx19',auto_offset_reset='earliest')
consumer.subscribe(topics=topics)
for msg in consumer:
    print msg
    value = ast.literal_eval(msg.value)
    value = dict((k, v) for k, v in value.iteritems() if v)
    # payload = [
    #     {
    #         "name" : value['id'].replace('-', '_'),
    #         "columns" : ["time","market_cap_usd", "price_usd", "price_btc", "24h_volume_usd", "percent_change_7d", "percent_change_1h"],
    #         "points" : [[msg.timestamp/1000, float(value.get('market_cap_usd',0.0)), float(value.get('price_usd',0.0)), float(value.get('price_btc',0.0)), float(value.get('24h_volume_usd',0.0)),float(value.get('percent_change_7d',0.0)),float(value.get('percent_change_1h',0.0))]]
    #     }
    # ]
    columns = ["market_cap_usd", "price_usd", "price_btc", "24h_volume_usd", "percent_change_7d", "percent_change_1h", "rank"]
    payload = ''
    for col in columns:
        payload += '{},id={} value={} {}\n'.format( col, value['id'].replace('-', '_'), float(value.get(col,0.0)), msg.timestamp*1000000 )
    print payload
    r = requests.post('http://cryptoeyes:1547896$s@35.185.14.155:8086/write?db=marketcap', data = bytes(payload))
    if r.status_code >= 400: r.raise_for_status()
