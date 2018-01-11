from kafka import KafkaConsumer,TopicPartition
import json, ast
import os.path,sys
import datetime,time
import requests
from prometheus_client import start_http_server, Gauge
dir_path = os.path.dirname(os.path.realpath(__file__))
rose_host = os.environ['ROSE_HOST']
start_http_server(8000)

exchanges = ['marketcap']
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
with open(dir_path+"/../config/coin_list.json") as coinListFile:
    coins = json.load(coinListFile)
    coinListFile.close()
columns = ["market_cap_usd", "price_usd", "price_btc", "percent_change_7d", "percent_change_1h", "rank"]
gauge_metrics = dict()
for col in columns:
    gauge_metrics[col] = Gauge(col, 'Marketcap gauge data', ['id', 'name'])
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
topics = tuple(ex + '.' + coin + '.' + partition for ex in exchanges for coin in coins)
print(topics)
consumer = KafkaConsumer(bootstrap_servers=rose_host,group_id='prometheus',auto_offset_reset='earliest')
consumer.subscribe(topics=topics)
for msg in consumer:
    print(msg)
    print(msg.value.decode('ascii'))
    value = json.loads(msg.value.decode('ascii'))
    payload = ''
    for col in columns:
        gauge_metrics[col].labels(value['id'].replace('-', '_'), value['name']).set(float(value.get(col,0.0)))
