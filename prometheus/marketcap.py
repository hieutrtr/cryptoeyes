from kafka import KafkaConsumer,TopicPartition
import json, ast
import os.path,sys
import datetime,time
import requests
from prometheus_client import start_http_server, Gauge
from telegram.ext import Updater,CommandHandler

FIVE_MIN = 300
HAFL_HOUR = 1800
HOUR = 3600
DAY = HOUR*24

dir_path = os.path.dirname(os.path.realpath(__file__))
rose_host = os.environ['ROSE_HOST']
prom_host = os.environ['PROM_HOST']
tracking_limit = int(os.environ['PROM_HOST']) if os.environ['PROM_HOST'] is not None else 5
updater = Updater(token='464648319:AAFO8SGTukV4LHYtzpmjhbybyrwt0QQwIp8')
job = updater.job_queue

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

def cap_check(coin_id,value):
    ts = time.time()
    r = requests.get(prom_host+'/api/v1/query?query=market_cap_usd{id="{}"}&time={}'.format(coin_id,int(ts)-FIVE_MIN))
    if r.status_code >= 400: r.raise_for_status()
    res = r.json()
    last_vals = res["data"]["result"][0]["value"]
    for lv in last_vals:
        if type[lv] is str:
            if float(lv) < float(value):
                percent = ((float(value) - float(lv)) / float(lv)) * 100
                if percent > tracking_limit:
                    return '{} is increase {} percent of marketcap in 5 minutes : {}'.format(coin_id,percent,value)
            elif float(lv) > float(value):
                percent = ((float(lv) - float(value)) / float(lv)) * 100
                if percent > tracking_limit:
                    return '{} is decrease {} percent of marketcap in 5 minutes : {}'.format(coin_id,percent,value)
    return None

def cap_alert(bot, job):
    start_http_server(8000)
    consumer.subscribe(topics=topics)
    for msg in consumer:
        print(msg)
        print(msg.value.decode('ascii'))
        value = json.loads(msg.value.decode('ascii'))
        payload = ''
        coin_id = value['id'].replace('-', '_')
        for col in columns:
            metric_val = float(value.get(col,0.0))
            if col == "market_cap_usd":
                message = cap_check(coin_id,metric_val)
                if message is not None:
                    bot.send_message(chat_id='423404239',text=message)
            gauge_metrics[col].labels(coin_id, value['name']).set(metric_val)

job.run_repeating(cap_alert, interval=300, first=0)
job.start()
