from kafka import KafkaConsumer,TopicPartition
import json, ast
import os.path,sys
import datetime,time
import requests
from prometheus_client import start_http_server, Gauge
from telegram.ext import Updater,CommandHandler
from telegram import ParseMode

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path+'/..')
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1, BUY_ORDERBOOK, TICKINTERVAL_ONEMIN, TICKINTERVAL_HOUR

FIVE_MIN = 300
HAFL_HOUR = 1800
HOUR = 3600
DAY = HOUR*24

dir_path = os.path.dirname(os.path.realpath(__file__))
rose_host = os.environ['ROSE_HOST']
prom_host = os.environ['PROM_HOST']
tracking_limit = int(os.environ['TRACKING_LIMIT']) if os.environ['TRACKING_LIMIT'] is not None else 5
updater = Updater(token='464648319:AAFO8SGTukV4LHYtzpmjhbybyrwt0QQwIp8')
job = updater.job_queue

exchanges = ['marketcap']
coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
with open(dir_path+"/../config/coin_list.json") as coinListFile:
    coins = json.load(coinListFile)
    coinListFile.close()
columns = ["market_cap_usd", "price_usd", "price_btc", "percent_change_24h","percent_change_7d", "percent_change_1h", "rank", "day_volume_usd"]
gauge_metrics = dict()
for col in columns:
    gauge_metrics[col] = Gauge(col, 'Marketcap gauge data', ['id', 'name'])
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
topics = tuple(ex + '.' + coin + '.' + partition for ex in exchanges for coin in coins)
print(topics)
consumer = KafkaConsumer(bootstrap_servers=rose_host,group_id='prometheus',auto_offset_reset='earliest')

def cap_check(volume,symbol,coin_id,value):
    ts = time.time()
    url = 'http://' + prom_host+'/api/v1/query?query=market_cap_usd{id="%s"}&time=%d' % (coin_id,int(ts)-DAY/2,)
    print("cap_check_url:",url)
    r = requests.get(url=url)
    if r.status_code >= 400: r.raise_for_status()
    res = r.json()
    last_vals = []
    if len(res["data"]["result"]) > 0:
        last_vals = res["data"]["result"][0]["value"]
    for lv in last_vals:
        if type(lv) is str:
            if float(lv) < float(value):
                percent = ((float(value) - float(lv)) / float(lv)) * 100
                print("cap increase percent",percent)
                if percent > tracking_limit:
                    return '*{} ({})* is *increase {}*  percent of marketcap in 12 hours at price *{}* with 24h volume *{}*'.format(coin_id,symbol,percent,value,volume)
            elif float(lv) > float(value):
                percent = ((float(lv) - float(value)) / float(lv)) * 100
                print("cap decrease percent",percent)
                if percent > tracking_limit:
                    return '*{}* is *decrease* {} percent of marketcap in 12 hours : {}'.format(coin_id,percent,value)
    return None

def price_check(coin_id,value):
    ts = time.time()
    if coin_id == 'bitcoin':
        url = 'http://' + prom_host+'/api/v1/query?query=price_usd{id="%s"}&time=%d' % (coin_id,int(ts)-DAY/2,)
    else:
        url = 'http://' + prom_host+'/api/v1/query?query=price_btc{id="%s"}&time=%d' % (coin_id,int(ts)-DAY/2,)
    print("price_check_url",url)
    r = requests.get(url=url)
    if r.status_code >= 400: r.raise_for_status()
    res = r.json()
    last_vals = []
    if len(res["data"]["result"]) > 0:
        last_vals = res["data"]["result"][0]["value"]
    for lv in last_vals:
        if type(lv) is str:
            if float(lv) < float(value):
                percent = ((float(value) - float(lv)) / float(lv)) * 100
                print("price increase percent",percent)
                return '---*{}* is *increase* {} percent of price in 12 hours : {}'.format(coin_id,percent,value)
            elif float(lv) > float(value):
                percent = ((float(lv) - float(value)) / float(lv)) * 100
                print("price decrease percent",percent)
                return '---*{}* is *decrease* {} percent of price in 12 hours : {}'.format(coin_id,percent,value)
    return None

def cap_alert(bot, job):
    start_http_server(8000)
    consumer.subscribe(topics=topics)
    bittrex = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])
    check_dup = ""
    for msg in consumer:
        print(msg)
        print(msg.value.decode('ascii'))
        value = json.loads(msg.value.decode('ascii'))
        payload = ''
        coin_id = value['id'].replace('-', '_')
        if check_dup == coin_id:
            continue
        else:
            check_dup = coin_id
        for col in columns:
            if col == "day_volume_usd":
                metric_val = float(value.get("24h_volume_usd",0.0)) if value.get("24h_volume_usd",0.0) is not None else 0.0
            else:
                metric_val = float(value.get(col,0.0)) if value.get(col,0.0) is not None else 0.0
            if col == "percent_change_24h":
                volume = float(value.get('24h_volume_usd',0.0))
                symbol = value.get('symbol',0.0)
                cap = value.get("market_cap_usd",0.0)
                if symbol == 'BTC':
                    bitres = bittrex.get_marketsummary("USDT-BTC")
                else:
                    bitres = bittrex.get_marketsummary("BTC-"+symbol)
                if bitres.get("success") == True:
                    if metric_val > 10 or metric_val < -10:
                        btc_last = bittrex.get_marketsummary("USDT-BTC")["result"][0]["Last"]
                        message = '*{} ({})* capacity (*{}*) is changed *{}* percent in 24 hours with volume *{}*\n'.format(coin_id,symbol,cap,metric_val,volume/btc_last)
                        bot.send_message(chat_id='423404239',text=message,parse_mode=ParseMode.MARKDOWN)
                    # message = cap_check(volume,symbol,coin_id,metric_val)
                    # if message is not None:
                    #     print(message)
                    #     bot.send_message(chat_id='423404239',text=message,parse_mode=ParseMode.MARKDOWN)
                    #     if coin_id == "bitcoin":
                    #         price_usd_mess = float(value.get("price_usd",0.0)) if value.get("price_usd",0.0) is not None else 0.0
                    #         message = price_check(coin_id,price_usd_mess)
                    #     else:
                    #         price_btc_mess = float(value.get("price_btc",0.0)) if value.get("price_btc",0.0) is not None else 0.0
                    #         message = price_check(coin_id,price_btc_mess)
                    #     if message is not None:
                    #         print(message)
                    #         bot.send_message(chat_id='423404239',text=message,parse_mode=ParseMode.MARKDOWN)
            gauge_metrics[col].labels(coin_id, value['name']).set(metric_val)

job.run_repeating(cap_alert, interval=3600 * 24 * 60, first=0)
job.start()
