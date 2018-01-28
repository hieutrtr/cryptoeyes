from kafka import KafkaConsumer,TopicPartition
import json
import os, time, datetime, sys
from telegram.ext import Updater,CommandHandler
from telegram import ParseMode
from telegram.error import (TelegramError, Unauthorized, BadRequest,
                            TimedOut, ChatMigrated, NetworkError)
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path+'/..')
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1, BUY_ORDERBOOK, TICKINTERVAL_ONEMIN, TICKINTERVAL_HOUR

bittrex = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])
rose_host = os.environ['ROSE_HOST']
my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
alert_limit = int(os.environ['ALERT_LIMIT'])
level_range = int(os.environ['LEVEL_RANGE'])
market = os.environ['MARKET']
back_day = int(os.environ['BACK_DAY'])
dispatcher = updater.dispatcher
job = updater.job_queue
price_count = 0
whale = {}

def initPartitions(back_day):
    partitions = []
    for i in range(back_day):
        backward_time = int(time.time()) - ((back_day - i) * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        partitions.append(partition)
    return partitions

def createKafkaConsumer(partition):
    consumer = KafkaConsumer(market + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000,max_partition_fetch_bytes=10485760,max_poll_records=100000)
    return consumer

def flatPrice(market, price):
    if market[8:] == 'USDT-BTC':
        price = (int(price)/1000)*1000
    else:
        global price_count
        price = '{0:.10f}'.format(price)
        if price_count == 0:
            for p in price[2:]:
                if p != '0':
                    price_count+=4
                    break
                price_count+=1
        price = float(price[:price_count])
    return price

def send_message(messages,bot):
    for mess in messages:
        bot.send_message(chat_id=my_chatid, text=mess,parse_mode=ParseMode.MARKDOWN)

def whale_proceed(timestamp,price):
    if alert_limit < total:
        moment = timestamp.split(':')[0]
        whale_value = '*{}* at {}'.format(total,price)
        if whale.get(moment, None) is None:
            whale[moment] = {}
            whale[moment]['BUY'] = []
            whale[moment]['SELL'] = []
        whale[moment][otype].append(whale_value)


def proceed(market,value):
    try:
        messages = []
        otype = value['OrderType']
        price = value['Price']
        total = value['Total']
        timestamp = value["TimeStamp"]
        flat_price = flatPrice(price)
        whale_proceed(timestamp,price)
        wall_proceed()
        leve_proceed()
    except Exception as e:
        print(e)

def watcher(bot, job):
    partitions = initPartitions(back_day)
    for partition in partitions:
        consumer = createKafkaConsumer(rose_host,market,partition)
        id_cache = []
        for msg in consumer:
            try:
                value = json.loads(msg.value.decode('ascii'))
                order_id = value['Id']
                if order_id in id_cache:
                    continue
                id_cache.append(order_id)
                message = proceed(market,value)
                send_message(message)
            except Exception as e:
                print(e)
    while(True):
        partition = todayPartition()
        consumer = createKafkaConsumer(rose_host,market,partition)
        for msg in consumer:
            try:
                value = json.loads(msg.value.decode('ascii'))
                order_id = value['Id']
                if order_id in id_cache:
                    continue
                id_cache.append(order_id)
                proceed(market,value)
            except Exception as e:
                print(e)



job.run_repeating(watcher, interval=3600*24*365, first=0)
job.start()
