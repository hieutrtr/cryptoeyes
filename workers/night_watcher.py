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
updater = Updater(token='464648319:AAFO8SGTukV4LHYtzpmjhbybyrwt0QQwIp8')
dispatcher = updater.dispatcher
job = updater.job_queue
alert_limit = os.environ['ALERT_LIMIT']
market = os.environ['MARKET']
back_day = int(os.environ['BACK_DAY'])
watcher = os.environ['WATCHER']

def watcher(bot, job):
    result = {}
    maxkey = 0
    id_cache = []
    message = ""
    whale = {}
    last_price = bittrex.get_marketsummary(market[8:])["result"][0]["Last"]
    # for bd in range(int(back_day)-1,-1,-1)[:int(back_day)-1]:
    bot.send_message(chat_id=update.message.chat_id, text="*Watcher {}* collecting {}'s data from {}".format(watcher,market,(int(time.time()) - ((back_day - 1) * 86400)).strftime('%Y-%m-%d')),parse_mode=ParseMode.MARKDOWN)
    while True:
        backward_time = int(time.time()) - ((back_day - 1) * 86400)
        if back_day - 1 > 0:
            back_day -= 1
            consumer_timeout = 5000
        else:
            consumer_timeout = 3600000
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(market + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=consumer_timeout,max_partition_fetch_bytes=10485760,max_poll_records=100000)
        for msg in consumer:
            value = json.loads(msg.value.decode('ascii'))
            order_id = value['Id']
            if order_id in id_cache:
                continue
            id_cache.append(order_id)
            otype = value['OrderType']
            price = value['Price']
            total = value['Total']
            price = str(price)
            if market[8:] == 'USDT-BTC':
                price = int(price[:2])
            elif 'e' in price:
                price = int(str(price)[:-4].replace('.','')[:2])
            else:
                price = int(str(int(str(price)[2:]))[:2])
            price = price if price > 10 else price * 10
            if alert_limit < total:
                whale[value['Price']] = '*{} {}* at *{}*'.format('B' if otype == 'BUY' else 'S',total,value["TimeStamp"])
            if otype == 'BUY':
                if price > maxkey:
                    maxkey = price
                result[price] = total if result.get(price) is None else total + result.get(price)
            else:
                if maxkey == 0 or total == 0:
                    continue
                while result[maxkey] % total == result[maxkey]:
                    total = total - result[maxkey]
                    del result[maxkey]
                    while result.get(maxkey) is None:
                        maxkey-=1
                        if maxkey == 0:
                            break
                    if maxkey == 0:
                        break
                if result.get(maxkey) is not None:
                    result[maxkey] = result[maxkey] - total
                message = ""
                for k,v in result.items():
                    message += 'at *{}* have *{}*\n'.format(k,v)
    bot.send_message(chat_id=update.message.chat_id, text="*Watcher {}* coin *{}'s* :\n{} \n *last price {}*".format(watcher,market,message,last_price),parse_mode=ParseMode.MARKDOWN)
    if whale != {}:
        message = ""
        for k,v in whale.items():
            message += 'at {} {}\n'.format(k,v)
        bot.send_message(chat_id=update.message.chat_id, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)
job.run_repeating(watcher, interval=3600*24*365, first=0)

def error_callback(bot, update, error):
    raise error
dispatcher.add_error_handler(error_callback)

updater.start_polling()
