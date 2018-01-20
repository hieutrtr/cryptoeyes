from kafka import KafkaConsumer,TopicPartition
import json
import os, time, datetime, sys

from telegram.ext import Updater,CommandHandler
from telegram import ParseMode
from telegram.error import (TelegramError, Unauthorized, BadRequest,
                            TimedOut, ChatMigrated, NetworkError)
my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
dispatcher = updater.dispatcher

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
result = {}
# maxkey = 0

def find_biggest_key(mydict):
    bg = 0
    for k,v in mydict.items():
        if k > bg:
            bg = k
    return bg

def count_order(bot, update, args):
    result = {}
    # maxkey = 0
    id_cache = []
    alert_limit = int(args[2])
    message = ""
    whale = {}
    price_count = 0
    last_price = bittrex.get_marketsummary(args[0][8:])["result"][0]["Last"]
    for bd in range(int(args[1])-1,-1,-1):
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000,max_partition_fetch_bytes=10485760,max_poll_records=100000)
        for msg in consumer:
            value = json.loads(msg.value.decode('ascii'))
            order_id = value['Id']
            if order_id in id_cache:
                continue
            id_cache.append(order_id)
            otype = value['OrderType']
            price = value['Price']
            total = value['Total']
            if args[0][8:] == 'USDT-BTC':
                price = (int(price)/1000)*1000
            else:
                price = '{0:.10f}'.format(price)
                if price_count == 0:
                    for p in price[2:]:
                        if p != '0':
                            price_count+=4
                            break
                        price_count+=1
                price = float(price[:price_count])
            if alert_limit < total:
                moment = value["TimeStamp"].split(':')[0]
                whale_value = '*{}* at {}'.format(total,value["Price"])
                if whale.get(moment, None) is None:
                    whale[moment] = {}
                    whale[moment]['BUY'] = []
                    whale[moment]['SELL'] = []
                whale[moment][otype].append(whale_value)
            if otype == 'BUY':
                result[price] = total if result.get(price) is None else total + result.get(price)
            else:
                maxkey = find_biggest_key(result)
                if maxkey == 0 or total == 0:
                    continue
                trykey = 0
                stepkey = 1
                while result[maxkey] % total == result[maxkey]:
                    total = total - result[maxkey]
                    del result[maxkey]
                    while result.get(maxkey) is None:
                        if args[0][8:] == 'USDT-BTC':
                            trykey = maxkey-stepkey*1000
                        else:
                            trykey = maxkey-stepkey*float(1)/(float(10**(price_count-2)))
                        stepkey+=1
                        if trykey == 0:
                            break
                    maxkey = trykey
                    if trykey == 0:
                        break
                if result.get(maxkey) is not None:
                    result[maxkey] = result[maxkey] - total
    message = ""
    for k in sorted(result.iterkeys()):
        message += 'at *{}* have *{}*\n'.format(k,result[k])
    bot.send_message(chat_id=update.message.chat_id, text="Your coin *{}'s* :\n{} \n *last price {}*".format(args[0],message,last_price),parse_mode=ParseMode.MARKDOWN)
    if whale != {}:
        message = ""
        for k in sorted(whale.iterkeys()):
            message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
        bot.send_message(chat_id=update.message.chat_id, text="*{}'s* Whale info:\n{}".format(args[0],message),parse_mode=ParseMode.MARKDOWN)
count_order_handler = CommandHandler('co', count_order, pass_args=True)
dispatcher.add_handler(count_order_handler)

def count_no_sell_order(bot, update, args):
    result = {}
    id_cache = []
    price_count = 0
    for bd in range(int(args[1])-1,-1,-1):
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000,max_partition_fetch_bytes=10485760,max_poll_records=100000)
        for msg in consumer:
            value = json.loads(msg.value.decode('ascii'))
            order_id = value['Id']
            if order_id in id_cache:
                continue
            id_cache.append(order_id)
            otype = value['OrderType']
            price = value['Price']
            total = value['Total']
            if args[0][8:] == 'USDT-BTC':
                price = (int(price)/1000)*1000
            else:
                price = '{0:.10f}'.format(price)
                if price_count == 0:
                    for p in price[2:]:
                        if p != '0':
                            price_count+=4
                            break
                        price_count+=1
                price = float(price[:price_count])
            if otype == 'BUY':
                result[price] = total if result.get(price) is None else total + result.get(price)
    message = ""
    for k in sorted(result.iterkeys()):
        message += 'at *{}* have *{}*\n'.format(k,result[k])
    bot.send_message(chat_id=update.message.chat_id, text="Your coin *{}'s* (BUY):\n{}".format(args[0],message),parse_mode=ParseMode.MARKDOWN)
count_order_no_sell_handler = CommandHandler('cons', count_no_sell_order, pass_args=True)
dispatcher.add_handler(count_order_no_sell_handler)

def count_sell_order(bot, update, args):
    result = {}
    id_cache = []
    price_count = 0
    for bd in range(int(args[1])-1,-1,-1):
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000,max_partition_fetch_bytes=10485760,max_poll_records=100000)
        for msg in consumer:
            value = json.loads(msg.value.decode('ascii'))
            order_id = value['Id']
            if order_id in id_cache:
                continue
            id_cache.append(order_id)
            otype = value['OrderType']
            price = value['Price']
            total = value['Total']
            if args[0][8:] == 'USDT-BTC':
                price = (int(price)/1000)*1000
            else:
                price = '{0:.10f}'.format(price)
                if price_count == 0:
                    for p in price[2:]:
                        if p != '0':
                            price_count+=4
                            break
                        price_count+=1
                price = float(price[:price_count])
            if otype == 'SELL':
                result[price] = total if result.get(price) is None else total + result.get(price)
    message = ""
    for k in sorted(result.iterkeys()):
        message += 'at *{}* have *{}*\n'.format(k,result[k])
    bot.send_message(chat_id=update.message.chat_id, text="Your coin *{}'s* (SELL):\n{}".format(args[0],message),parse_mode=ParseMode.MARKDOWN)
count_sell_order_handler = CommandHandler('cos', count_sell_order, pass_args=True)
dispatcher.add_handler(count_sell_order_handler)

def my_balance(bot, update, args):
    message = ""
    sum_btc = 0
    for ba in bittrex.get_balances()["result"]:
        if ba["Balance"] != 0:
            ticker = "N/A"
            if ba["Currency"] not in ['BTC','USDT']:
                ticker = bittrex.get_ticker("BTC-"+ba["Currency"])["result"]
                last_price = ticker["Last"] if ticker["Last"] else 0.0
                ticker = last_price*ba["Balance"]
                sum_btc += ticker
            elif ba["Currency"] == 'BTC':
                sum_btc += ba["Balance"]
            message += '*{}*:{} ({})\n'.format(ba["Currency"],ba["Balance"],ticker)

    btc_last = bittrex.get_marketsummary("USDT-BTC")["result"][0]["Last"]
    message+='*Sum BTC*: {} btc / {} usdt'.format(sum_btc,sum_btc*btc_last)
    bot.send_message(chat_id=update.message.chat_id, text=message,parse_mode=ParseMode.MARKDOWN)
my_balance_handler = CommandHandler('mb', my_balance, pass_args=True)
dispatcher.add_handler(my_balance_handler)

def my_trans(bot, update, args):
    message = "{}".format(bittrex.get_order_history()["result"])
    bot.send_message(chat_id=update.message.chat_id, text=message,parse_mode=ParseMode.MARKDOWN)
my_trans_handler = CommandHandler('mt', my_trans, pass_args=True)
dispatcher.add_handler(my_trans_handler)

def sum_market(bot, update, args):
    bot.send_message(chat_id=update.message.chat_id, text='{}'.format(bittrex.get_marketsummary(args[0])),parse_mode=ParseMode.MARKDOWN)
sum_market_handler = CommandHandler('sum', sum_market, pass_args=True)
dispatcher.add_handler(sum_market_handler)

def error_callback(bot, update, error):
    raise error
dispatcher.add_error_handler(error_callback)

updater.start_polling()
