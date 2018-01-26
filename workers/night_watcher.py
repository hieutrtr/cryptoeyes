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
dispatcher = updater.dispatcher
job = updater.job_queue

def find_biggest_key(mydict):
    bg = 0
    for k,v in mydict.items():
        if k > bg:
            bg = k
    return bg

# def send_message(bot,result,whale):
#     if back_day - 1 == 0:
#         message = ""
#         for k in sorted(result.iterkeys()):
#             message += 'at *{}* have *{}*\n'.format(k,result[k])
#         bot.send_message(chat_id=my_chatid, text="*Watcher {}* the wall at {} was broken\n{}".format(market,maxkey,message),parse_mode=ParseMode.MARKDOWN)
#         if whale != {}:
#             message = ""
#             for k in sorted(whale.iterkeys()):
#                 message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
#             bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)

def watcher(bot, job):
    alert_limit = int(os.environ['ALERT_LIMIT'])
    level_range = int(os.environ['LEVEL_RANGE'])
    market = os.environ['MARKET']
    back_day = int(os.environ['BACK_DAY'])
    result = {}
    # maxkey = 0
    message = ""
    last_price = bittrex.get_marketsummary(market[8:])["result"][0]["Last"]
    wall_price = 10 # btc
    price_count = 0
    level = 0
    sum_total = 0.0
    prev_sum_total = 0.0
    # for bd in range(int(back_day)-1,-1,-1)[:int(back_day)-1]:
    partition = datetime.datetime.fromtimestamp(int(time.time()) - ((back_day - 1) * 86400)).strftime('%Y-%m-%d')
    bot.send_message(chat_id=my_chatid, text="*Watcher {}* collecting data from {}".format(market,partition),parse_mode=ParseMode.MARKDOWN)
    while True:
        whale = {}
        id_cache = []
        backward_time = int(time.time()) - ((back_day - 1) * 86400)
        if back_day - 1 > 0:
            back_day -= 1
            consumer_timeout = 5000
        else:
            consumer_timeout = 1800000
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(market + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=consumer_timeout,max_partition_fetch_bytes=10485760,max_poll_records=100000)
        for msg in consumer:
            try:
                value = json.loads(msg.value.decode('ascii'))
                order_id = value['Id']
                if order_id in id_cache:
                    continue
                id_cache.append(order_id)
                otype = value['OrderType']
                price = value['Price']
                total = value['Total']
                if market[8:] == 'USDT-BTC':
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
                    sum_total += total
                    if result.get(price) is None:
                        result[price] = total
                        if back_day - 1 == 0:
                            sum_total_change = sum_total - prev_sum_total
                            sum_total_change *= -1 if sum_total_change < 0 else 1
                            if sum_total_change > level_range/5:
                                message = ""
                                for k in sorted(result.iterkeys()):
                                    message += 'at *{}* have *{}*\n'.format(k,result[k])
                                bot.send_message(chat_id=my_chatid, text="*Watcher {}* new wall is building up at {}\n{}".format(market,price,message),parse_mode=ParseMode.MARKDOWN)
                                if whale != {}:
                                    message = ""
                                    for k in sorted(whale.iterkeys()):
                                        message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
                                    bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)
                    else:
                        result[price] = total + result.get(price)
                else:
                    sum_total -= total
                    maxkey = find_biggest_key(result)
                    if maxkey == 0 or total == 0:
                        continue
                    trykey = 0
                    stepkey = 1
                    while result[maxkey] % total == result[maxkey]:
                        total = total - result[maxkey]
                        del result[maxkey]
                        if back_day - 1 == 0:
                            sum_total_change = sum_total - prev_sum_total
                            sum_total_change *= -1 if sum_total_change < 0 else 1
                            if sum_total_change > level_range/5:
                                message = ""
                                for k in sorted(result.iterkeys()):
                                    message += 'at *{}* have *{}*\n'.format(k,result[k])
                                bot.send_message(chat_id=my_chatid, text="*Watcher {}* the wall at {} was broken\n{}".format(market,maxkey,message),parse_mode=ParseMode.MARKDOWN)
                                if whale != {}:
                                    message = ""
                                    for k in sorted(whale.iterkeys()):
                                        message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
                                    bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)
                        while result.get(maxkey) is None:
                            if market[8:] == 'USDT-BTC':
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
                new_level = int(sum_total) / level_range
                if back_day - 1 == 0:
                    sum_total_change = sum_total - prev_sum_total
                    sum_total_change *= -1 if sum_total_change < 0 else 1
                    if sum_total_change > level_range/5:
                        if new_level > level:
                            message = ""
                            for k in sorted(result.iterkeys()):
                                message += 'at *{}* have *{}*\n'.format(k,result[k])
                            last_price = bittrex.get_marketsummary(market[8:])["result"][0]["Last"]
                            bot.send_message(chat_id=my_chatid, text="*Watcher {}* the wall level *up* {} of {}\n{}\nLast price: {}".format(market,new_level,level_range,message,last_price),parse_mode=ParseMode.MARKDOWN)
                            if whale != {}:
                                message = ""
                                for k in sorted(whale.iterkeys()):
                                    message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
                                bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)
                        elif new_level < level:
                            message = ""
                            for k in sorted(result.iterkeys()):
                                message += 'at *{}* have *{}*\n'.format(k,result[k])
                            last_price = bittrex.get_marketsummary(market[8:])["result"][0]["Last"]
                            bot.send_message(chat_id=my_chatid, text="*Watcher {}* the wall level *down* {} of {}\n{}\nLast price: {}".format(market,new_level,level_range,message,last_price),parse_mode=ParseMode.MARKDOWN)
                            if whale != {}:
                                message = ""
                                for k in sorted(whale.iterkeys()):
                                    message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
                                bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)

                if sum_total_change > level_range/5:
                    prev_sum_total = sum_total
                level = new_level
            except Exception as e:
                print(e)
        try:
            message = ""
            for k in sorted(result.iterkeys()):
                message += 'at *{}* have *{}*\n'.format(k,result[k])
            bot.send_message(chat_id=my_chatid, text="*Watcher {}* :\n{} \n *last price {}*".format(market,message,last_price),parse_mode=ParseMode.MARKDOWN)
            if whale != {}:
                message = ""
                for k in sorted(whale.iterkeys()):
                    message += '*{}* have\nBUY: {}\nSELL: {}\n'.format(k,', '.join(whale[k]['BUY']),', '.join(whale[k]['SELL']))
                bot.send_message(chat_id=my_chatid, text="*{}'s* Whale info:\n{}".format(market,message),parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            print(e)
job.run_repeating(watcher, interval=3600*24*365, first=0)
job.start()
