from kafka import KafkaConsumer,TopicPartition
import json
import os, time, datetime
from telegram.ext import Updater,CommandHandler
rose_host = os.environ['ROSE_HOST']
updater = Updater(token='464648319:AAFO8SGTukV4LHYtzpmjhbybyrwt0QQwIp8')
dispatcher = updater.dispatcher
result = {}
maxkey = 0

def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="I'm a bot, please talk to me!")
start_handler = CommandHandler('start', start)
dispatcher.add_handler(start_handler)

def count_order(bot, update, args):
    result = {}
    maxkey = 0
    id_cache = []
    alert_limit = int(args[2])
    last_price = 0
    for bd in range(int(args[1])-1,-1,-1):
        whale = {}
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000)
        for msg in consumer:
            value = json.loads(msg.value.decode('ascii'))
            order_id = value['Id']
            if order_id in id_cache:
                continue
            id_cache.append(order_id)
            otype = value['OrderType']
            price = value['Price']
            last_price = price
            total = value['Total']
            price = str(price)
            if 'e' in price:
                price = int(str(price)[:-4].replace('.','')[:2])
            else:
                price = int(str(int(str(price)[2:]))[:2])
            price = price if price > 10 else price * 10
            if alert_limit < total:
                whale[value['Price']] = '{} {} btc'.format(otype,total)
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
    bot.send_message(chat_id=update.message.chat_id, text="Your coin {} result:{} - *last price {}*".format(args[0],json.dumps(result),last_price))
    if whale != {}:
        bot.send_message(chat_id=update.message.chat_id, text="*{}'s* Whale info:{}".format(args[0],json.dumps(whale)))
count_order_handler = CommandHandler('co', count_order, pass_args=True)
dispatcher.add_handler(count_order_handler)

def count_no_sell_order(bot, update, args):
    result = {}
    id_cache = []
    for bd in range(int(args[1])-1,-1,-1):
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000)
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
            if 'e' in price:
                price = int(str(price)[:-4].replace('.','')[:2])
            else:
                price = int(str(int(str(price)[2:]))[:2])
            price = price if price > 10 else price * 10
            if otype == 'BUY':
                result[price] = total if result.get(price) is None else total + result.get(price)
    bot.send_message(chat_id=update.message.chat_id, text="Your coin {} result of BUY:{}".format(args[0],json.dumps(result)))
count_order_no_sell_handler = CommandHandler('cons', count_no_sell_order, pass_args=True)
dispatcher.add_handler(count_order_no_sell_handler)

def count_sell_order(bot, update, args):
    result = {}
    id_cache = []
    for bd in range(int(args[1])-1,-1,-1):
        backward_time = int(time.time()) - (bd * 86400)
        partition = datetime.datetime.fromtimestamp(backward_time).strftime('%Y-%m-%d')
        consumer = KafkaConsumer(args[0] + '.history.' + partition,bootstrap_servers=rose_host,auto_offset_reset='earliest',consumer_timeout_ms=5000)
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
            if 'e' in price:
                price = int(str(price)[:-4].replace('.','')[:2])
            else:
                price = int(str(int(str(price)[2:]))[:2])
            price = price if price > 10 else price * 10
            if otype == 'SELL':
                result[price] = total if result.get(price) is None else total + result.get(price)
    bot.send_message(chat_id=update.message.chat_id, text="Your coin {} result of SELL:{}".format(args[0],json.dumps(result)))
count_sell_order_handler = CommandHandler('cos', count_sell_order, pass_args=True)
dispatcher.add_handler(count_sell_order_handler)

def fibo_config(bot, update, args):
    with open("config/fibo.json") as fiboFile:
        fibo = json.load(fiboFile)
        fibo[args[0]] = {"bottom": float(args[1]), "top": float(args[2])}
        fiboFile.close()
    with open("config/fibo.json", "w") as fiboFile:
        fiboFile.write(json.dumps(fibo))
        fiboFile.close()
    bot.send_message(chat_id=update.message.chat_id, text="As you wish, My Lord !!!\n"+ args[0] + "'s fibo is " + json.dumps(fibo[args[0]]))
fibo_handler = CommandHandler('fibo', fibo_config, pass_args=True)
dispatcher.add_handler(fibo_handler)

def follow_config(bot, update, args):
    follow = []
    with open("config/follow.json") as followFile:
        follow = json.load(followFile)
        if args[0] == "more":
            follow.extend(args[1:])
        else:
            follow = args
        followFile.close()
    with open("config/follow.json", "w") as followFile:
        followFile.write(json.dumps(follow))
        followFile.close()
    bot.send_message(chat_id=update.message.chat_id, text="As you wish, My Lord !!!\n Follow list is " + json.dumps(follow))
follow_handler = CommandHandler('follow', follow_config, pass_args=True)
dispatcher.add_handler(follow_handler)

def list_data(bot, update, args):
    def listCoin():
        with open("config/coin_list.json") as listCoinFile:
            listCoin = json.load(listCoinFile)
            listCoinFile.close()
        return listCoin
    message = {
        "coins": listCoin()
    }[args[0]]
    bot.send_message(chat_id=update.message.chat_id, text="Your list of coins is below, My Lord !!!\n" + json.dumps(message))
list_data_handler = CommandHandler('list', list_data, pass_args=True)
dispatcher.add_handler(list_data_handler)

updater.start_polling()
