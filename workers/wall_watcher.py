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
my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
alert_limit = int(os.environ['ALERT_LIMIT'])
dispatcher = updater.dispatcher
job = updater.job_queue

walls_cache = {}

def send_message(bot,market,walls,otype):
    if market == 'USDT-BTC':
        alimit = alert_limit*10000
    else:
        alimit = alert_limit
    be_send = False
    last_price = bittrex.get_marketsummary(market)["result"][0]["Last"]
    message = "*{} wall - {}*\n".format(otype,market)
    for k in sorted(walls.iterkeys()):
        if walls_cache[market][otype].get(k) is None:
            if walls[k] > alimit:
                be_send = True
                message += 'at *{}* have *{}* as new\n'.format(k,walls[k])
        elif (walls[k] > walls_cache[market][otype][k] + alimit):
            if walls[k] > alimit:
                be_send = True
                message += 'at *{}* have *{}* increase *{}*\n'.format(k,walls[k],walls[k] - walls_cache[market][otype][k])
        elif walls[k] > alimit:
            message += 'at *{}* have {}\n'.format(k,walls[k])
    message += "\nLast price:{}".format(last_price)
    if be_send is True:
        walls_cache[market][otype] = walls
        bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)

def flatPrice(market,price):
    price_count = 0
    if market == 'USDT-BTC':
        price = (int(price)/100)*100
    else:
        price = '{0:.10f}'.format(price)
        if price_count == 0:
            for p in price[2:]:
                if p != '0':
                    price_count+=5
                    break
                price_count+=1
        price = float(price[:price_count])
    return price

def watcher(bot, job):
    markets = os.environ['MARKETS'].split(",")
    for market in markets:
        if walls_cache.get(market) is None:
            walls_cache[market] = {"buy":{},"sell":{}}
        buy_walls = {}
        sell_walls = {}
        for res in bittrex.get_orderbook(market,"buy")["result"]:
            price = flatPrice(market,res["Rate"])
            Quantity = (res["Quantity"] * res["Rate"])
            buy_walls[price] = Quantity if buy_walls.get(price) is None else buy_walls[price] + Quantity
        send_message(bot,market,buy_walls,"buy")
        for res in bittrex.get_orderbook(market,"sell")["result"]:
            price = flatPrice(market,res["Rate"])
            Quantity = (res["Quantity"] * res["Rate"])
            sell_walls[price] = Quantity if sell_walls.get(price) is None else sell_walls[price] + Quantity
        send_message(bot,market,sell_walls,"sell")

job.run_repeating(watcher, interval=120, first=0)
job.start()
