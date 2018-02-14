import redis
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

from binance.client import Client
from binance.enums import *
bnb_client = Client(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])

my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
alert_limit = int(os.environ['ALERT_LIMIT'])
dispatcher = updater.dispatcher
job = updater.job_queue

walls_cache = {}
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def send_message(bot,market,walls,otype):
    if market == 'BTCUSDT':
        alimit = alert_limit*10000
    else:
        alimit = alert_limit
    be_send = False

    ticker = bnb_client.get_symbol_ticker(symbol=market)
    last_price = float(ticker["price"])
    message = "*{} wall - {}*\n".format(otype,market)
    print(walls)
    for k in sorted(walls.keys()):
        print("k",k)
        print("walls[k]",walls[k])
        if walls_cache[market][otype].get(k) is None:
            if walls[k] > alimit:
                be_send = True
                message += 'at *{}* have *{}* as new\n'.format(k,walls[k])
        elif (walls[k] > walls_cache[market][otype][k] + alimit/5):
            if walls[k] > alimit:
                be_send = True
                message += 'at *{}* have *{}* increase *{}*\n'.format(k,walls[k],walls[k] - walls_cache[market][otype][k])
        elif (walls_cache[market][otype][k] > walls[k] + alimit/5):
            if walls[k] > alimit:
                be_send = True
                message += 'at *{}* have *{}* decrease *{}*\n'.format(k,walls[k],walls_cache[market][otype][k] - walls[k])
        elif walls[k] > alimit:
            message += 'at *{}* have {}\n'.format(k,walls[k])
    if be_send is True:
        for k,v in walls_cache[market][otype].items():
            if walls.get(k,0) < alimit and v > alimit:
                message += 'at *{}* have *{}* is disapeared\n'.format(k,v)
        message += "\nLast price:{}".format(last_price)
        walls_cache[market][otype] = walls
        rkey = 'binance.{}_wall.{}'.format(otype,market)
        r.set(rkey,walls)
        bot.send_message(chat_id=my_chatid, text="*Binance*\n"+message,parse_mode=ParseMode.MARKDOWN)

def flatPrice(market,price):
    price_count = 0
    if market == 'BTCUSDT':
        price = round(int(round(price))/100)*100 #(int(price)/100)*100
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
        order_books = bnb_client.get_order_book(symbol=market,limit=1000)
        for res in order_books["bids"]:
            price = flatPrice(market,float(res[0]))
            Quantity = (float(res[1]) * float(res[0]))
            buy_walls[price] = Quantity if buy_walls.get(price) is None else buy_walls[price] + Quantity
        send_message(bot,market,buy_walls,"buy")
        for res in order_books["asks"]:
            price = flatPrice(market,float(res[0]))
            Quantity = (float(res[1]) * float(res[0]))
            sell_walls[price] = Quantity if sell_walls.get(price) is None else sell_walls[price] + Quantity
        send_message(bot,market,sell_walls,"sell")

job.run_repeating(watcher, interval=120, first=0)
job.start()
