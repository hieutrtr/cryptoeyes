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
dispatcher = updater.dispatcher
job = updater.job_queue

def send_message(bot,market,walls,otype):
    message = "*{} wall - {}*\n".format(otype,market)
    message += "\n".join(["at *{}* have *{}*".format(k,v) for k,v in walls.items()])
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)

def watcher(bot, job):
    markets = os.environ['MARKETS'].split(",")
    asset = float(args[2])
    price = 0.0
    message = ""
    for market in markets:
        for res in bittrex.get_orderbook(market,"buy")["result"]:
            price = flatPrice(res["Rate"])
            buy_walls[price] = res["Quantity"] if matrix.get(price) is None else matrix[price] + res["Quantity"]
        send_message(bot,market,buy_walls,"buy")
        for res in bittrex.get_orderbook(market,"sell")["result"]:
            price = flatPrice(res["Rate"])
            sell_walls[price] = res["Quantity"] if matrix.get(price) is None else matrix[price] + res["Quantity"]
        send_message(bot,market,sell_walls,"sell")

job.run_repeating(watcher, interval=120, first=0)
job.start()
