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
from binance.client import Client
from binance.enums import *
bnb_client = Client(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])

TICKINTERVAL = {
    "HOUR":Client.KLINE_INTERVAL_1HOUR,
    "FIVEMIN": Client.KLINE_INTERVAL_5MINUTE,
    "DAY": Client.KLINE_INTERVAL_1DAY
}

CHAT_INTERVAL = {
    "HOUR":3600,
    "FIVEMIN": 60*5,
    "DAY": 3600*24
}

tinterval = os.environ['TICKINTERVAL']
my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
dispatcher = updater.dispatcher
job = updater.job_queue

def get_candle(market):
    candle = bnb_client.get_klines(symbol=market, interval=TICKINTERVAL[tinterval])
    return candle[-2]

def analyze_candle(candle):
    if not candle:
        return {}
    O = float(candle[1])
    C = float(candle[4])
    H = float(candle[2])
    L = float(candle[3])
    body = C - O
    up_tail = H - C if body >= 0 else H - O
    low_tail = O - L if body >= 0 else C - L
    body = (body * -1) if body < 0 else body
    if up_tail >= body*4:
        return candle
    elif low_tail >= body*4:
        return candle
    return []

def send_message(bot,market,candle):
    if candle:
        message = "*Binance*\n*{} {} is reversing*\n{}\n".format(market,tinterval,candle)
        bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)

def watcher(bot, job):
    markets = os.environ['MARKETS'].split(",")
    for market in markets:
        candle = get_candle(market)
        acandle = analyze_candle(candle)
        send_message(bot,market,acandle)

job.run_repeating(watcher, interval=CHAT_INTERVAL[tinterval], first=0)
job.start()
