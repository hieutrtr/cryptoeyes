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
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1, BUY_ORDERBOOK, TICKINTERVAL_ONEMIN, TICKINTERVAL_HOUR, TICKINTERVAL_DAY, TICKINTERVAL_FIVEMIN

TICKINTERVAL = {
    "HOUR":TICKINTERVAL_HOUR,
    "FIVEMIN": TICKINTERVAL_FIVEMIN,
    "DAY": TICKINTERVAL_DAY
}

CHAT_INTERVAL = {
    "HOUR":3600,
    "FIVEMIN": 60*5,
    "DAY": 3600*24
}

tinterval = os.environ['TICKINTERVAL']

bittrex = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])
bittrexv2 = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'],api_version=API_V2_0)
my_chatid = os.environ['MY_CHATID']
updater = Updater(token=os.environ['BOT_TOKEN'])
dispatcher = updater.dispatcher
job = updater.job_queue

def get_candle(market):
    candle = bittrexv2.get_latest_candle(market,TICKINTERVAL[tinterval])
    if candle["success"] is True:
        return candle["result"][0]
    return {}

def analyze_candle(candle):
    if not candle:
        return {}
    body = candle['O'] - candle['C']
    up_tail = candle['H'] - candle['C'] if body >= 0 else candle['H'] - candle['O']
    low_tail = candle['O'] - candle['L'] if body >= 0 else candle['C'] - candle['L']
    body = (body * -1) if body < 0 else body
    if up_tail >= body*2 and low_tail <= body/2:
        return candle
    elif low_tail >= body*2 and up_tail <= body/2:
        return candle
    return {}

def send_message(bot,market,candle):
    if not candle
        message = "*{} is reversing*\n{}\n".format(market,candle)
        bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)

def watcher(bot, job):
    markets = os.environ['MARKETS'].split(",")
    for market in markets:
        candle = get_candle(market)
        acandle = analyze_candle(candle)
        send_message(bot,market,acandle)

job.run_repeating(watcher, interval=CHAT_INTERVAL[tinterval], first=0)
job.start()
