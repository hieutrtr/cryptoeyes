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

def verify(chat_id):
    return chat_id == my_chatid

def my_balance(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
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
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
my_balance_handler = CommandHandler('mb', my_balance, pass_args=True)
dispatcher.add_handler(my_balance_handler)

def my_trans(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    message = ""
    for res in bittrex.get_order_history()["result"][:10]:
        message += "*{}* {} {} at {} \n".format(res["Exchange"],res["OrderType"].replace("_"," "),res["Quantity"],res["PricePerUnit"])
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
my_trans_handler = CommandHandler('mt', my_trans, pass_args=True)
dispatcher.add_handler(my_trans_handler)

def sum_market(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    bot.send_message(chat_id=my_chatid, text='{}'.format(bittrex.get_marketsummary(args[0])),parse_mode=ParseMode.MARKDOWN)
sum_market_handler = CommandHandler('sum', sum_market, pass_args=True)
dispatcher.add_handler(sum_market_handler)

def protect_btc(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    quantity = float(args[0])
    rate = float(args[1])
    result = bittrex.sell_limit("USDT-BTC",quantity,rate)
    message = "{}".format(result)
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
protect_btc_handler = CommandHandler('pbtc', protect_btc, pass_args=True)
dispatcher.add_handler(protect_btc_handler)

def error_callback(bot, update, error):
    raise error
dispatcher.add_error_handler(error_callback)

updater.start_polling()
