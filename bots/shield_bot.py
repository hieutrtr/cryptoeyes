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
bittrexv2 = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'],api_version=API_V2_0)

def verify(chat_id):
    return str(chat_id) == my_chatid

def my_balance(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    message = ""
    sum_btc = 0
    usdt = 0
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
            elif ba["Currency"] == 'USDT':
                usdt = ba["Balance"]
            message += '*{}*:{} ({})\n'.format(ba["Currency"],ba["Balance"],ticker)

    btc_last = bittrex.get_marketsummary("USDT-BTC")["result"][0]["Last"]
    message+='*Sum BTC*: {} btc / {} usdt\n'.format(sum_btc,sum_btc*btc_last)
    message+='*Sum USDT*: {} usdt'.format(usdt+(sum_btc*btc_last))
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
my_balance_handler = CommandHandler('mb', my_balance, pass_args=True)
dispatcher.add_handler(my_balance_handler)

def my_open_order(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    message = ""
    for res in bittrex.get_open_orders()["result"]:
        message += "*{}* {} {} at {} when {} \n /c{} \n".format(res["Exchange"],res["OrderType"].replace("_"," "),res["Quantity"],res["Limit"],res["ConditionTarget"],res["OrderUuid"])
    bot.send_message(chat_id=update.message.chat_id, text=message,parse_mode=ParseMode.MARKDOWN)
my_open_order_handler = CommandHandler('od', my_open_order, pass_args=True)
dispatcher.add_handler(my_open_order_handler)

def my_trans(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    lim = int(args[0])
    message = ""
    for res in bittrex.get_order_history()["result"][:lim]:
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
    otype = args[2]
    result = "N/A"
    order_id = 0
    if len(args) < 4:
        if otype == 's':
            result = bittrex.sell_limit("USDT-BTC",quantity,rate)
        elif otype == 'b':
            result = bittrex.buy_limit("USDT-BTC",quantity,rate)
        order_id = result['result']['uuid']
    else:
        target = float(args[3])
        if otype == 's':
            result = bittrexv2.trade_sell(market="USDT-BTC",order_type='LIMIT',quantity=quantity,rate=rate,target=target,condition_type='LESS_THAN',time_in_effect='GOOD_TIL_CANCELLED')
        elif otype == 'b':
            result = bittrexv2.trade_buy(market="USDT-BTC",order_type='LIMIT',quantity=quantity,rate=rate,target=target,condition_type='GREATER_THAN',time_in_effect='GOOD_TIL_CANCELLED')
        order_id = result['result']['OrderId']
    message = "{}".format(order_id)
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
protect_btc_handler = CommandHandler('pbtc', protect_btc, pass_args=True)
dispatcher.add_handler(protect_btc_handler)

def protect(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    quantity = float(args[0])
    rate = float(args[1])
    otype = args[2]
    coin = args[3]
    result = "N/A"
    order_id = 0
    if len(args) < 5:
        if otype == 's':
            result = bittrex.sell_limit(coin,quantity,rate)
        elif otype == 'b':
            result = bittrex.buy_limit(coin,quantity,rate)
        order_id = result['result']['uuid']
    else:
        target = float(args[4])
        if otype == 's':
            result = bittrexv2.trade_sell(market=coin,order_type='LIMIT',quantity=quantity,rate=rate,target=target,condition_type='LESS_THAN',time_in_effect='GOOD_TIL_CANCELLED')
        elif otype == 'b':
            result = bittrexv2.trade_buy(market=coin,order_type='LIMIT',quantity=quantity,rate=rate,target=target,condition_type='GREATER_THAN',time_in_effect='GOOD_TIL_CANCELLED')
        order_id = result['result']['OrderId']
    message = "{}".format(order_id)
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
protect_handler = CommandHandler('p', protect, pass_args=True)
dispatcher.add_handler(protect_handler)

def cancel(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    uuid = args[0]
    result = bittrex.cancel(uuid)
    message = "{}".format(result)
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
cancel_handler = CommandHandler('c', cancel, pass_args=True)
dispatcher.add_handler(cancel_handler)

def error_callback(bot, update, error):
    raise error
dispatcher.add_error_handler(error_callback)

updater.start_polling()
