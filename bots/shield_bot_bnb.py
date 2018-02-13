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
from binance.client import Client
from binance.enums import *
bnb_client = Client(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])

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
    for ba in bnb_client.get_account()["balances"]:
        balance = float(ba["free"]) + float(ba["locked"])
        if balance != 0 :
            ticker = "N/A"
            if ba["asset"] not in ['BTC','USDT']:
                ticker = bnb_client.get_symbol_ticker(symbol=ba["asset"]+"BTC")
                last_price = float(ticker["price"]) if ticker["price"] else 0.0
                ticker = last_price*balance
                sum_btc += ticker
            elif ba["asset"] == 'BTC':
                sum_btc += balance
            elif ba["asset"] == 'USDT':
                usdt = balance
            message += '*{}*:{} ({})\n'.format(ba["asset"],balance,ticker)

    btc_last = float(bnb_client.get_symbol_ticker(symbol="BTCUSDT")["price"])
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
    for res in bnb_client.get_open_orders():
        message += "*{}* {} {} {} at {} when {} \n /c{} \n".format(res["symbol"],res["type"],res["side"],res["origQty"],res["price"],res["stopPrice"],res["orderId"])
    bot.send_message(chat_id=update.message.chat_id, text=message,parse_mode=ParseMode.MARKDOWN)
my_open_order_handler = CommandHandler('od', my_open_order, pass_args=True)
dispatcher.add_handler(my_open_order_handler)

# def my_trans(bot, update, args):
#     if verify(update.message.chat_id) is False:
#         bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
#         bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
#         return
#     lim = int(args[0])
#     message = ""
#     for res in bittrex.get_order_history()["result"][:lim]:
#         message += "*{}* {} {} at {} \n".format(res["Exchange"],res["OrderType"].replace("_"," "),res["Quantity"],res["PricePerUnit"])
#     bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
# my_trans_handler = CommandHandler('mt', my_trans, pass_args=True)
# dispatcher.add_handler(my_trans_handler)

def sum_market(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    bot.send_message(chat_id=my_chatid, text='{}'.format(bnb_client.get_ticker(symbol=args[0])),parse_mode=ParseMode.MARKDOWN)
sum_market_handler = CommandHandler('sum', sum_market, pass_args=True)
dispatcher.add_handler(sum_market_handler)

def protect(bot, update, args):
    if verify(update.message.chat_id) is False:
        bot.send_message(chat_id=update.message.chat_id, text="*You're not my master!!*",parse_mode=ParseMode.MARKDOWN)
        bot.send_message(chat_id=my_chatid, text="*Someone call to your bot* id {}".format(update.message.chat_id),parse_mode=ParseMode.MARKDOWN)
        return
    quantity = float(args[0])
    rate = args[1]
    otype = args[2]
    coin = args[3]
    result = "N/A"
    order_id = 0
    if len(args) < 5:
        if otype == 's':
            result = bnb_client.create_order(
                        symbol=coin,
                        side=SIDE_SELL,
                        type=ORDER_TYPE_LIMIT,
                        timeInForce=TIME_IN_FORCE_GTC,
                        quantity=quantity,
                        price=rate)
        elif otype == 'b':
            result = bnb_client.create_order(
                        symbol=coin,
                        side=SIDE_BUY,
                        type=ORDER_TYPE_LIMIT,
                        timeInForce=TIME_IN_FORCE_GTC,
                        quantity=quantity,
                        price=rate)
        order_id = result['orderId']
    else:
        target = float(args[4])
        if otype == 's':
            result = bnb_client.create_order(
                        symbol=coin,
                        side=SIDE_SELL,
                        type=ORDER_TYPE_STOP_LOSS_LIMIT,
                        timeInForce=TIME_IN_FORCE_GTC,
                        quantity=quantity,
                        price=rate,
                        stopPrice=target)
        elif otype == 'b':
            result = bnb_client.create_order(
                        symbol=coin,
                        side=SIDE_BUY,
                        type=ORDER_TYPE_STOP_LOSS_LIMIT,
                        timeInForce=TIME_IN_FORCE_GTC,
                        quantity=quantity,
                        price=rate,
                        stopPrice=target)
        order_id = result['orderId']
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
    symbol = args[1]
    result = bnb_client.cancel_order(symbol=symbol,orderId=uuid)
    message = "{}".format(result)
    bot.send_message(chat_id=my_chatid, text=message,parse_mode=ParseMode.MARKDOWN)
cancel_handler = CommandHandler('c', cancel, pass_args=True)
dispatcher.add_handler(cancel_handler)

def error_callback(bot, update, error):
    raise error
dispatcher.add_error_handler(error_callback)

updater.start_polling()
