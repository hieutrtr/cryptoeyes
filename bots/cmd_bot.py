import json
from telegram.ext import Updater,CommandHandler
updater = Updater(token='481353725:AAFIPgZmgz1bv7C6NgDFeIf25ZSNPWU3XP0')
dispatcher = updater.dispatcher
def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text="I'm a bot, please talk to me!")
start_handler = CommandHandler('start', start)
dispatcher.add_handler(start_handler)

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
