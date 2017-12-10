import json
from telegram.ext import Updater,CommandHandler
updater = Updater(token='481353725:AAFIPgZmgz1bv7C6NgDFeIf25ZSNPWU3XP0')
job = updater.job_queue
# dispatcher = updater.dispatcher
# def start(bot, update):
#     bot.send_message(chat_id=update.message.chat_id, text="I'm a bot, please talk to me!")
#
# start_handler = CommandHandler('start', start)
# dispatcher.add_handler(start_handler)


def update_price(bot, job):
    prices = {}
    with open("/home/hieutrtr/cryptoeyes/price.json") as pricesFile:
        prices = json.load(pricesFile)
        pricesFile.close()
    for key, value in prices.items():
        prediction = json.dumps(value["prediction"])
        del value["prediction"]
        bot.send_message(chat_id='423404239',text=key + ' : ' + json.dumps(value) + '\n' + prediction)

job.run_repeating(update_price, interval=300, first=0)
job.start()
# updater.start_polling()
