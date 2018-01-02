import json
from telegram.ext import Updater,CommandHandler
updater = Updater(token='481353725:AAFIPgZmgz1bv7C6NgDFeIf25ZSNPWU3XP0')
job = updater.job_queue

def update_price(bot, job):
    prices = {}
    followcoin = []
    with open("config/follow.json") as followFile:
        followcoin = json.load(followFile)
        followFile.close()
    print followcoin
    with open("price.json") as pricesFile:
        prices = json.load(pricesFile)
        pricesFile.close()
    for key, value in prices.items():
        if key in followcoin:
            prediction = json.dumps(value["prediction"])
            del value["prediction"]
            bot.send_message(chat_id='423404239',text=key + ' : ' + json.dumps(value) + '\n' + prediction)

job.run_repeating(update_price, interval=300, first=0)
job.start()
# updater.start_polling()
