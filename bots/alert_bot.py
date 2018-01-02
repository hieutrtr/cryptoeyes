import json
from telegram.ext import Updater,CommandHandler
updater = Updater(token='464648319:AAFO8SGTukV4LHYtzpmjhbybyrwt0QQwIp8')
job = updater.job_queue

def alert_price(bot, job):
    prices = {}
    with open("price.json") as pricesFile:
        prices = json.load(pricesFile)
        pricesFile.close()
    for key, value in prices.items():
        preDict = value["prediction"]
        message = ""
        if float(value["last"]) >= preDict['1.618'] and float(value["last"]) <= preDict['1.618']*1.1:
            message = key +  "'s price is reach 1.618 !!!\n Let take your profit, My Lord !!!"
        elif float(value["last"]) >= preDict['1.382'] and float(value["last"]) <= preDict['1.382']*1.1 :
            message = key +  "'s price is reach 1.382 !!!\n Let take your profit, My Lord !!!"
        elif float(value["last"]) >= preDict['100'] and float(value["last"]) <= preDict['100']*1.1:
            message = key +  "'s price is back to 100 !!!\n Let consider your strategy !!!"
        elif float(value["last"]) >= preDict['0.618'] and float(value["last"]) <= preDict['0.618']*1.1:
            message = key +  "'s price is back to 0.618 !!!\n It should get back !!!"
        elif float(value["last"]) >= preDict['0.382'] and float(value["last"]) <= preDict['0.382']*1.1:
            message = key +  "'s price is back to 0.382 !!!\n It should get back !!!"
        elif float(value["last"]) >= preDict['0'] and float(value["last"]) <= preDict['0']*1.1:
            message = key +  "'s price is back to 0 !!!\n It should get back !!!"
        elif float(value["last"]) >= preDict['-0.382'] and float(value["last"]) <= preDict['-0.382']*1.1:
            message = key +  "'s price is back to -0.382 !!!\n Can we buy some, My Lord !!!"
        elif float(value["last"]) >= preDict['-0.618'] and float(value["last"]) <= preDict['-0.618']*1.1:
            message = key +  "'s price is back to -0.618 !!!\n Can we buy some, My Lord !!!"
        if message != "":
            bot.send_message(chat_id='423404239',text=message)

job.run_repeating(alert_price, interval=1800, first=0)
job.start()
# updater.start_polling()
