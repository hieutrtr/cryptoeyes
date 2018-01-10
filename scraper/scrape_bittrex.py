import sys,os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path+'/..')
from coinmarketcap import Market
from kafka import KafkaProducer
import json,datetime,time
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1, BUY_ORDERBOOK, TICKINTERVAL_ONEMIN, TICKINTERVAL_HOUR

bittrex = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])
bittrexv2 = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'],api_version=API_V2_0)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d')
coins = coinmarketcap.ticker(limit=200)
TICKINTERVAL = {
    "HOUR":TICKINTERVAL_HOUR,
    "ONEMIN": TICKINTERVAL_ONEMIN
}
for coin in coins:
    market = 'BTC-' + coin["symbol"]
    print(market)
    topic = 'bittrex.' + market + '.hourly'# + partition
    candles = bittrexv2.get_latest_candle(market,TICKINTERVAL[sys.argv[1]])
    if candles.get("success") == True and len(candles.get("result",[])) > 0:
        for can in candles["result"]:
            producer.send(topic, bytes(can))
    else: print(market,candles)
# print "there're " + str(len(coins)) + " of coins are tracking."
#
# print "--------Ticker\n", bittrex.get_ticker("BTC-LTC")
# print "--------Sum\n", bittrex.get_marketsummary("BTC-LTC")
# print "--------Order\n", bittrex.get_orderbook("BTC-LTC")
# print "--------History\n", bittrex.get_market_history("BTC-LTC")
# print "--------Candle\n", bittrexv2.get_candles("BTC-LTC",TICKINTERVAL_HOUR)
# print "--------Latest Candle\n", bittrexv2.get_latest_candle("BTC-LTC",TICKINTERVAL_HOUR)
