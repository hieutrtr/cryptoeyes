import sys,os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path+'/..')
from coinmarketcap import Market
from kafka import KafkaProducer
import json,datetime,time
import redis
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1, BUY_ORDERBOOK, TICKINTERVAL_FIVEMIN, TICKINTERVAL_HOUR, TICKINTERVAL_DAY
import _thread

rose_host = os.environ['ROSE_HOST']

TICKINTERVAL = {
    "HOUR":TICKINTERVAL_HOUR,
    "FIVEMIN": TICKINTERVAL_FIVEMIN,
    "DAY": TICKINTERVAL_DAY
}
tinterval = os.environ['TICKINTERVAL']
last = os.environ['LAST']
coinmarketcap = Market()
# coins = ['bitcoin','ethereum','bitcoin-cash','iota','ripple','dash','litecoin']
partition = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y')
coins = coinmarketcap.ticker(limit=500)

def scrape(chunk=1,ticker_interval=TICKINTERVAL_HOUR):
    producer = KafkaProducer(bootstrap_servers=rose_host)
    for coin in coins[(chunk-1)*10:chunk*10]:
        market = 'BTC-' + coin["symbol"]
        if coin["symbol"] == 'BTC':
            market = 'USDT-' + coin["symbol"]

        bittrex = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'])
        bittrexv2 = Bittrex(os.environ['CRYPTOEYES_KEY'], os.environ['CRYPTOEYES_SEC'],api_version=API_V2_0)
        # histories = bittrex.get_market_history(market)
        if last == "True":
            candles = bittrexv2.get_latest_candle(market,ticker_interval)
        else:
            candles = bittrexv2.get_candles(market,ticker_interval)
        if candles.get("success") == True and candles.get("result") is not None:
            topic = 'bittrex.' + market + '.candle.' + ticker_interval + '.' + partition
            for can in candles["result"]:
                producer.send(topic, json.dumps(can).encode())
        else: print(market,candles)
    print("there're " + str(len(coins)) + " of coins are tracking.")

# Create two threads as follows
try:
   for i in range(50):
       _thread.start_new_thread( scrape, (i, TICKINTERVAL[tinterval]) )
except:
   print("Error: unable to start thread")

time.sleep(120)
