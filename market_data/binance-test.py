
from time import sleep
import datetime
import pandas as pd
import numpy as np
import json
import math
import time
import copy
import redis
from rq import Queue
from Metrics import getMetrics
import asyncio
import async_timeout
import redis.asyncio as redis
import pickle
#from sklearn.ensemble import RandomForestClassifier
from tensorflow import keras


channel = "TRADE|SPOT|ETH|USDT"

r = redis.Redis()
q = Queue(connection=r)

# Set up logging (optional)
import logging
logging.basicConfig(filename="binance.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

global_data = []
last_mid_price = 0
net_delta_hist = []
global_time = None
global_model = None


def load_model():

    global global_model
    #global_model = pickle.load(open('../MarketDelta/finalized_model.sav', 'rb'))
    global_model = keras.models.load_model('../final_seq_nn.sav')
    print("LOADED MODEL")


def unix2read(ts):
    #.strftime('%Y-%m-%d %H:%M:%S.%f')
    return datetime.datetime.fromtimestamp(int(ts) / 1000,tz=datetime.timezone.utc)

def background_task(l):

    #job = q.enqueue(getMetrics,l,net_delta_hist,last_mid_price)
    resp = getMetrics(l,net_delta_hist)
    if len(resp) != 0:
        
        dir = str(resp['direction'])
        data = resp['data']
        mod_resp = [data]

        header = "DIR:{0} TotalVolume:{1} NetDelta:{2} Duration:{3} Volume_Sec:{4} BidImb:{5} AskImb:{6} Sd:{7} NetDelta3({8}) NetDelta3({9}) Result:{10:.2f}"
        txt = header.format(dir,data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8],global_model.predict(mod_resp)[0][0])
        print(len(l),net_delta_hist,global_model.predict(mod_resp)[0][0])
        logging.info(txt)


async def handle_notification():
    r = redis.Redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("TRADE|SPOT|ETH|BUSD")

    while True:
        
        try:
            async with async_timeout.timeout(1):
                sample_data()
                message = await pubsub.get_message()
                if message and message["type"] == "message":
                    payload = json.loads(message["data"])
                    #print(payload['symbol'])
                    global global_data
                    global_data.append(payload)
        except (asyncio.TimeoutError, json.decoder.JSONDecodeError) as e:
            logging.error(e)


def sample_data():

    minutes = 5
    seconds = minutes * 60
    now_ms = time.time() * 1000
    now_ms = pd.to_datetime( now_ms ,unit='ms')
    
    global global_time
    if global_time == None:
        global_time = now_ms 
    else:    
        diff = (now_ms - global_time).total_seconds()
        if diff > seconds:
            local_data = []
            global global_data
            for value in global_data:
                    
                symbol = value['symbol']
                price = value['price']
                size = value['size']
                side = 1 if value['side'] == 'buy' else -1
                ts = value['systs']
                ts_ms = pd.to_datetime(int(value['systs'])/1e3,unit='ms')

                msg = {
                    'symbol':symbol
                    ,'price':float(price)
                    ,'qty':float(size)
                    ,'side':side
                    ,'ts':ts
                    ,'ts_ms':ts_ms
                }    
                    
                local_data.append(msg)

                
            background_task(local_data)
            global_time = None
            global_data = []


if __name__ == "__main__":
    
    load_model()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle_notification())
    