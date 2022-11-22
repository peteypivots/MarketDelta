import argparse
import functools
import pandas as pd
import numpy as np
import datetime
import json
import sys
import os
import math

last_mid_price = 0
    
def init():
    parser = argparse.ArgumentParser(
        prog = __file__,
        description = 'delta math')
        
    parser.add_argument('-f','--filename')
    parser.add_argument('-o','--output',nargs='?', const=1, type=str, default=None)
    
    args = parser.parse_args()
    global dfile
    dfile = args.filename
    global dout
    dout = args.output
    global net_delta_hist
    net_delta_hist = []
    
    print('file: ',dfile)
    print('output: ',dout)
    
def parseFile():
    header = ['tid','price','qty','quoteQty','time','side','bmatch']
    global df
    df = pd.read_csv (dfile,names=header)
    print(df)

def cleanData(minutes):
    
    seconds = minutes * 60

    df['time'] = pd.to_datetime(df['time'], unit='ms')
    
    df['side'] = np.where(df['side']==True,1, -1)
    
    print(df)
    
    current_ind = 0
    total_count=0
    count = 0
    
    l = []

    if os.path.exists('output.csv'):
        os.remove('output.csv')
    
    with open('output.csv', 'a') as f:
        
        if dout != None:
            header = "start_time,end_time,open_price,closing_price,total_volume,bar_direction,cum_delta,bar_duration,volume_sec,high_wick_bid_ask,low_wick_bid_ask,bid_imb,ask_imb,price_sd,price_mean,net_delta_t3,net_delta_t2,net_delta_t1,log_return_p1\n"
            f.write(header)
        
        t1 = df['time'][current_ind]
        l.append(json.loads(df.loc[current_ind].to_json()))
        for j in range(1,len(df)):
        
            t2 = df['time'][j]
            diff = (t2 - t1).total_seconds()
        
            count+=1
            #print(t1,t2,diff,current_ind,count)
            l.append(json.loads(df.loc[j].to_json()))
            if diff > seconds:
            
                current_ind = j
                count=0
            
                t1 = df['time'][current_ind]
                o = getMetrics(l)
                
                if dout != None and o != "":
                    f.write(o)   

                l = []

    f.close()          

def unix2read(ts):
    #.strftime('%Y-%m-%d %H:%M:%S.%f')
    return datetime.datetime.fromtimestamp(int(ts) / 1000,tz=datetime.timezone.utc)

def addBuySell(i,p,v,s):
    
    if p in i:
        if s == -1:
            i[p]['sell'] += float(v)
        elif s == 1:
            i[p]['buy'] += float(v)
    else:
        if s == -1:
            i[p] = {'sell':float(v),'buy':0}
        elif s == 1:
            i[p] = {'sell':0,'buy':float(v)}

    
def getMetrics(l):
    
    total_volume = 0
    net_buy_volume = 0
    net_sell_volume = 0
    imbalance = {}
    market_depth = []
    
    first_value = l[0]
    last_value = l[len(l)-1]
    
    start_time = unix2read(first_value['time'])
    end_time = unix2read(last_value['time'])
    time_diff = (end_time - start_time).total_seconds()
    
    open_price = first_value['price']
    closing_price = last_value['price']
    
    mean_price = getMean(l)
    sd = getStd(l)
    
    max_price = max(l, key=lambda feature: feature['price'])['price']
    min_price = min(l, key=lambda feature: feature['price'])['price']
    
    
    log_return = 0
    global last_mid_price
    if last_mid_price == 0:
        last_mid_price = (max_price + min_price)/2
    else:
        mid_price = (max_price + min_price)/2
        log_return = abs(np.log(mid_price) - np.log(last_mid_price)) * 100
        log_return = "{:.5f}".format(log_return)
        last_mid_price = mid_price
    
    bar_direction = 1 if (open_price < closing_price) else -1
    
    for value in l:
        
        price = str(value['price'])
        total_volume += float(value['qty'])
        
        if value['side'] == 1:
            net_buy_volume += float(value['qty'])
            addBuySell(imbalance,price,value['qty'],1)
        elif value['side'] == -1:
            net_sell_volume += float(value['qty'])
            addBuySell(imbalance,price,value['qty'],-1)
    
    
    volume_sec = "{:.2f}".format(total_volume / time_diff)
    net_delta = "{:.2f}".format(net_sell_volume - net_buy_volume)
    
    net_delta_hist.append(net_delta)

    total_volume = "{:.2f}".format(total_volume)
    mean_price = "{:.2f}".format(mean_price)
    sd = "{:.2f}".format(sd)
    
    for k in sorted(imbalance.keys(),reverse=False):
        
        b = imbalance[k]['buy']
        s = imbalance[k]['sell']
        
        market_depth.append({
            'p':k
            ,'s':s
            ,'b':b
        });
     
    low_wick_bid_ask=market_depth[0]
    high_wick_bid_ask=market_depth[len(market_depth)-1]
    

    buy_imb = 0
    sell_imb = 0
    threshold = 100 # size

    i = 0
    p1 = market_depth[i]['s']
    for j in range(1,len(market_depth)):
        
        p2 = market_depth[j]['b']
        
        if p1 > p2:
            if p2 != 0:
                
                d=(p1 / p2)
                if d > threshold:
                    sell_imb+=1
                    #print(market_depth[i]['p'],'sell',"{:.2f}".format(d),"{:.2f}".format(p1),"{:.2f}".format(p2))
        else:
            if p1 != 0:

                d=(p2 / p1)
                if d > threshold:
                    buy_imb+=1    
                    #print(market_depth[j]['p'],'buy',"{:.2f}".format(d),"{:.2f}".format(p1),"{:.2f}".format(p2))

        i+=1
        p1 = market_depth[i]['s']

    print(total_volume,net_delta,bar_direction,volume_sec,min_price,max_price,time_diff)    
    print('HighWick',high_wick_bid_ask)
    print('LowWick',low_wick_bid_ask)

    high_wick = str(high_wick_bid_ask['s']) + 'x'+ str(high_wick_bid_ask['b'])
    low_wick = str(low_wick_bid_ask['s']) + 'x'+ str(low_wick_bid_ask['b'])

    print('BuyImb',buy_imb,'SellImb',sell_imb)
    print('************************')
    
    output =""
    
    if len(net_delta_hist) == 3:
        
        
        output += start_time.strftime('%Y-%m-%d %H:%M:%S.%f') + ","
        output += end_time.strftime('%Y-%m-%d %H:%M:%S.%f') + ","
        output += str(open_price) + ","
        output += str(closing_price) + ","
        output += str(total_volume) + ","
        output += str(bar_direction) + ","
        output += str(net_delta) + ","
        output += str(time_diff) + ","
        output += str(volume_sec) + ","
        output += str(high_wick) + ","
        output += str(low_wick) + ","
        output += str(buy_imb) + ","
        output += str(sell_imb) + ","
        output += str(sd) + ","
        output += str(mean_price) + ","
        output += net_delta_hist[0] + ","
        output += net_delta_hist[1] + ","
        output += net_delta_hist[2] + ","
        output += log_return + "\n"
        
        del net_delta_hist[0]
    
    return output

def getMean(l):
    mean = 0
    for v in l:
        mean += v['price']
        
    return mean/len(l)
    
def getStd(l):
    
    sq_diff = 0
    mean = getMean(l)
    
    deviations = [(x['price'] - mean) ** 2 for x in l]
    variance = sum(deviations) / len(l)
    std_dev = math.sqrt(variance)
    
    return std_dev
    
    
    
    
if __name__ == "__main__":
    
    init()
    parseFile()
    cleanData(5)
    
    
    
    
    
    