import argparse
import pandas as pd
import numpy as np
import datetime

import json

def init():
    parser = argparse.ArgumentParser(
        prog = __file__,
        description = 'delta math')
        
    parser.add_argument('-f','--filename')

    args = parser.parse_args()
    global dfile
    dfile = args.filename 
    print('file: ',dfile)
    #return dfile

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
    #df1 = df.groupby(pd.Grouper(key='time', freq= str(minutes) +'m'))
    
    #windows = (df.time.diff().apply(lambda x: x.total_seconds()) >= 1)
    
    current_ind = 0
    total_count=0
    count = 0
    
    l = []
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
            getMetrics(l)
            l = []
            break

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
    
    start_price = first_value['price']
    closing_price = last_value['price']
    
    max_price = max(l, key=lambda feature: feature['price'])['price']
    min_price = min(l, key=lambda feature: feature['price'])['price']
    
    bar_direction = 'u' if (start_price < closing_price) else 'd'
    
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
    total_volume = "{:.2f}".format(total_volume)
    
    print(total_volume,net_delta,bar_direction,volume_sec,min_price,max_price,time_diff)
    
    
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
    
    print('HighWick',high_wick_bid_ask)
    print('LowWick',low_wick_bid_ask)
    
    
    buy_imb = 0
    sell_imb = 0
    
    i = 0
    p1 = market_depth[i]['s']
    for j in range(1,len(market_depth)):
        
        p2 = market_depth[j]['b']
        
        #print(p1,p2)
        if p1 > p2:
            if p2 != 0:
                print(market_depth[i]['p'],'sell',p1 / p2,p1,p2)
        else:
            if p1 != 0:
                print(market_depth[j]['p'],'buy',p2 / p1,p1,p2)
        
        
        i+=1
        p1 = market_depth[i]['s']
        
        
    
if __name__ == "__main__":
    
    init()
    parseFile()
    cleanData(1)
    
    
    
    
    
    