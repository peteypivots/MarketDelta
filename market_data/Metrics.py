import math
import numpy as np


def getMetrics(l,net_delta_hist):

    if len(l) == 0:
        return []

    total_volume = 0
    net_buy_volume = 0
    net_sell_volume = 0
    imbalance = {}
    market_depth = []
    
    first_value = l[0]
    last_value = l[len(l)-1]
    
    start_time = first_value['ts_ms']
    end_time = last_value['ts_ms']
    
    time_diff = (end_time - start_time).total_seconds()

    open_price = first_value['price']
    closing_price = last_value['price']
    bar_direction = 1 if (open_price < closing_price) else -1
    
    mean_price = getMean(l)
    sd = getStd(l)
    
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

    if len(net_delta_hist) < 3:
        return []
    
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

    resp = {'direction':bar_direction,'data':[float(total_volume)
    ,float(net_delta),float(time_diff),float(volume_sec),float(sell_imb)
    ,float(buy_imb),float(sd),float(net_delta_hist[0]),float(net_delta_hist[1])]}

    if len(net_delta_hist) == 3:
        del net_delta_hist[0]

    return resp
    






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
