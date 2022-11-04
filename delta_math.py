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
    l.append(df.loc[current_ind].to_json())
    for j in range(1,len(df)):
        
        t2 = df['time'][j]
        diff = (t2 - t1).total_seconds()
        
        count+=1
        #print(t1,t2,diff,current_ind,count)
        l.append(df.loc[j].to_json())
        if diff > seconds:
            
            current_ind = j
            count=0
            
            t1 = df['time'][current_ind]
            getMetrics(l)
            l = []
            #break

def unix2read(ts):
    return datetime.datetime.fromtimestamp(int(ts) / 1000,tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')

def getMetrics(l):
    
    total_volume = 0
    
    first_value = json.loads(l[0])
    last_value = json.loads(l[len(l)-1])
    
    start_time = unix2read(first_value['time'])
    end_time = unix2read(last_value['time'])
    
    start_price = first_value['price']
    closing_price = last_value['price']
    
    for value in l:
        blob = json.loads(value)
        total_volume += float(blob['qty'])
        
        

    print(total_volume)
    
if __name__ == "__main__":
    
    init()
    parseFile()
    cleanData(1)
    
    
    
    
    
    