#!/bin/bash 

output="output.zip"
curl https://data.binance.vision/data/spot/daily/trades/ETHBUSD/ETHBUSD-trades-2022-11-02.zip --output $output 
unzip $output
rm output.zip
