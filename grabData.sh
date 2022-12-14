#!/bin/bash 

output="output.zip"
curl https://data.binance.vision/data/spot/monthly/trades/ETHBUSD/ETHBUSD-trades-2022-10.zip --output $output 
unzip $output
rm output.zip

