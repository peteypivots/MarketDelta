# first neural network with keras tutorial
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
#import sklearn as sk
import pandas as pd

import os
import numpy as np
import pandas as pd

col_names = ["start_time","end_time","open_price","closing_price","total_volume","bar_direction","cum_delta","bar_duration","volume_sec","high_wick_bid_ask","low_wick_bid_ask","bid_imb","ask_imb","price_sd","price_mean","net_delta_t3","net_delta_t2","net_delta_t1","log_return_p1"]

data = pd.read_csv("output.csv", skiprows=1, header=None, names=col_names)
data = data[["total_volume","bar_direction","cum_delta","bar_duration","volume_sec","bid_imb","ask_imb","price_sd","open_price","closing_price","price_mean","net_delta_t3","net_delta_t2","net_delta_t1","log_return_p1"]]

data['log_return_p1'] = data['log_return_p1'].shift(-1)
data = data.dropna()

data["log_return_p1"] = data["log_return_p1"].astype(float)
#data['log_bool'] = data['log_return_p1'].apply(lambda x: 'T' if float(x) > 0.2 else 'F')


def logic(row):
    if ((row["bar_direction"] == 1 
         and row["cum_delta"] < 0) or (row["bar_direction"] == -1 
         and row["cum_delta"] > 0)) and row["log_return_p1"] > 0.02:
        
        return 1
    else:
        return 0

data = data.assign(log_bool=data.apply(logic, axis=1))


#len(data["log_return_p1"] )
len(data[data['log_bool'] == 1])

data[data['log_bool'] == 1]

COLUMNS = ["total_volume","cum_delta","bar_duration","volume_sec","bid_imb","ask_imb","price_sd","net_delta_t3","net_delta_t2"]

X_Cols = data[COLUMNS]
Y_Cols = data["log_bool"]

train_size = int(len(X_Cols) * .95)
test_size = len(X_Cols) - train_size

train_x,test_x = X_Cols.iloc[0:train_size],X_Cols.iloc[0:test_size]
train_y,test_y = Y_Cols.iloc[0:train_size],Y_Cols.iloc[0:test_size]

#train_x, test_x, train_y, test_y = sk.model_selection.train_test_split(X_Cols, Y_Cols, random_state = 50)

print(train_x.shape,train_y.shape)
print(train_y.dtypes)

model = keras.Sequential()
model.add(keras.layers.Dense(12, input_shape=(9,), activation='relu'))
model.add(keras.layers.Dense(9, activation='relu'))
model.add(keras.layers.Dense(1, activation='sigmoid'))


model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
# fit the keras model on the dataset
model.fit(train_x, train_y, epochs=1000, batch_size=10)

_, accuracy = model.evaluate(test_x, test_y)
print('Accuracy: %.2f' % (accuracy*100))

model.save('final_seq_nn.sav')