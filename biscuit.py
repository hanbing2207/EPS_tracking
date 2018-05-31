import tushare as ts
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



stock = "600188"
df_stock = ts.get_h_data(stock)
print(df_stock)

#get derivativ#: return
close = df_stock["close"]
ror = close.shift(1)/close