import numpy as np
from funcat import *

from funcat.data.tushare_backend import TushareDataBackend
from funcat.data.mongodb_backend import MongodbBackend

B1 = TushareDataBackend()
a1 = B1.get_price('688389.XSHE', '2020-08-01', '2020-08-06', '1d')
d1 = B1.symbol('000001.XSHG')
b1 = B1.get_order_book_id_list()
c1 = B1.get_trading_dates('2020-05-01', '2020-05-30')
print(a1)
print('==========')
B2 = MongodbBackend()
a2 = B2.get_price('688389.XSHE', '2020-08-01', '2020-08-06', '1d')
d2 = B2.symbol('000001.XSHG')
b2 = B2.get_order_book_id_list()
c2 = B2.get_trading_dates('2020-05-01', '2020-05-30')
print(a2)
