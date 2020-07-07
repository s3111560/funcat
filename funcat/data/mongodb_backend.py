# -*- coding: utf-8 -*-
#

from cached_property import cached_property

from .backend import DataBackend
from ..utils import lru_cache, get_str_date_from_int, get_int_date

import pymongo
import QUANTAXIS as qa
import pandas as pd

# XSHG - 上证
# XSHE - 深证

class MongodbBackend(DataBackend):
    skip_suspended = True

    def __init__(self):
      client = pymongo.MongoClient('localhost', 27017)
      self.db = client['quantaxis']

    @lru_cache(maxsize=4096)
    def get_price(self, order_book_id, start, end, freq):
        """
        :param order_book_id: e.g. 000002.XSHE
        :param start: 20160101
        :param end: 20160201
        :param freq: 1m 1d 5m 15m ...
        :returns:
        :rtype: numpy.rec.array
        """
        s = get_str_date_from_int(start)
        e = get_str_date_from_int(end)
        if freq != '1d':
            raise NotImplementedError

        is_index = False
        if ((order_book_id.startswith("0") and order_book_id.endswith(".XSHG")) or
            (order_book_id.startswith("3") and order_book_id.endswith(".XSHE"))):
            is_index = True

        if order_book_id.endswith(".XSHG") or order_book_id.endswith(".XSHE"):
            order_book_id = order_book_id[:6]

        L = list(self.db[is_index and 'index_day' or 'stock_day']
                    .find({'code': order_book_id, 'date': {'$gte': s,'$lte': e}},{'_id':0,'date_stamp':0}).sort('date',1))
        df = pd.DataFrame(L)
        df.rename(columns={'vol': 'volume'}, inplace=True)
        del df['code']
        df['volume'] *= 100

        if freq == '1d':
            df["datetime"] = df["date"].apply(lambda x: int(x.replace("-", "")) * 1000000)

        df = df[['date','open','close','high','low','volume','datetime']]

        arr = df.to_records()
        return arr

    def get_order_book_id_list(self):
        """获取所有的
        """
        L = list(self.db['stock_list'].find({'sec':'stock_cn', '$or': [{'sse': 'sz'}, {'sse':'sh'}]}, {'_id':0,'code':1,'sse':1}))
        return [l['sse'] == 'sz' and l['code']+'.XSHE' or l['code']+'.XSHG' for l in L]

    def get_trading_dates(self, start, end):
        """获取所有的交易日

        :param start: 20160101
        :param end: 20160201
        """

        s = get_str_date_from_int(start)
        e = get_str_date_from_int(end)

        L = list(self.db['index_day'].find({'code': '000001', 'date': { '$gte': s,'$lte': e}},
                                           {'_id':0, 'date':1}).sort('date',1))
        return [get_int_date(l['date']) for l in L]

    def symbol(self, order_book_id):
        """获取order_book_id对应的名字
        :param order_book_id str: 股票代码
        :returns: 名字
        :rtype: str
        """
        code = order_book_id
        if order_book_id.endswith(".XSHG") or order_book_id.endswith(".XSHE"):
            code = order_book_id[:6]
        L = list(self.db['stock_list'].find({'code': code, 'sec':'stock_cn'}, {'_id':0,'name':1}))
        nm= "UNKNOWN"
        if len(L) > 0: nm = L[0]['name']
        return "{}[{}]".format(order_book_id, nm)

