# -*- coding: utf-8 -*-

import time

from redis import StrictRedis
from redis._compat import iteritems


class StrictRedisExtensions(StrictRedis):

    def __new__(cls, *args, **kwargs):
        # Private Var
        cls.__rate = 10000000000000  # 10 ** 13,
        # Private Var
        cls.__max_timestamp = 9999999999999
        return super(StrictRedisExtensions, cls).__new__(cls, *args, **kwargs)

    def get_delete(self, key):
        pipe = self.pipeline()
        pipe.get(key)
        pipe.delete(key)
        return pipe.execute()

    def get_rename(self, key, suffix='del'):
        pipe = self.pipeline()
        pipe.get(key)
        pipe.rename(key, '{}_{}'.format(key, suffix)) if self.exists(key) else pipe.exists(key)
        return pipe.execute()

    def multi_pop(self, key, num):
        if num <= 0:
            return [[], False, 0]
        pipe = self.pipeline()
        pipe.lrange(key, 0, num - 1)
        pipe.ltrim(key, num, -1)
        pipe.llen(key)
        return pipe.execute()

    def trim_lpush(self, key, num, *values):
        pipe = self.pipeline()
        pipe.lpush(key, *values)
        pipe.ltrim(key, 0, num - 1)
        pipe.llen(key)
        return pipe.execute()

    def trim_rpush(self, key, num, *values):
        pipe = self.pipeline()
        pipe.rpush(key, *values)
        pipe.ltrim(key, -num, - 1)
        pipe.llen(key)
        return pipe.execute()

    # Private Function
    def __timestamps(self, desc=False):
        stamp = int(time.time() * 1000)
        return self.__max_timestamp - stamp if desc else stamp

    def rawscore(self, score):
        if not score:
            return 0.0
        return float(int(float(score) / self.__rate))

    # Private Function
    def __stampscore(self, score, desc=False):
        return score * self.__rate + self.__timestamps(desc)

    def zaddwithstamps(self, name, *args, **kwargs):
        desc = 'desc' in kwargs and kwargs.pop('desc')
        pieces = [item if index % 2 else self.__stampscore(item, desc) for index, item in enumerate(args)]
        for pair in iteritems(kwargs):
            pieces.append(self.__timestamps(pair[1], desc))
            pieces.append(pair[0])
        return self.zadd(name, *pieces)

    def zincrbywithstamps(self, name, value, amount=1, desc=False):
        return self.zadd(name, self.__stampscore(self.rawscore(self.zscore(name, value)) + amount, desc), value)
