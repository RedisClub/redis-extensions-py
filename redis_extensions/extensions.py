# -*- coding: utf-8 -*-

import time

from redis._compat import iteritems


class RedisExtensions(object):

    def __init__(self):
        self.rate = 10 ** 13
        self.max_timestamp = 9999999999999

    def get_delete(self, conn, key):
        pipe = conn.pipeline()
        pipe.get(key)
        pipe.delete(key)
        return pipe.execute()

    def get_rename(self, conn, key, suffix='del'):
        pipe = conn.pipeline()
        pipe.get(key)
        pipe.rename(key, '{}_{}'.format(key, suffix)) if conn.exists(key) else pipe.exists(key)
        return pipe.execute()

    def multi_pop(self, conn, key, num):
        if num <= 0:
            return [[], False, 0]
        pipe = conn.pipeline()
        pipe.lrange(key, 0, num - 1)
        pipe.ltrim(key, num, -1)
        pipe.llen(key)
        return pipe.execute()

    def trim_lpush(self, conn, key, num, *values):
        pipe = conn.pipeline()
        pipe.lpush(key, *values)
        pipe.ltrim(key, 0, num - 1)
        pipe.llen(key)
        return pipe.execute()

    def trim_rpush(self, conn, key, num, *values):
        pipe = conn.pipeline()
        pipe.rpush(key, *values)
        pipe.ltrim(key, -num, - 1)
        pipe.llen(key)
        return pipe.execute()

    def timestamps(self, desc=False):
        stamp = int(time.time() * 1000)
        return self.max_timestamp - stamp if desc else stamp

    def rawscore(self, score):
        if not score:
            return 0.0
        return float(int(int(score) / self.rate))

    def stampscore(self, score, desc=False):
        return score * self.rate + self.timestamps(desc)

    def zaddwithstamps(self, conn, name, *args, **kwargs):
        desc = 'desc' in kwargs and kwargs.pop('desc')
        pieces = [item if index % 2 else self.stampscore(item, desc) for index, item in enumerate(args)]
        for pair in iteritems(kwargs):
            pieces.append(self.timestamps(pair[1], desc))
            pieces.append(pair[0])
        return conn.zadd(name, *pieces)

    def zincrbywithstamps(self, conn, name, value, amount=1, desc=False):
        return conn.zadd(name, self.stampscore(self.rawscore(conn.zscore(name, value)) + amount, desc), value)

_global_instance = RedisExtensions()

# String
get_delete = _global_instance.get_delete
get_rename = _global_instance.get_rename
# List
multi_pop = _global_instance.multi_pop
trim_lpush = _global_instance.trim_lpush
trim_rpush = _global_instance.trim_rpush
# Sorted Set
rawscore = _global_instance.rawscore
zaddwithstamps = _global_instance.zaddwithstamps
zincrbywithstamps = _global_instance.zincrbywithstamps
