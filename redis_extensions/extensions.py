# -*- coding: utf-8 -*-

import json
import time
import uuid

from redis import StrictRedis
from redis._compat import iteritems
from redis.exceptions import WatchError


class StrictRedisExtensions(StrictRedis):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """

    def __new__(cls, *args, **kwargs):
        cls.__rate = 10000000000000  # 10 ** 13,
        cls.__max_timestamp = 9999999999999
        return super(StrictRedisExtensions, cls).__new__(cls, *args, **kwargs)

    # String Section
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

    # List Section
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

    # Sorted Set Section
    def __timestamps(self, desc=False):
        stamp = int(time.time() * 1000)
        return self.__max_timestamp - stamp if desc else stamp

    def __stampscore(self, score, desc=False):
        return score * self.__rate + self.__timestamps(desc)

    def rawscore(self, score):
        if not score:
            return 0.0
        return float(int(float(score) / self.__rate))

    def zaddwithstamps(self, name, *args, **kwargs):
        desc = 'desc' in kwargs and kwargs.pop('desc')
        pieces = [item if index % 2 else self.__stampscore(item, desc) for index, item in enumerate(args)]
        for pair in iteritems(kwargs):
            pieces.append(self.__timestamps(pair[1], desc))
            pieces.append(pair[0])
        return self.zadd(name, *pieces)

    def zincrbywithstamps(self, name, value, amount=1, desc=False):
        return self.zadd(name, self.__stampscore(self.rawscore(self.zscore(name, value)) + amount, desc), value)

    # Lock Section
    def __acquire_lock(self, lockname, acquire_timeout=10):
        identifier = str(uuid.uuid4())

        end = time.time() + acquire_timeout
        while time.time() < end:
            if self.setnx('lock:' + lockname, identifier):
                return identifier

            time.sleep(.001)

        return False

    def __release_lock(self, lockname, identifier):
        pipe = self.pipeline(True)
        lockname = 'lock:' + lockname

        while True:
            try:
                pipe.watch(lockname)
                if pipe.get(lockname) == identifier:
                    pipe.multi()
                    pipe.delete(lockname)
                    pipe.execute()
                    return True

                pipe.unwatch()
                break
            except WatchError:
                pass

        return False

    # Delay_task Section
    def execute_later(self, queue, name, args=None, delayed='delayed:default', delay=0):
        identifier = str(uuid.uuid4())

        item = json.dumps([identifier, queue, name, args])

        if delay > 0:
            self.zadd(delayed, time.time() + delay, item)
        else:
            self.rpush('queue:' + queue, item)

        return identifier

    def poll_queue(self, callbacks={}, delayed='delayed:default'):
        while True:
            item = self.zrange(delayed, 0, 0, withscores=True)

            if not item or item[0][1] > time.time():
                time.sleep(.01)
                continue

            item = item[0][0]
            identifier, queue, name, args = json.loads(item)

            locked = self.__acquire_lock(identifier)
            if not locked:
                continue

            # Callbacks
            for queue in callbacks:
                callbacks[queue](name, args)

            if self.zrem(delayed, item):
                self.rpush('queue:' + queue, item)

            self.__release_lock(identifier, locked)
