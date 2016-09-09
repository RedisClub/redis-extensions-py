# -*- coding: utf-8 -*-

import importlib
import json
import logging
import time
import uuid

from redis import StrictRedis
from redis._compat import iteritems
from redis.exceptions import ResponseError, WatchError


logger = logging.getLogger('redis_extensions')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class StrictRedisExtensions(StrictRedis):
    """
    Extension of [redis-py](https://github.com/andymccurdy/redis-py)'s StrictRedis.

    Support all implementations of StrictRedis and Realize some frequently used functions.
    """

    def __new__(cls, *args, **kwargs):
        cls.__rate = 10000000000000  # 10 ** 13,
        cls.__max_timestamp = 9999999999999
        return super(StrictRedisExtensions, cls).__new__(cls, *args, **kwargs)

    # Keys Section
    def delete_keys(self, pattern='*'):
        """
        Delete a list of keys matching ``pattern``.
        """
        return self.delete(*self.scan_iter(pattern))

    # Strings Section
    def get_multi(self, *names):
        """
        Return the values at keys ``names``.
        """
        pipe = self.pipeline()
        for name in names:
            pipe.get(name)
        return pipe.execute()

    def get_delete(self, name):
        """
        Return the value at key ``name``.
        Delete key ``name``.
        """
        return self.pipeline().get(name).delete(name).execute()

    def get_rename(self, name, suffix='del'):
        """
        Return the value at key ``name``.
        Rename key ``name`` to ``name_suffix``.

        ``suffix`` for rename key ``name``, default ``del``.
        """
        try:
            return self.pipeline().get(name).rename(name, '{}_{}'.format(name, suffix)).execute()
        except ResponseError:
            return [None, False]

    def get_or_set(self, name, value=None):
        """
        Return the value at key ``name``, or Set and return if the key doesn't exist.
        """
        return self.pipeline().set(name, value, nx=True).get(name).execute()[::-1]

    def get_or_setex(self, name, time, value=None):
        """
        Return the value at key ``name``, or Setex and return if the key doesn't exist.
        """
        return self.pipeline().set(name, value, ex=time, nx=True).get(name).execute()[::-1]

    # Lists Section
    def lpush_nx(self, name, value, force=True):
        """
        Push ``value`` onto the head of the list ``name`` if ``value`` not exists.

        ``force`` if set to True, will ``lrem`` first then lpush.
        """
        if force:
            return self.pipeline().lrem(name, 0, value).lpush(name, value).execute()
        else:
            if not str(value) in self.lrange(name, 0, -1):
                return self.lpush(name, value)

    def rpush_nx(self, name, value, force=True):
        """
        Push ``value`` onto the tail of the list ``name`` if ``value`` not exists.

        ``force`` if set to True, will ``lrem`` first then rpush.
        """
        if force:
            return self.pipeline().lrem(name, 0, value).rpush(name, value).execute()
        else:
            if not str(value) in self.lrange(name, 0, -1):
                return self.rpush(name, value)

    def push_nx(self, name, value, force=True):
        """
        Alias for lpush_nx.
        """
        return self.lpush_nx(name, value, force)

    def multi_lpop(self, name, num):
        """
        LPop multi items of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).ltrim(name, num, -1).llen(name).execute()

    def multi_rpop(self, name, num):
        """
        RPop multi items of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).ltrim(name, 0, -num - 1).llen(name).execute()

    def multi_pop(self, name, num):
        """
        Alias for multi_lpop.
        """
        return self.multi_lpop(name, num)

    def multi_lpop_delete(self, name, num):
        """
        LPop multi items of the list ``name``.
        Then delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).delete(name).execute()

    def multi_rpop_delete(self, name, num):
        """
        RPop multi items of the list ``name``.
        Then delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).delete(name).execute()

    def multi_pop_delete(self, name, num):
        """
        Alias for multi_lpop_delete.
        """
        return self.multi_lpop_delete(name, num)

    def trim_lpush(self, name, num, *values):
        """
        LPush ``values`` onto the head of the list ``name``.
        Limit ``num`` from the head of the list ``name``.
        """
        return self.pipeline().lpush(name, *values).ltrim(name, 0, num - 1).llen(name).execute()

    def trim_rpush(self, name, num, *values):
        """
        RPush ``values`` onto the tail of the list ``name``.
        Limit ``num`` from the tail of the list ``name``.
        """
        return self.pipeline().rpush(name, *values).ltrim(name, -num, - 1).llen(name).execute()

    def trim_push(self, name, num, *values):
        """
        Alias for trim_lpush.
        """
        return self.trim_lpush(name, num, *values)

    # Sorted Sets Section
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

    # Locks Section
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

    # Delay Tasks Section
    def execute_later(self, queue, name, args=None, delayed='delayed:default', delay=0):
        identifier = str(uuid.uuid4())

        item = json.dumps([identifier, queue, name, args])

        if delay > 0:
            self.zadd(delayed, time.time() + delay, item)
        else:
            self.rpush('queue:' + queue, item)

        return identifier

    def __callable_func(self, f):
        if callable(f):
            return f
        try:
            module, func = f.rsplit('.', 1)
            m = importlib.import_module(module)
            return getattr(m, func)
        except Exception as e:
            logger.error(e)
            return None

    def poll_queue(self, callbacks={}, delayed='delayed:default'):
        callbacks = {k: self.__callable_func(v) for k, v in iteritems(callbacks)}
        callbacks = {k: v for k, v in iteritems(callbacks) if v}

        logger.info('Available callbacks ({}):'.format(len(callbacks)))
        for k, v in iteritems(callbacks):
            logger.info('* {}: {}'.format(k, v))

        while True:
            item = self.zrange(delayed, 0, 0, withscores=True)

            if not item or item[0][1] > time.time():
                time.sleep(.01)
                continue

            logger.info(item)

            item = item[0][0]
            identifier, queue, name, args = json.loads(item)

            locked = self.__acquire_lock(identifier)
            if not locked:
                continue

            # Callbacks
            if queue in callbacks:
                callbacks[queue](name, args)

            if self.zrem(delayed, item):
                self.rpush('queue:' + queue, item)

            self.__release_lock(identifier, locked)
