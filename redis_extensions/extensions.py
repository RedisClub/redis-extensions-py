# -*- coding: utf-8 -*-

import datetime
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
        cls.rate = 10000000000000  # 10 ** 13,
        cls.max_timestamp = 9999999999999
        return super(StrictRedisExtensions, cls).__new__(cls, *args, **kwargs)

    # Keys Section
    def delete_keys(self, pattern='*', iter=False, count=None):
        """
        Delete a list of keys matching ``pattern``.

        ``iter`` if set to True, will ``scan_iter`` first then ``delete``, else will ``keys`` first then ``delete``.
        ``count`` allows for hint the minimum number of returns, ``iter`` if set to True, else the maximum number to delete once.

        Warning: ``iter`` if set to True, ``scan_iter`` will be very very very slow when keys' amount very large.
        """
        dels = 0
        while True:
            try:
                dels += self.delete(*(self.scan_iter(pattern, count) if iter else self.keys(pattern)[:count]))
            except ResponseError:
                break
            if count is None:
                break
        return dels

    def incr_limit(self, name, amount=1, limit=None, value=None):
        """
        Increments the value of ``key`` by ``amount``. If no key exists, the value will be initialized as ``amount``.
        """
        locked = self.acquire_lock(name)
        if not locked:
            return None
        amount = self.incr(name, amount)
        if limit and amount > limit:
            amount = self.decr(name, amount - (value or limit))
        self.release_lock(name, locked)
        return amount

    def decr_limit(self, name, amount=1, limit=None, value=None):
        """
        Decrements the value of ``key`` by ``amount``. If no key exists, the value will be initialized as 0 - ``amount``.
        """
        locked = self.acquire_lock(name)
        if not locked:
            return None
        amount = self.decr(name, amount)
        if limit and amount < limit:
            amount = self.decr(name, amount - (value or limit))
        self.release_lock(name, locked)
        return amount

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

    def lpush_ex(self, name, ex_time, value):
        """
        Push ``value`` that expires in ``ex_time`` seconds onto the head of the list ``name``.

        ``ex_time`` can be represented by an integer or a Python timedelta object.
        """
        if isinstance(ex_time, datetime.timedelta):
            ex_time = ex_time.seconds + ex_time.days * 24 * 3600
        return self.zadd(name, time.time() + ex_time, value)

    def lrange_ex(self, name):
        cur_time = time.time()
        return self.pipeline().zrangebyscore(name, cur_time, '+inf').zremrangebyscore(name, 0, cur_time).execute()

    # Sorted Sets Section
    def __list_substractor(self, minuend, subtrahend):
        return [x for x in minuend if x not in subtrahend]

    def zgt(self, name, value, withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores (``value`` < score < ``+inf``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        gte, eq = self.pipeline().zrangebyscore(name, value, '+inf', withscores=withscores, score_cast_func=score_cast_func).zrangebyscore(name, value, value, withscores=withscores, score_cast_func=score_cast_func).execute()
        return self.__list_substractor(gte, eq)

    def zgte(self, name, value, withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores (``value`` <= score < ``+inf``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        return self.zrangebyscore(name, value, '+inf', withscores=withscores, score_cast_func=score_cast_func)

    def zlt(self, name, value, withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores (``-inf`` < score < ``value``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        lte, eq = self.pipeline().zrangebyscore(name, '-inf', value, withscores=withscores, score_cast_func=score_cast_func).zrangebyscore(name, value, value, withscores=withscores, score_cast_func=score_cast_func).execute()
        return self.__list_substractor(lte, eq)

    def zlte(self, name, value, withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores (``-inf`` < score <= ``value``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        return self.zrangebyscore(name, '-inf', value, withscores=withscores, score_cast_func=score_cast_func)

    def zuniquerank(self, name, value):
        """
        Return a unique 0-based value indicating the rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return len(self.zlt(name, score))

    def zuniquerevrank(self, name, value):
        """
        Return a unique 0-based value indicating the descending rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return len(self.zgt(name, score))

    def zmax(self, name, withscores=False, score_cast_func=float):
        """
        Return ``max`` value from sorted set ``name``.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        try:
            return self.zrevrange(name, 0, 0, withscores=withscores, score_cast_func=score_cast_func)[0]
        except IndexError:
            return

    def zmin(self, name, withscores=False, score_cast_func=float):
        """
        Return ``min`` value from sorted set ``name``.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        try:
            return self.zrange(name, 0, 0, withscores=withscores, score_cast_func=score_cast_func)[0]
        except IndexError:
            return

    def __timestamps(self, desc=False):
        stamp = int(time.time() * 1000)
        return self.max_timestamp - stamp if desc else stamp

    def __stampscore(self, score, desc=False):
        return score * self.rate + self.__timestamps(desc)

    def rawscore(self, score):
        if not score:
            return 0.0
        return float(int(float(score) / self.rate))

    def zaddwithstamps(self, name, *args, **kwargs):
        desc = 'desc' in kwargs and kwargs.pop('desc')
        pieces = [item if index % 2 else self.__stampscore(item, desc) for index, item in enumerate(args)]
        for pair in iteritems(kwargs):
            pieces.append(self.__stampscore(pair[1], desc))
            pieces.append(pair[0])
        return self.zadd(name, *pieces)

    def zincrbywithstamps(self, name, value, amount=1, desc=False):
        return self.zadd(name, self.__stampscore(self.rawscore(self.zscore(name, value)) + amount, desc), value)

    def zrawscore(self, name, value):
        """
        Return the raw score of element ``value`` in sorted set ``name``
        """
        return self.rawscore(self.zscore(name, value))

    # Locks Section
    def acquire_lock(self, lockname, acquire_timeout=10):
        identifier = str(uuid.uuid4())

        end = time.time() + acquire_timeout
        while time.time() < end:
            if self.setnx('lock:' + lockname, identifier):
                return identifier

            time.sleep(.001)

        return False

    def release_lock(self, lockname, identifier):
        pipe = self.pipeline()
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

            locked = self.acquire_lock(identifier)
            if not locked:
                continue

            # Callbacks
            if queue in callbacks:
                callbacks[queue](name, args)

            if self.zrem(delayed, item):
                self.rpush('queue:' + queue, item)

            self.release_lock(identifier, locked)
