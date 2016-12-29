# -*- coding: utf-8 -*-

import datetime
import importlib
import json
import logging
import re
import time as mod_time
import uuid

import vcode
from redis import StrictRedis
from redis._compat import iteritems, xrange
from redis.exceptions import ResponseError, WatchError
from redis_extensions.expires import BaseRedisExpires
from TimeConvert import TimeConvert as tc


logger = logging.getLogger('redis_extensions')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


KEY_PREFIX = 'redis:extensions:'  # Prefix of redis-extensions used key


class StrictRedisExtensions(BaseRedisExpires, StrictRedis):
    """
    Extension of [redis-py](https://github.com/andymccurdy/redis-py)'s StrictRedis.

    Support all implementations of StrictRedis and Realize some frequently used functions.
    """

    def __init__(self, *args, **kwargs):
        self.rate = 10000000000000  # 10 ** 13,
        self.max_timestamp = 9999999999999
        self.timezone = kwargs.pop('timezone', None)
        tc.__init__(timezone=self.timezone)
        super(StrictRedisExtensions, self).__init__(*args, **kwargs)

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

    def incr_cmp(self, name, amount=1, cmp='>', limit=0):
        if not re.match(r'^[><=]+$', cmp):
            raise ValueError('Cmp Value Incorrect')
        amount = self.incr(name, amount)
        return amount, eval('{}{}{}'.format(amount, cmp, limit))

    def incr_gt(self, name, amount=1, limit=0):
        amount = self.incr(name, amount)
        return amount, amount > limit

    def incr_ge(self, name, amount=1, limit=0):
        amount = self.incr(name, amount)
        return amount, amount >= limit

    def incr_eq(self, name, amount=1, limit=0):
        amount = self.incr(name, amount)
        return amount, amount == limit

    def decr_cmp(self, name, amount=1, cmp='<', limit=0):
        if not re.match(r'^[><=]+$', cmp):
            raise ValueError('Cmp Value Incorrect')
        amount = self.decr(name, amount)
        return amount, eval('{}{}{}'.format(amount, cmp, limit))

    def decr_lt(self, name, amount=1, limit=0):
        amount = self.decr(name, amount)
        return amount, amount < limit

    def decr_le(self, name, amount=1, limit=0):
        amount = self.decr(name, amount)
        return amount, amount <= limit

    def decr_eq(self, name, amount=1, limit=0):
        amount = self.decr(name, amount)
        return amount, amount == limit

    # Strings Section
    def get_delete(self, name):
        """
        Return the value at key ``name`` & Delete key ``name``.
        """
        return self.pipeline().get(name).delete(name).execute()

    def get_rename(self, name, suffix='del'):
        """
        Return the value at key ``name`` & Rename key ``name`` to ``name_suffix``.

        ``suffix`` for rename key ``name``, default ``del``.
        """
        try:
            return self.pipeline().get(name).rename(name, '{}_{}'.format(name, suffix)).execute()
        except ResponseError:
            return [None, False]

    def getsetex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time`` seconds & Returns the old value at key ``name`` atomically.

        ``time`` can be represented by an integer or a Python timedelta object.
        """
        return self.pipeline().getset(name, value).expire(name, time).execute()[0]

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

    def multi_lpop(self, name, num=1):
        """
        Pop multi items from the head of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).ltrim(name, num, -1).llen(name).execute()

    def multi_rpop(self, name, num=1):
        """
        Pop multi items from the tail of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).ltrim(name, 0, -num - 1).llen(name).execute()

    def multi_pop(self, name, num=1):
        """
        Alias for multi_lpop.
        """
        return self.multi_lpop(name, num)

    def multi_lpop_delete(self, name, num=1):
        """
        Pop multi items from the head of the list ``name`` & Delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).delete(name).execute()

    def multi_rpop_delete(self, name, num=1):
        """
        Pop multi items from the tail of the list ``name`` & Delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).delete(name).execute()

    def multi_pop_delete(self, name, num=1):
        """
        Alias for multi_lpop_delete.
        """
        return self.multi_lpop_delete(name, num)

    def trim_lpush(self, name, num, *values):
        """
        Push ``values`` onto the head of the list ``name`` & Limit ``num`` from the head of the list ``name``.
        """
        return self.pipeline().lpush(name, *values).ltrim(name, 0, num - 1).llen(name).execute()

    def trim_rpush(self, name, num, *values):
        """
        Push ``values`` onto the tail of the list ``name`` & Limit ``num`` from the tail of the list ``name``.
        """
        return self.pipeline().rpush(name, *values).ltrim(name, -num, - 1).llen(name).execute()

    def trim_push(self, name, num, *values):
        """
        Alias for trim_lpush.
        """
        return self.trim_lpush(name, num, *values)

    def delete_lpush(self, name, *values):
        """
        Delete key specified by ``name`` & Push ``values`` onto the head of the list ``name``.
        """
        return self.pipeline().delete(name).lpush(name, *values).execute()[::-1]

    def delete_rpush(self, name, *values):
        """
        Delete key specified by ``name`` & Push ``values`` onto the tail of the list ``name``.
        """
        return self.pipeline().delete(name).rpush(name, *values).execute()[::-1]

    def delete_push(self, name, *values):
        """
        Alias for delete_lpush.
        """
        return self.delete_lpush(name, *values)

    def lpush_ex(self, name, time, value):
        """
        Push ``value`` that expires in ``time`` seconds onto the head of the list ``name``.

        ``time`` can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.zadd(name, mod_time.time() + time, value)

    def lrange_ex(self, name):
        cur_time = mod_time.time()
        return self.pipeline().zrangebyscore(name, cur_time, '+inf').zremrangebyscore(name, 0, cur_time).execute()

    def sorted_pop(self, name, rank=0, sorted_func=None, reverse=True):
        # Acquire Lock
        locked = self.acquire_lock(name)
        # Get Items
        items = self.lrange(name, 0, -1)
        # Sort Items
        sorts = sorted(items, key=sorted_func, reverse=reverse)
        # Get Item
        item = sorts[rank]
        # Remove Item
        self.lrem(name, -1, item)
        # Release Lock
        self.release_lock(name, locked)
        return item

    # Sets Section
    def delete_sadd(self, name, *values):
        """
        Delete key specified by ``name`` & Add ``value(s)`` to set ``name``.
        """
        return self.pipeline().delete(name).sadd(name, *values).execute()[::-1]

    def multi_spop(self, name, num=1):
        """
        Remove and return multi random member of set ``name``.
        """
        p = self.pipeline()
        for _ in xrange(num):
            p.spop(name)
        eles = p.execute()
        return eles, sum(x is not None for x in eles)

    # ZSorts(Sorted Sets) Section
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

    def zge(self, name, value, withscores=False, score_cast_func=float):
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

    def zle(self, name, value, withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores (``-inf`` < score <= ``value``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        return self.zrangebyscore(name, '-inf', value, withscores=withscores, score_cast_func=score_cast_func)

    def zgtcount(self, name, value):
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``value`` < score < ``+inf``).
        """
        ge, eq = self.pipeline().zcount(name, value, '+inf').zcount(name, value, value).execute()
        return ge - eq

    def zgecount(self, name, value):
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``value`` <= score < ``+inf``).
        """
        return self.zcount(name, value, '+inf')

    def zltcount(self, name, value):
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``-inf`` < score < ``value``).
        """
        le, eq = self.pipeline().zcount(name, '-inf', value).zcount(name, value, value).execute()
        return le - eq

    def zlecount(self, name, value):
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``-inf`` < score <= ``value``).
        """
        return self.zcount(name, '-inf', value)

    def zuniquerank(self, name, value):
        """
        Return a unique 0-based value indicating the rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return self.zltcount(name, score)

    def zuniquerevrank(self, name, value):
        """
        Return a unique 0-based value indicating the descending rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return self.zgtcount(name, score)

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
        stamp = int(mod_time.time() * 1000)
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
    def acquire_lock(self, lockname, ex=None, acquire_timeout=10):
        """
        Acquire lock for ``lockname``.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``acquire_timeout`` indicates retry time of acquiring lock.
        """
        identifier = str(uuid.uuid4())
        end = mod_time.time() + acquire_timeout
        while mod_time.time() < end:
            if self.set(KEY_PREFIX + 'lock:' + lockname, identifier, ex=ex, nx=True):
                return identifier
            mod_time.sleep(.001)
        return False

    def release_lock(self, lockname, identifier):
        """
        Release lock for ``lockname``.
        """
        lockname = KEY_PREFIX + 'lock:' + lockname
        pipe = self.pipeline()
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

    def lock_exists(self, lockname, regex=False):
        """
        Check lock for ``lockname`` exists or not.
        """
        lockname = KEY_PREFIX + 'lock:' + lockname
        return self.keys(lockname) if regex else self.exists(lockname)

    # SignIns Section
    def __get_signin_info(self, signname):
        """
        signin_info:
            signin_date
            signin_days
            signin_total_days
            signin_longest_days
        """
        name = '{}signin:info:{}'.format(KEY_PREFIX, signname)
        # Signin Info
        signin_info = json.loads(self.get(name) or '{}')
        # Last Signin Date, Format ``%Y-%m-%d``
        last_signin_date = signin_info.get('signin_date', '1988-06-15')
        # Today Local Date, Format ``%Y-%m-%d``
        signin_date = tc.local_string(format='%Y-%m-%d')
        # Delta Days between ``Last Signin Date`` and ``Today Local Date``
        delta_days = tc.string_delta(signin_date, last_signin_date, format='%Y-%m-%d')['days']
        return name, signin_info, signin_date, last_signin_date, delta_days

    def signin(self, signname):
        name, signin_info, signin_date, _, delta_days = self.__get_signin_info(signname)
        # Today Unsigned, To Signin
        if delta_days != 0:
            # If Uncontinuous
            if delta_days != 1:
                signin_info['signin_days'] = 0
            # Update Signin Info
            signin_info['signin_date'] = signin_date
            signin_info['signin_days'] = signin_info.get('signin_days', 0) + 1
            signin_info['signin_total_days'] = signin_info.get('signin_total_days', 0) + 1
            signin_info['signin_longest_days'] = max(signin_info.get('signin_longest_days', 0), signin_info['signin_days'])
            self.set(name, json.dumps(signin_info))
        return dict(signin_info, signed_today=True, delta_days=delta_days)

    def signin_status(self, signname):
        _, signin_info, _, last_signin_date, delta_days = self.__get_signin_info(signname)
        if delta_days == 0:  # Today Signed
            return dict(signin_info, signed_today=True, delta_days=delta_days)
        return {
            'signed_today': False,
            'signin_date': last_signin_date,
            'signin_days': 0 if delta_days != 1 else signin_info.get('signin_days', 0),
            'signin_total_days': signin_info.get('signin_total_days', 0),
            'signin_longest_days': signin_info.get('signin_longest_days', 0),
            'delta_days': delta_days,
        }

    # Token
    def token(self, name, time=1800, token_generate_func=None):
        """
        Generate token.

        ``time`` indicates expire time of generating code, which can be represented by an integer or a Python timedelta object, Default: 30 minutes.

        ``token_generate_func`` a callable used to generate the token.
        """
        code = token_generate_func() if token_generate_func else str(uuid.uuid4())
        token_key = '{}token:{}'.format(KEY_PREFIX, name)
        self.setex(token_key, time, code)
        return code

    def token_exists(self, name, code):
        """
        Check token code exists or not.
        """
        token_key = '{}token:{}'.format(KEY_PREFIX, name)
        return self.get(token_key) == str(code)

    # Verification Codes Section
    def __black_list(self, value, cate='phone'):
        black_key = '{}vcode:{}:black:list'.format(KEY_PREFIX, cate)
        return self.sismember(black_key, value)

    def __quota_incr(self, value, cate='phone', quota=10):
        quota_key = '{}vcode:{}:quota:{}'.format(KEY_PREFIX, cate, value)
        num = self.incr(quota_key)
        if num == 1:
            self.expire(quota_key, 86400)  # Only can called ``quota`` num within 24 hours.
        return num > quota

    def __quota_num(self, value, cate='phone'):
        quota_key = '{}vcode:{}:quota:{}'.format(KEY_PREFIX, cate, value)
        return int(self.get(quota_key) or 0)

    def __req_interval(self, value, cate='phone', req_interval=60):
        req_stamp_key = '{}vcode:{}:req:stamp:{}'.format(KEY_PREFIX, cate, value)
        curstamp = tc.utc_timestamp(ms=False)
        laststamp = int(self.getset(req_stamp_key, curstamp) or 0)
        if curstamp - laststamp < req_interval:
            black_key = '{}vcode:{}:black:list'.format(KEY_PREFIX, cate)
            self.sadd(black_key, value)
            return True
        return False

    def vcode(self, phone, ipaddr=None, quota=10, req_interval=60, black_list=True, ndigits=6, time=1800, code_cast_func=str):
        """
        Generate verification code if not reach quota. Return a 2-item tuple: (Verification code, Whether reach quota or not, Whether in black list or not).

        ``quota`` indicates limitation of generating code, ``phone`` and ``ipaddr`` use in common, 0 for limitlessness.

        ``req_interval`` indicates interval of two request, ``phone`` and ``ipaddr`` use in common, 0 for limitlessness.

        ``black_list`` indicates whether check black list or not.

        ``ndigits`` indicates length of generated code.

        ``time`` indicates expire time of generating code, which can be represented by an integer or a Python timedelta object, Default: 30 minutes.

        ``code_cast_func`` a callable used to cast the code return value.

        ``black_list`` - ``redis:extensions:vcode:phone:black:list`` & ``redis:extensions:vcode:ipaddr:black:list``
        """
        # Black List Check
        if black_list:
            if self.__black_list(phone, cate='phone') or (ipaddr and self.__black_list(ipaddr, cate='ipaddr')):
                return None, None, True
        # Quota Check
        if quota:
            # Phone Quota
            if self.__quota_incr(phone, cate='phone', quota=quota):
                return None, True, None
            # Ipaddr Quota If `ipaddr`` Isn't ``None``
            if ipaddr:
                if self.__quota_incr(ipaddr, cate='ipaddr', quota=quota):
                    return None, True, None
        # Req Interval Check
        if req_interval:
            # Phone Interval
            if self.__req_interval(phone, cate='phone', req_interval=req_interval):
                return None, False, True
            # Ipaddr Interval If `ipaddr`` Isn't ``None``
            if ipaddr:
                if self.__req_interval(ipaddr, cate='ipaddr', req_interval=req_interval):
                    return None, False, True
        code = vcode.digits(ndigits=ndigits, code_cast_func=code_cast_func)
        vcode_key = '{}vcode:{}'.format(KEY_PREFIX, phone)
        self.setex(vcode_key, time, code)
        return code, False, False

    def vcode_quota(self, phone=None, ipaddr=None):
        if phone and not ipaddr:
            return self.__quota_num(phone, cate='phone')
        if not phone and ipaddr:
            return self.__quota_num(ipaddr, cate='ipaddr')
        return self.__quota_num(phone, cate='phone'), self.__quota_num(ipaddr, cate='ipaddr')

    def vcode_exists(self, phone, code):
        """
        Check verification code exists or not.
        """
        vcode_key = '{}vcode:{}'.format(KEY_PREFIX, phone)
        return self.get(vcode_key) == str(code)

    # Delay Tasks Section
    def execute_later(self, queue, name, args=None, delayed=KEY_PREFIX + 'delayed:default', delay=0):
        identifier = str(uuid.uuid4())

        item = json.dumps([identifier, queue, name, args])

        if delay > 0:
            self.zadd(delayed, mod_time.time() + delay, item)
        else:
            self.rpush(KEY_PREFIX + 'queue:' + queue, item)

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

    def poll_queue(self, callbacks={}, delayed=KEY_PREFIX + 'delayed:default', unlocked_warning_func=None):
        callbacks = {k: self.__callable_func(v) for k, v in iteritems(callbacks)}
        callbacks = {k: v for k, v in iteritems(callbacks) if v}

        logger.info('Available callbacks ({}):'.format(len(callbacks)))
        for k, v in iteritems(callbacks):
            logger.info('* {}: {}'.format(k, v))

        while True:
            item = self.zrange(delayed, 0, 0, withscores=True)

            if not item or item[0][1] > mod_time.time():
                mod_time.sleep(.01)
                continue

            logger.info(item)

            item = item[0][0]
            identifier, queue, name, args = json.loads(item)

            locked = self.acquire_lock(identifier)
            if not locked:
                # Call ``unlocked_warning_func`` if exists when ``acquire_lock`` fails
                if unlocked_warning_func:
                    unlocked_warning_func(queue, name, args)
                continue

            # Callbacks
            if queue in callbacks:
                callbacks[queue](name, args)

            if self.zrem(delayed, item):
                self.rpush(KEY_PREFIX + 'queue:' + queue, item)

            self.release_lock(identifier, locked)

    # For rename official function
    def georem(self, name, *values):
        return self.zrem(name, *values)

    # For naming conventions compatibility, order by define
    deletekeys = delete_keys
    incrlimit = incr_limit
    decrlimit = decr_limit
    incrcmp = incr_cmp
    incrgt = incr_gt
    incrge = incr_ge
    increq = incr_eq
    decrcmp = decr_cmp
    decrlt = decr_lt
    decrle = decr_le
    decreq = decr_eq
    getdelete = get_delete
    getrename = get_rename
    getorset = get_or_set
    getorsetex = get_or_setex
    lpushnx = lpush_nx
    rpushnx = rpush_nx
    pushnx = push_nx
    multilpop = multi_lpop
    multirpop = multi_rpop
    multipop = multi_pop
    multilpopdelete = multi_lpop_delete
    multirpopdelete = multi_rpop_delete
    multipopdelete = multi_pop_delete
    trimlpush = trim_lpush
    trimrpush = trim_rpush
    trimpush = trim_push
    deletelpush = delete_lpush
    deleterpush = delete_rpush
    deletepush = delete_push
    lpushex = lpush_ex
    lrangeex = lrange_ex
    sortedpop = sorted_pop
    deletesadd = delete_sadd
    multispop = multi_spop

    # For backwards compatibility
    zgte = zge
    zlte = zle
    vcode_status = vcode_exists
