import datetime
import importlib
import json
import logging
import random
import re
import signal
import socket
import time as mod_time
import uuid
import warnings
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Type, Union

import gvcode
import shortuuid
import vcode as mod_vcode
from CodeConvert import CodeConvert as cc
from redis import StrictRedis
from redis.client import bool_ok
from redis.exceptions import DataError, ResponseError, WatchError
from redis.typing import AnyKeyT, EncodableT, ExpiryT, FieldT, KeyT, ZScoreBoundT
from TimeConvert import TimeConvert as tc

from .expires import BaseRedisExpires


ResponseT = Union[Awaitable, Any]
ResponseIntT = Union[Awaitable[int], int]
ResponseStrT = Union[Awaitable[str], str]
ResponseBoolT = Union[Awaitable[bool], bool]
ResponseListT = Union[Awaitable[list], list]
ResponseZ = Union[List[str], List[Tuple[str, float]]]
ResponseJSON = Union[List, Dict[str, Any]]


logger = logging.getLogger('redis_extensions')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


KEY_PREFIX = 'r:'  # Prefix of redis-extensions used key
WARNING_LOG = '``{0}`` used, may be very very very slow when keys\' amount very large'  # ``r.keys()`` and ``r.scan_iter()`` not support use


POLL_QUEUE_CONTINUE_FLAG = True


# Signal Handler
def sigintHandler(signum, frame):
    global POLL_QUEUE_CONTINUE_FLAG
    POLL_QUEUE_CONTINUE_FLAG = False


# signal.SIGKILL, `KILL -9`, unblockable
for signum in [signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGTSTP]:
    signal.signal(signum, sigintHandler)


# Get the local ip
def get_network_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.connect(('<broadcast>', 0))
        ip = s.getsockname()[0]
        s.close()
    except OSError:
        ip = '127.0.0.1'
    return ip


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

    def __str(self, x: Any) -> Union[str, bytes]:
        if isinstance(x, int):
            return str(x)
        return x if isinstance(x, (str, bytes)) else bytes(x)

    def __local_ymd(self, format: str = '%Y-%m-%d') -> str:
        return tc.local_string(format=format)

    def __uuid(self, short_uuid: bool = False) -> str:
        return shortuuid.uuid() if short_uuid else uuid.uuid4().hex

    # Keys Section(Delete Relative)
    def delete_keys(self, pattern: str = '*', iter: bool = False, count: Optional[int] = None) -> int:
        """
        Delete a list of keys matching ``pattern``.

        ``iter`` if set to True, will ``scan_iter`` first then ``delete``, else will ``keys`` first then ``delete``.

        ``count`` allows for hint the minimum number of returns, ``iter`` if set to True, else the maximum number to delete once.

        Warning: ``iter`` if set to True, ``scan_iter`` will be very very very slow when keys' amount very large.
        """
        logger.warning(WARNING_LOG.format('r.scan_iter()' if iter else 'r.keys()'))
        dels = 0
        while True:
            try:
                dels += self.delete(*(self.scan_iter(pattern, count) if iter else self.keys(pattern)[:count]))
            except ResponseError:
                break
            if count is None:
                break
        return dels

    def __todel(self, key: str, matched_list: List) -> bool:
        for matched in matched_list:
            if matched in key:
                return False
        return True

    def delete_unmatched_keys(self, pattern: str = '*', matched_list: List = [], iter: bool = False) -> int:
        logger.warning('Not use in production, this func is just for manual delete keys which unused for yonks')
        dels = 0
        keys = self.scan_iter(pattern, dels) if iter else self.keys(pattern)
        for key in keys:
            if self.__todel(key, matched_list):
                dels += self.delete(key)
        return dels

    def delete_yonks_unused_keys(self, pattern: str = '*', iter: bool = False, idletime: int = 86400) -> int:
        logger.warning('Not use in production, this func is just for manual delete keys which unused for yonks')
        dels = 0
        keys = self.scan_iter(pattern, dels) if iter else self.keys(pattern)
        for key in keys:
            if self.object('idletime', key) > idletime:
                dels += self.delete(key)
        return dels

    # Keys Section(Incr/Decr Relative)
    def incr_limit(self, name: str, amount: int = 1, limit: Optional[int] = None, value: Optional[int] = None) -> Optional[int]:
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

    def decr_limit(self, name: str, amount: int = 1, limit: Optional[int] = None, value: Optional[int] = None) -> Optional[int]:
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

    def incr_cmp(self, name: str, amount: int = 1, cmp: str = '>', limit: int = 0) -> Tuple[int, bool]:
        if not re.match(r'^[><=]+$', cmp):
            raise ValueError('Cmp Value Incorrect')
        amount = self.incr(name, amount)
        return amount, eval('{0}{1}{2}'.format(amount, cmp, limit))

    def incr_gt(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.incr(name, amount)
        return amount, amount > limit

    def incr_ge(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.incr(name, amount)
        return amount, amount >= limit

    def incr_eq(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.incr(name, amount)
        return amount, amount == limit

    def decr_cmp(self, name: str, amount: int = 1, cmp: str = '<', limit: int = 0) -> Tuple[int, bool]:
        if not re.match(r'^[><=]+$', cmp):
            raise ValueError('Cmp Value Incorrect')
        amount = self.decr(name, amount)
        return amount, eval('{0}{1}{2}'.format(amount, cmp, limit))

    def decr_lt(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.decr(name, amount)
        return amount, amount < limit

    def decr_le(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.decr(name, amount)
        return amount, amount <= limit

    def decr_eq(self, name: str, amount: int = 1, limit: int = 0) -> Tuple[int, bool]:
        amount = self.decr(name, amount)
        return amount, amount == limit

    # # Keys Section(Rename Relative)
    def quiet_rename(self, src: KeyT, dst: KeyT) -> ResponseBoolT:
        # if self.exists(src):
        #     try:
        #         return self.rename(src, dst)
        #     except ResponseError:
        #         pass
        # return False
        quiet_rename_script = """
        if redis.call('exists', KEYS[1]) == 1 then
            return redis.call('rename', KEYS[1], KEYS[2])
        else
            return ''
        end"""
        return bool_ok(self.eval(quiet_rename_script, 2, src, dst))

    # Strings Section
    def get_delete(self, name: str) -> Tuple[ResponseT, ResponseT]:
        """
        Return the value at key ``name`` & Delete key ``name``.
        """
        return self.pipeline().get(name).delete(name).execute()

    def get_rename(self, name: str, suffix: str = 'del') -> Tuple[ResponseT, ResponseT]:
        """
        Return the value at key ``name`` & Rename key ``name`` to ``name_suffix``.

        ``suffix`` for rename key ``name``, default ``del``.
        """
        try:
            return self.pipeline().get(name).renamenx(name, '{0}_{1}'.format(name, suffix)).execute()
        except ResponseError:
            return [None, False]

    def getsetex(self, name: str, time: ExpiryT, value: EncodableT) -> Tuple[ResponseT, ResponseT]:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time`` seconds & Returns the old value at key ``name`` atomically.

        ``time`` can be represented by an integer or a Python timedelta object.
        """
        # GETSET
        # As of Redis version 6.2.0, this command is regarded as deprecated.
        # It can be replaced by SET with the GET argument when migrating or writing new code.
        warnings.warn(
            f"{self.__class__.__name__}.getsetex() is deprecated since Redis version 6.2.0. "
            f"Use {self.__class__.__name__}.set() with the GET/EX argument instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.pipeline().getset(name, value).expire(name, time).execute()[0]

    def get_or_set(self, name: str, value: Optional[EncodableT] = None) -> Tuple[ResponseT, ResponseT]:
        """
        Return the value at key ``name``, or Set and return if the key doesn't exist.
        """
        # GETSET
        # As of Redis version 6.2.0, this command is regarded as deprecated.
        # It can be replaced by SET with the GET argument when migrating or writing new code.
        warnings.warn(
            f"{self.__class__.__name__}.getorset() is deprecated since Redis version 6.2.0. "
            f"Use {self.__class__.__name__}.set() with the GET/NX argument instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.pipeline().set(name, value, nx=True).get(name).execute()[::-1]

    def get_or_setex(self, name: str, time: ExpiryT, value: Optional[EncodableT] = None) -> Tuple[ResponseT, ResponseT]:
        """
        Return the value at key ``name``, or Setex and return if the key doesn't exist.
        """
        # GETSET
        # As of Redis version 6.2.0, this command is regarded as deprecated.
        # It can be replaced by SET with the GET argument when migrating or writing new code.
        warnings.warn(
            f"{self.__class__.__name__}.getorsetex() is deprecated since Redis version 6.2.0. "
            f"Use {self.__class__.__name__}.set() with the GET/EX/NX argument instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.pipeline().set(name, value, ex=time, nx=True).get(name).execute()[::-1]

    # Lists Section
    def lpush_nx(self, name: str, value: str, force: bool = True) -> ResponseIntT:
        """
        Push ``value`` onto the head of the list ``name`` if ``value`` not exists.

        ``force`` if set to True, will ``lrem`` first then lpush.
        """
        if force:
            return self.pipeline().lrem(name, 0, value).lpush(name, value).execute()[-1]
        else:
            if not self.__str(value) in self.lrange(name, 0, -1):
                return self.lpush(name, value)

    def rpush_nx(self, name: str, value: str, force: bool = True) -> ResponseIntT:
        """
        Push ``value`` onto the tail of the list ``name`` if ``value`` not exists.

        ``force`` if set to True, will ``lrem`` first then rpush.
        """
        if force:
            return self.pipeline().lrem(name, 0, value).rpush(name, value).execute()[-1]
        else:
            if not self.__str(value) in self.lrange(name, 0, -1):
                return self.rpush(name, value)

    def multi_lpop(self, name: str, num: int = 1) -> Tuple[ResponseListT, ResponseStrT, ResponseIntT]:
        """
        Pop multi items from the head of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).ltrim(name, num, -1).llen(name).execute()

    def multi_rpop(self, name: str, num: int = 1) -> Tuple[ResponseListT, ResponseStrT, ResponseIntT]:
        """
        Pop multi items from the tail of the list ``name``.
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).ltrim(name, 0, -num - 1).llen(name).execute()

    def multi_lpop_delete(self, name: str, num: int = 1) -> Tuple[ResponseListT, ResponseT]:
        """
        Pop multi items from the head of the list ``name`` & Delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, 0, num - 1).delete(name).execute()

    def multi_rpop_delete(self, name: str, num: int = 1) -> Tuple[ResponseListT, ResponseT]:
        """
        Pop multi items from the tail of the list ``name`` & Delete the list ``name``
        """
        if num < 0:
            raise ValueError('The num argument should not be negative')
        return self.pipeline().lrange(name, -num, -1).delete(name).execute()

    def trim_lpush(self, name: str, num: int, *values: FieldT) -> Tuple[ResponseIntT, ResponseStrT, ResponseIntT]:
        """
        Push ``values`` onto the head of the list ``name`` & Limit ``num`` from the head of the list ``name``.
        """
        return self.pipeline().lpush(name, *values).ltrim(name, 0, num - 1).llen(name).execute()

    def trim_rpush(self, name: str, num: int, *values: FieldT) -> Tuple[ResponseIntT, ResponseStrT, ResponseIntT]:
        """
        Push ``values`` onto the tail of the list ``name`` & Limit ``num`` from the tail of the list ``name``.
        """
        return self.pipeline().rpush(name, *values).ltrim(name, -num, - 1).llen(name).execute()

    def delete_lpush(self, name: str, *values: FieldT) -> Tuple[ResponseIntT, ResponseT]:
        """
        Delete key specified by ``name`` & Push ``values`` onto the head of the list ``name``.
        """
        return self.pipeline().delete(name).lpush(name, *values).execute()[::-1]

    def delete_rpush(self, name: str, *values: FieldT) -> Tuple[ResponseIntT, ResponseT]:
        """
        Delete key specified by ``name`` & Push ``values`` onto the tail of the list ``name``.
        """
        return self.pipeline().delete(name).rpush(name, *values).execute()[::-1]

    def lpush_ex(self, name: str, time: ExpiryT, value: AnyKeyT) -> ResponseT:
        """
        Push ``value`` that expires in ``time`` seconds onto the head of the list ``name``.

        ``time`` can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.zadd(name, {value: mod_time.time() + time})

    def lrange_ex(self, name: str) -> Tuple[ResponseT, ResponseT]:
        cur_time = mod_time.time()
        return self.pipeline().zrangebyscore(name, cur_time, '+inf').zremrangebyscore(name, 0, cur_time).execute()

    def sorted_pop(self, name: str, rank: int = 0, sorted_func: Optional[Union[type, Callable]] = None, reverse: bool = True) -> Optional[str]:
        # Acquire Lock
        locked = self.acquire_lock(name)
        # Get Items
        items = self.lrange(name, 0, -1)
        # IndexError: list index out of range
        # Raises IndexError => Return None
        if rank > len(items) - 1:
            return
        # Sort Items
        sorts = sorted(items, key=sorted_func, reverse=reverse)
        # Get Item
        item = sorts[rank]
        # Remove Item
        self.lrem(name, -1, item)
        # Release Lock
        self.release_lock(name, locked)
        return item

    def blpopv(self, keys: List, timeout: Optional[int] = 0) -> ResponseListT:
        kv = self.blpop(keys, timeout)
        if not kv:
            return None
        return kv[-1]

    def brpopv(self, keys: List, timeout: Optional[int] = 0) -> ResponseListT:
        kv = self.brpop(keys, timeout)
        if not kv:
            return None
        return kv[-1]

    # Sets Section
    def delete_sadd(self, name: str, *values: FieldT) -> Tuple[ResponseIntT, ResponseT]:
        """
        Delete key specified by ``name`` & Add ``value(s)`` to set ``name``.
        """
        return self.pipeline().delete(name).sadd(name, *values).execute()[::-1]

    def multi_spop(self, name: str, num: int = 1) -> Tuple[List[Optional[str]], int]:
        """
        Remove and return multi random member of set ``name``.
        """
        # SPOP
        # Starting with Redis version 3.2.0: Added the count argument.
        warnings.warn(
            f"{self.__class__.__name__}.multispop() is deprecated since Redis version 3.2.0. "
            f"Use {self.__class__.__name__}.spop() with the COUNT argument instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        p = self.pipeline()
        for _ in range(num):
            p.spop(name)
        eles = p.execute()
        return eles, sum(x is not None for x in eles)

    def srandmember_shuffle(self, name: str, number: Optional[int] = None) -> Union[str, List, None]:
        # https://github.com/antirez/redis/blob/e4903ce586c191fe4699913a5e360e12812024a3/src/t_set.c#L616
        # Srandmember isn't random enough
        # When number is close to ``the number of elements inside the set``
        memebers = self.srandmember(name, number=number)
        if memebers:
            random.shuffle(memebers)
        return memebers

    # ZSorts(Sorted Sets) Section
    def __list_substractor(self, minuend: ResponseZ, subtrahend: ResponseZ) -> ResponseZ:
        return [x for x in minuend if x not in subtrahend]

    def zgt(self, name: str, value: ZScoreBoundT, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseZ:
        """
        Return a range of values from the sorted set ``name`` with scores (``value`` < score < ``+inf``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        gte, eq = self.pipeline().zrangebyscore(name, value, '+inf', withscores=withscores, score_cast_func=score_cast_func).zrangebyscore(name, value, value, withscores=withscores, score_cast_func=score_cast_func).execute()
        return self.__list_substractor(gte, eq)

    def zge(self, name: str, value: ZScoreBoundT, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseT:
        """
        Return a range of values from the sorted set ``name`` with scores (``value`` <= score < ``+inf``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        return self.zrangebyscore(name, value, '+inf', withscores=withscores, score_cast_func=score_cast_func)

    def zlt(self, name: str, value: ZScoreBoundT, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseZ:
        """
        Return a range of values from the sorted set ``name`` with scores (``-inf`` < score < ``value``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        lte, eq = self.pipeline().zrangebyscore(name, '-inf', value, withscores=withscores, score_cast_func=score_cast_func).zrangebyscore(name, value, value, withscores=withscores, score_cast_func=score_cast_func).execute()
        return self.__list_substractor(lte, eq)

    def zle(self, name: str, value: ZScoreBoundT, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseT:
        """
        Return a range of values from the sorted set ``name`` with scores (``-inf`` < score <= ``value``).

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        return self.zrangebyscore(name, '-inf', value, withscores=withscores, score_cast_func=score_cast_func)

    def zgtcount(self, name: str, value: ZScoreBoundT) -> int:
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``value`` < score < ``+inf``).
        """
        ge, eq = self.pipeline().zcount(name, value, '+inf').zcount(name, value, value).execute()
        return ge - eq

    def zgecount(self, name: str, value: ZScoreBoundT) -> ResponseT:
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``value`` <= score < ``+inf``).
        """
        return self.zcount(name, value, '+inf')

    def zltcount(self, name: str, value: ZScoreBoundT) -> int:
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``-inf`` < score < ``value``).
        """
        le, eq = self.pipeline().zcount(name, '-inf', value).zcount(name, value, value).execute()
        return le - eq

    def zlecount(self, name: str, value: ZScoreBoundT) -> ResponseT:
        """
        Returns the number of elements in the sorted set at key ``name`` with scores (``-inf`` < score <= ``value``).
        """
        return self.zcount(name, '-inf', value)

    def zuniquerank(self, name: str, value: EncodableT) -> Optional[int]:
        """
        Return a unique 0-based value indicating the rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return self.zltcount(name, score)

    def zuniquerevrank(self, name: str, value: EncodableT) -> Optional[int]:
        """
        Return a unique 0-based value indicating the descending rank of ``value`` in sorted set ``name``.
        """
        score = self.zscore(name, value)
        if not score:
            return
        return self.zgtcount(name, score)

    def ztopn(self, name: str, count: int, desc: bool = True, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseZ:
        return self.zrange(name, 0, count - 1, desc=desc, withscores=withscores, score_cast_func=score_cast_func)

    def zistopn(self, name: str, value: str, count: int) -> bool:
        return value in self.ztopn(name, count, withscores=False)

    def zmax(self, name: str, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> Union[str, Tuple[str, float]]:
        """
        Return ``max`` value from sorted set ``name``.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        try:
            return self.ztopn(name, 1, desc=True, withscores=withscores, score_cast_func=score_cast_func)[0]
        except IndexError:
            return

    def zmin(self, name: str, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> Union[str, Tuple[str, float]]:
        """
        Return ``min`` value from sorted set ``name``.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.
        """
        try:
            return self.ztopn(name, 1, desc=False, withscores=withscores, score_cast_func=score_cast_func)[0]
        except IndexError:
            return

    def __timestamps(self, desc: bool = False) -> int:
        stamp = int(mod_time.time() * 1000)
        return self.max_timestamp - stamp if desc else stamp

    def __stampscore(self, score: float, desc: bool = False) -> float:
        return score * self.rate + self.__timestamps(desc)

    def rawscore(self, score: Union[str, int, float]) -> float:
        if not score:
            return 0.0
        return float(int(float(score) / self.rate))

    def zaddwithstamps(self, name: str, *args: List, **kwargs: Dict) -> ResponseT:
        desc = 'desc' in kwargs and kwargs.pop('desc')
        mapping = kwargs.pop('mapping') if 'mapping' in kwargs else {}
        for idx, item in enumerate(args):
            if idx % 2:
                mapping[item] = self.__stampscore(args[idx + 1], desc)
        for k, v in kwargs.items():
            mapping[k] = self.__stampscore(v, desc)
        return self.zadd(name, mapping)

    def zincrbywithstamps(self, name: str, value: EncodableT, amount: int = 1, desc: bool = False) -> ResponseT:
        return self.zadd(name, {value: self.__stampscore(self.rawscore(self.zscore(name, value)) + amount, desc)})

    def zrawscore(self, name: str, value: EncodableT) -> float:
        """
        Return the raw score of element ``value`` in sorted set ``name``
        """
        return self.rawscore(self.zscore(name, value))

    # Hash Section
    def hincrbyex(self, name: str, key: str, amount: int = 1, time: ExpiryT = 1800) -> Tuple[ResponseIntT, Optional[ResponseT]]:
        if self.exists(name):
            return self.hincrby(name, key, amount=amount), None
        return self.pipeline().hincrby(name, key, amount=amount).expire(name, time=time).execute()

    # INT Section
    def get_int(self, name: str, default: int = 0) -> int:
        return int(self.get(name) or default)

    def hget_int(self, name: str, key: str, default: int = 0) -> int:
        return int(self.hget(name, key) or default)

    def hmget_int(self, name: str, keys: List, default: int = 0, *args: List) -> List[int]:
        vals = self.hmget(name, keys, *args)
        return [int(v or default) for v in vals]

    def hvals_int(self, name: str, default: int = 0) -> List[int]:
        vals = self.hvals(name)
        return [int(v or default) for v in vals]

    def hgetall_int(self, name: str, default: int = 0) -> Dict[str, int]:
        kvs = self.hgetall(name)
        return {k: int(v or default) for (k, v) in kvs.items()}

    # FLOAT Section
    def get_float(self, name: str, default: int = 0) -> float:
        return float(self.get(name) or default)

    def hget_float(self, name: str, key: str, default: int = 0) -> float:
        return float(self.hget(name, key) or default)

    def hmget_float(self, name: str, keys: List, default: int = 0, *args: List) -> List[float]:
        vals = self.hmget(name, keys, *args)
        return [float(v or default) for v in vals]

    def hvals_float(self, name: str, default: int = 0) -> List[float]:
        vals = self.hvals(name)
        return [float(v or default) for v in vals]

    def hgetall_float(self, name: str, default: int = 0) -> Dict[str, float]:
        kvs = self.hgetall(name)
        return {k: float(v or default) for (k, v) in kvs.items()}

    # STR Section
    def get_str(self, name: str, default: str = '') -> ResponseT:
        return self.get(name) or default

    def hget_str(self, name: str, key: str, default: str = '') -> ResponseStrT:
        return self.hget(name, key) or default

    def hmget_str(self, name: str, keys: List, default: str = '', *args: List) -> List[str]:
        vals = self.hmget(name, keys, *args)
        return [(v or default) for v in vals]

    def hvals_str(self, name: str, default: str = '') -> List[str]:
        vals = self.hvals(name)
        return [(v or default) for v in vals]

    def hgetall_str(self, name: str, default: str = '') -> Dict[str, str]:
        kvs = self.hgetall(name)
        return {k: (v or default) for (k, v) in kvs.items()}

    # JSON Section
    def __json_params(self, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> Dict[str, Any]:
        return {**{'cls': cls}, **(json_params or {})}

    def set_json(self, name: str, value: EncodableT, ex: Optional[ExpiryT] = None, px: Optional[ExpiryT] = None, nx: bool = False, xx: bool = False, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseT:
        """
        Set the value at key ``name`` to ``json dumps value``.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it already exists.
        """
        return self.set(name, json.dumps(value, **self.__json_params(cls, json_params)), ex=ex, px=px, nx=nx, xx=xx)

    def setex_json(self, name: str, time: ExpiryT, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseT:
        """
        Set the value of key ``name`` to ``json dumps value`` that expires in ``time`` seconds.

        ``time`` can be represented by an integer or a Python timedelta object.
        """
        return self.setex(name, time, json.dumps(value, **self.__json_params(cls, json_params)))

    def setnx_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseT:
        """
        Set the value of key ``name`` to ``json dumps value`` if key doesn't exist.
        """
        return self.setnx(name, json.dumps(value, **self.__json_params(cls, json_params)))

    def get_json(self, name: str, default: str = '{}') -> ResponseJSON:
        return json.loads(self.get(name) or default)

    def get_list(self, name: str) -> List:
        return self.get_json(name, default='[]')

    def hset_json(self, name: str, key: Optional[str] = None, value: Optional[EncodableT] = None, mapping: Optional[dict] = None, items: Optional[list] = None, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseIntT:
        if key is None and not mapping and not items:
            raise DataError("'hsetjson' with no key value pairs")
        if key is not None:
            value = json.dumps(value, **self.__json_params(cls, json_params))
        if mapping:
            mapping = {k: json.dumps(v, **self.__json_params(cls, json_params)) for (k, v) in mapping.items()}
        if items:
            items = [(json.dumps(item, **self.__json_params(cls, json_params)) if idx % 2 else item) for idx, item in enumerate(items)]
        return self.hset(name, key=key, value=value, mapping=mapping, items=items)

    def hsetnx_json(self, name: str, key: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseBoolT:
        return self.hsetnx(name, key, json.dumps(value, **self.__json_params(cls, json_params)))

    def hmset_json(self, name: str, mapping: Dict[str, Any], cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> ResponseStrT:
        # HMSET
        # As of Redis version 4.0.0, this command is regarded as deprecated.
        # It can be replaced by HSET with multiple field-value pairs when migrating or writing new code.
        warnings.warn(
            f"{self.__class__.__name__}.hmsetjson() is deprecated since Redis version 4.0.0. "
            f"Use {self.__class__.__name__}.hsetjson() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        mapping = {k: json.dumps(v, **self.__json_params(cls, json_params)) for (k, v) in mapping.items()}
        return self.hmset(name, mapping)

    def hget_json(self, name: str, key: str, default: str = '{}') -> ResponseJSON:
        return json.loads(self.hget(name, key) or default)

    def hget_list(self, name: str, key: str) -> List:
        return self.hget_json(name, key, default='[]')

    def hmget_json(self, name: str, keys: List, default: str = '{}', *args: List) -> List[ResponseJSON]:
        vals = self.hmget(name, keys, *args)
        return [json.loads(v or default) for v in vals]

    def hmget_list(self, name: str, keys: List, *args: List) -> List[List]:
        return self.hmget_json(name, keys, default='[]', *args)

    def hvals_json(self, name: str, default: str = '{}') -> List[ResponseJSON]:
        vals = self.hvals(name)
        return [json.loads(v or default) for v in vals]

    def hvals_list(self, name: str) -> List[List]:
        return self.hvals_json(name, default='[]')

    def hgetall_json(self, name: str, default: str = '{}') -> Dict[str, Any]:
        kvs = self.hgetall(name)
        return {k: json.loads(v or default) for (k, v) in kvs.items()}

    def hgetall_list(self, name: str) -> Dict[str, Any]:
        return self.hgetall_json(name, default='[]')

    def lpush_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None):
        return self.lpush(name, json.dumps(value, **self.__json_params(cls, json_params)))

    def rpush_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None):
        return self.rpush(name, json.dumps(value, **self.__json_params(cls, json_params)))

    def lpushx_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None):
        return self.lpushx(name, json.dumps(value, **self.__json_params(cls, json_params)))

    def rpushx_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None):
        return self.rpushx(name, json.dumps(value, **self.__json_params(cls, json_params)))

    def lpushnx_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, force: bool = True, json_params: Dict[str, Any] = None):
        return self.lpushnx(name, json.dumps(value, **self.__json_params(cls, json_params)), force=force)

    def rpushnx_json(self, name: str, value: EncodableT, cls: Optional[Type[json.JSONDecoder]] = None, force: bool = True, json_params: Dict[str, Any] = None):
        return self.rpushnx(name, json.dumps(value, **self.__json_params(cls, json_params)), force=force)

    def lpop_json(self, name: str, default: str = '{}') -> Dict[str, Any]:
        return json.loads(self.lpop(name) or default)

    def rpop_json(self, name: str, default: str = '{}') -> Dict[str, Any]:
        return json.loads(self.rpop(name) or default)

    def blpop_json(self, keys: List, timeout: int = 0, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        kv = self.blpop(keys, timeout=timeout)
        return (kv[0], json.loads(kv[1], **self.__json_params(cls, json_params))) if kv else (None, None)

    def brpop_json(self, keys: List, timeout: int = 0, cls: Optional[Type[json.JSONDecoder]] = None, json_params: Dict[str, Any] = None) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        kv = self.brpop(keys, timeout=timeout)
        return (kv[0], json.loads(kv[1], **self.__json_params(cls, json_params))) if kv else (None, None)

    # Locks Section
    def __lock_key(self, name: str) -> str:
        return '{0}lock:{1}'.format(KEY_PREFIX, name)

    def acquire_lock(self, name: str, time: Optional[ExpiryT] = None, acquire_timeout: int = 10, short: bool = False) -> Union[str, bool]:
        """
        Acquire lock for ``name``.

        ``time`` sets an expire flag on key ``name`` for ``time`` seconds.

        ``acquire_timeout`` indicates retry time of acquiring lock.
        """
        identifier = self.__uuid(short)
        end = mod_time.time() + acquire_timeout
        while mod_time.time() < end:
            if self.set(self.__lock_key(name), identifier, ex=time, nx=True):
                return identifier
            mod_time.sleep(.001)
        return False

    def release_lock(self, name: str, identifier: str) -> bool:
        """
        Release lock for ``name``.
        """
        lock_key = self.__lock_key(name)
        pipe = self.pipeline()
        while True:
            try:
                pipe.watch(lock_key)
                if pipe.get(lock_key) == identifier:
                    pipe.multi()
                    pipe.delete(lock_key)
                    pipe.execute()
                    return True
                pipe.unwatch()
                break
            except WatchError:
                pass
        return False

    def delete_lock(self, name: str) -> ResponseT:
        """
        Delete lock for ``name``.
        """
        return self.delete(self.__lock_key(name))

    def exists_lock(self, name: str, regex: bool = False) -> ResponseT:
        """
        Check lock for ``name`` exists or not.
        """
        if regex:
            logger.warning(WARNING_LOG.format('r.keys()'))
        lock_key = self.__lock_key(name)
        return self.keys(lock_key) if regex else self.exists(lock_key)

    # Quota Section
    def __quota_key(self, name: str) -> str:
        return '{0}quota:{1}'.format(KEY_PREFIX, name)

    def __quota(self, quota_key: str, amount: int = 10, time: Optional[ExpiryT] = None) -> bool:
        num = self.incr(quota_key)
        if num == 1 and time:
            self.expire(quota_key, time)
        return num > amount

    def quota(self, name: str, amount: int = 10, time: Optional[ExpiryT] = None) -> bool:
        """
        Check whether overtop amount or not.
        """
        return self.__quota(self.__quota_key(name), amount=amount, time=time)

    # Quote/UnQuote Section
    def __quote_key(self, name: str) -> str:
        return '{0}quote:{1}'.format(KEY_PREFIX, name)

    def quote(self, s: str, ex: bool = True, time: int = 1800, short_uuid: bool = False) -> str:
        identifier = self.__uuid(short_uuid)
        identifier_key = self.__quote_key(identifier)
        self.setex(identifier_key, time, s) if ex else self.set(identifier_key, s)
        return identifier

    def unquote(self, identifier: str, buf: bool = False) -> ResponseT:
        identifier_key = self.__quote_key(identifier)
        return self.get(identifier_key) if buf else self.get_delete(identifier_key)[0]

    # SignIns Section
    def __get_signin_info(self, signname: str) -> Tuple[str, Dict[str, Any], str, str, int]:
        """
        signin_info:
            signin_date
            signin_days
            signin_total_days
            signin_longest_days
        """
        name = '{0}signin:info:{1}'.format(KEY_PREFIX, signname)
        # Signin Info
        signin_info = self.get_json(name)
        # Last Signin Date, Format ``%Y-%m-%d``
        last_signin_date = signin_info.get('signin_date', '1988-06-15')
        # Today Local Date, Format ``%Y-%m-%d``
        signin_date = self.__local_ymd()
        # Delta Days between ``Last Signin Date`` and ``Today Local Date``
        delta_days = tc.string_delta(signin_date, last_signin_date, format='%Y-%m-%d')['days']
        return name, signin_info, signin_date, last_signin_date, delta_days

    def signin(self, signname: str) -> Dict[str, Any]:
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

    def signin_status(self, signname: str) -> Dict[str, Any]:
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
    def __token_key(self, name: str) -> str:
        return '{0}token:{1}'.format(KEY_PREFIX, name)

    def __token_buffer_key(self, name: str) -> str:
        return '{0}token:buffer:{1}'.format(KEY_PREFIX, name)

    def token(self, name: str, ex: bool = True, time: int = 1800, buf: bool = True, buf_time: int = 300, short_uuid: bool = True, token_generate_func: Union[type, Callable] = None) -> str:
        """
        Generate token.

        ``ex`` indicates whether token expire or not.

        ``time`` indicates expire time of generating code, which can be represented by an integer or a Python timedelta object, Default: 30 minutes.

        ``buf`` indicates whether replaced token buffer or not.

        ``buf_time`` indicates buffer time of replaced token, which can be represented by an integer or a Python timedelta object, Default: 5 minutes.

        ``token_generate_func`` a callable used to generate the token.
        """
        code = token_generate_func() if token_generate_func else self.__uuid(short_uuid)
        token_key = self.__token_key(name)
        buf_code = self.getsetex(token_key, time, code) if ex else self.getset(token_key, code)
        if buf_code and buf:
            self.setex(self.__token_buffer_key(name), buf_time, buf_code)
        return code

    def token_exists(self, name: str, code: str) -> bool:
        """
        Check token code exists or not.
        """
        return self.__str(code) in self.pipeline().get(self.__token_key(name)).get(self.__token_buffer_key(name)).execute()

    def token_delete(self, name: str) -> ResponseT:
        """
        Delete token.
        """
        return self.pipeline().delete(self.__token_key(name)).delete(self.__token_buffer_key(name)).execute()[0]

    # Counter
    def _counter_key(self, name: str, time_part_func: Optional[Union[type, Callable]] = None) -> str:
        time_part = time_part_func() if time_part_func else self.__local_ymd(format='%Y%m%d')
        return '{0}counter:{1}:{2}'.format(KEY_PREFIX, name, time_part)

    def counter(self, name: str, amount: int = 1, limit: Optional[int] = None, ex: bool = True, time: int = 86400, time_part_func: Optional[Union[type, Callable]] = None) -> Tuple[int, int, int]:
        """
        Counter, default ``daily``.
        """
        if amount < 0:
            raise ValueError('The amount argument should not be negative')
        name = self._counter_key(name, time_part_func=time_part_func)
        pre_amount = self.get_int(name)
        if amount == 0:
            return pre_amount, pre_amount, 0
        amount = self.incr_limit(name, amount=amount, limit=limit)
        if amount == 1 and ex:
            self.expire(name, time)
        return amount, pre_amount, amount - pre_amount

    # Verification Codes Section
    def __black_list(self, value: str, cate: str = 'phone') -> Union[Awaitable[bool], bool]:
        black_key = '{0}vcode:{1}:black:list'.format(KEY_PREFIX, cate)
        return self.sismember(black_key, value)

    def __vcode_key(self, phone: str) -> str:
        return '{0}vcode:{1}'.format(KEY_PREFIX, phone)

    def __quota_key(self, value: str, cate: str = 'phone') -> str:
        return '{0}vcode:{1}:quota:{2}'.format(KEY_PREFIX, cate, value)

    def __quota_incr(self, value: str, cate: str = 'phone', quota: int = 10) -> bool:
        return self.__quota(self.__quota_key(value, cate=cate), amount=quota, time=86400)

    def __quota_num(self, value: str, cate: str = 'phone') -> int:
        return int(self.get(self.__quota_key(value, cate=cate)) or 0)

    def __quota_delete(self, value: str, cate: str = 'phone') -> ResponseT:
        return self.delete(self.__quota_key(value, cate=cate))

    def __req_stamp_key(self, value: str, cate: str = 'phone') -> str:
        return '{0}vcode:{1}:req:stamp:{2}'.format(KEY_PREFIX, cate, value)

    def __req_stamp_delete(self, value: str, cate: str = 'phone') -> ResponseT:
        return self.delete(self.__req_stamp_key(value, cate=cate))

    def __black_list_key(self, cate: str = 'phone') -> str:
        return '{0}vcode:{1}:black:list'.format(KEY_PREFIX, cate)

    def __final_code(self, code: str, ignore_blank: bool = True) -> str:
        final_code = code or ''
        final_code = (final_code.replace(' ', '') if ignore_blank else final_code).lower()
        return final_code

    def __req_interval(self, value: str, cate: str = 'phone', req_interval: int = 60) -> bool:
        curstamp = tc.utc_timestamp(ms=False)
        laststamp = int(self.getset(self.__req_stamp_key(value, cate=cate), curstamp) or 0)
        if curstamp - laststamp < req_interval:
            self.sadd(self.__black_list_key(cate=cate), value)
            return True
        return False

    def vcode(self, phone: str, ipaddr: Optional[str] = None, quota: int = 10, req_interval: int = 60, black_list: bool = True, ndigits: int = 6, time: int = 1800, code_cast_func: Union[type, Callable] = str) -> Tuple[Union[str, bool, None], Optional[bool], Optional[bool]]:
        """
        Generate verification code if not reach quota. Return a 3-item tuple: (Verification code, Whether reach quota or not, Whether in black list or not).

        ``quota`` indicates limitation of generating code, ``phone`` and ``ipaddr`` use in common, 0 for limitlessness.

        ``req_interval`` indicates interval of two request, ``phone`` and ``ipaddr`` use in common, 0 for limitlessness.

        ``black_list`` indicates whether check black list or not.

        ``ndigits`` indicates length of generated code.

        ``time`` indicates expire time of generating code, which can be represented by an integer or a Python timedelta object, Default: 30 minutes.

        ``code_cast_func`` a callable used to cast the code return value.

        ``black_list`` - ``redis:extensions:vcode:phone:black:list`` & ``redis:extensions:vcode:ipaddr:black:list``
        """
        # Black List Check
        if black_list and (self.__black_list(phone, cate='phone') or (ipaddr and self.__black_list(ipaddr, cate='ipaddr'))):
            return None, None, True
        # Quota Check
        if quota:
            # Phone Quota
            if self.__quota_incr(phone, cate='phone', quota=quota):
                return None, True, None
            # Ipaddr Quota If `ipaddr`` Isn't ``None``
            if ipaddr and self.__quota_incr(ipaddr, cate='ipaddr', quota=quota):
                return None, True, None
        # Req Interval Check
        if req_interval:
            # Phone Interval
            if self.__req_interval(phone, cate='phone', req_interval=req_interval):
                return None, False, True
            # Ipaddr Interval If `ipaddr`` Isn't ``None``
            if ipaddr and self.__req_interval(ipaddr, cate='ipaddr', req_interval=req_interval):
                return None, False, True
        code = mod_vcode.digits(ndigits=ndigits, code_cast_func=code_cast_func)
        self.setex(self.__vcode_key(phone), time, code)
        # Delete vcode exists quota key
        self.__quota_delete(phone, cate='exists')
        return code, False, False

    def vcode_quota(self, phone: Optional[str] = None, ipaddr: Optional[str] = None) -> Union[int, Tuple[int]]:
        if phone and not ipaddr:
            return self.__quota_num(phone, cate='phone')
        if not phone and ipaddr:
            return self.__quota_num(ipaddr, cate='ipaddr')
        return self.__quota_num(phone, cate='phone'), self.__quota_num(ipaddr, cate='ipaddr')

    def vcode_exists(self, phone: str, code: str, ipaddr: Optional[str] = None, keep: bool = False, quota: int = 3, ignore_blank: bool = True) -> bool:
        """
        Check verification code exists or not.
        """
        exists = self.get(self.__vcode_key(phone)) == self.__final_code(self.__str(code), ignore_blank=ignore_blank)
        # Delete req stamp when vcode exists
        if exists:
            self.__req_stamp_delete(phone, cate='phone')
            ipaddr and self.__req_stamp_delete(ipaddr, cate='ipaddr')
        # Deleted when exists or not quota(default 3) times in a row
        if not keep and (exists or self.__quota_incr(phone, cate='exists', quota=quota - 1)):
            self.vcode_delete(phone)
        return exists

    def vcode_delete(self, phone: str) -> ResponseT:
        """
        Delete verification code.
        """
        return self.delete(self.__vcode_key(phone))

    # Graphic Verification Codes Section
    def __gvcode_str(self) -> str:
        b64str, vcode = gvcode.base64()
        return json.dumps({
            'b64str': b64str,
            'vcode': vcode,
        })

    def _gvcode_key(self) -> str:
        return '{0}graphic:vcode'.format(KEY_PREFIX)

    def __gvcode_key(self, name: str) -> str:
        return '{0}graphic:vcode:{1}'.format(KEY_PREFIX, name)

    def gvcode_add(self, num: int = 10) -> ResponseIntT:
        if num <= 0:
            raise ValueError('The num argument should be positive')
        gvcodes = (self.__gvcode_str() for _ in range(num))
        return self.sadd(self._gvcode_key(), *gvcodes)

    def gvcode_initial(self, num: int = 10) -> ResponseIntT:
        return self.gvcode_add(num=num)

    def __gvcode_cut_num(self, num: int = 10) -> int:
        # Prevent completely spopped
        pre_num = self.scard(self._gvcode_key())
        return max(pre_num - 1, 0) if num >= pre_num else num

    def gvcode_cut(self, num: int = 10) -> int:
        if num <= 0:
            raise ValueError('The num argument should be positive')
        return self.multi_spop(self._gvcode_key(), num=self.__gvcode_cut_num(num=num))[-1]

    def gvcode_refresh(self, num: int = 10) -> int:
        if num <= 0:
            raise ValueError('The num argument should be positive')
        cut_num = self.__gvcode_cut_num(num=num)
        return cut_num and self.gvcode_cut(num=cut_num), self.gvcode_add(num=num)

    def __gvcode_b64str(self) -> Dict[str, Any]:
        return json.loads(self.srandmember(self._gvcode_key()) or '{}')

    def gvcode_b64str(self, name: str, time: int = 1800, data_uri_scheme: bool = False) -> str:
        gvcode = self.__gvcode_b64str()
        if not gvcode:
            self.gvcode_refresh()
            gvcode = self.__gvcode_b64str()
            if not gvcode:
                logger.warning('Gvcode not found, exec gvcode_add or gvcode_refresh first')
        b64str, vcode = gvcode.get('b64str', ''), gvcode.get('vcode', '')
        self.setex(self.__gvcode_key(name), time, vcode)
        return '{0}{1}'.format('data:image/png;base64,' if data_uri_scheme else '', cc.Convert2Utf8(b64str))

    def gvcode_exists(self, name: str, code: str, ignore_blank: bool = True) -> bool:
        return (self.get(self.__gvcode_key(name)) or '').lower() == self.__final_code(code, ignore_blank=ignore_blank)

    # Delay Tasks Section
    def __queue_key(self, queue: str) -> str:
        return '{0}queue:{1}'.format(KEY_PREFIX, queue)

    def execute_later(self, queue: str, name: str, args: Dict[str, Any] = None, delayed: str = KEY_PREFIX + 'delayed:default', delay: int = 0, short_uuid: bool = False, enable_queue: bool = False) -> str:
        """
        Producer of delay execute.
        """
        identifier = self.__uuid(short_uuid)
        item = json.dumps([identifier, queue, name, args])
        if delay > 0:
            self.zadd(delayed, {item: mod_time.time() + delay})
        else:
            if enable_queue:
                self.rpush(self.__queue_key(queue), item)
        return identifier

    def __callable_func(self, f: Union[str, Callable]) -> Optional[Callable]:
        if callable(f):
            return f
        try:
            module, func = f.rsplit('.', 1)
            m = importlib.import_module(module)
            return getattr(m, func)
        except Exception as e:
            logger.error(e)
            return None

    def release_poll_queue_lock(self, delayed: str, final_logger: Optional[logging.Logger] = None) -> ResponseT:
        if not delayed:
            return
        item = self.zrange(delayed, 0, 0, withscores=True)
        if not item:
            return
        item = item[0][0]
        identifier, queue, name, args = json.loads(item)
        final_logger.info('  * Release lock: {0}'.format(identifier))
        return self.delete_lock(identifier)

    def __release_lock_when_launch(self, release_lock_when_launch: bool, release_lock_eth0_inet_addr: Optional[str], final_process_lock_key: str, delayed: str, final_logger: Optional[logging.Logger]):
        if not release_lock_when_launch:
            return
        if not release_lock_eth0_inet_addr:
            final_logger.info('>>> Release lock should pass `release_lock_eth0_inet_addr`')
            return
        eth0_inet_addr = get_network_ip()
        if eth0_inet_addr not in release_lock_eth0_inet_addr:
            final_logger.info('>>> Release lock `release_lock_eth0_inet_addr` not match')
            return
        final_logger.info('>>> Release process lock start')
        self.delete_lock(final_process_lock_key)
        final_logger.info('>>> Release process lock end')
        final_logger.info('>>> Release item lock start')
        self.release_poll_queue_lock(delayed, final_logger=final_logger)
        final_logger.info('>>> Release item lock end')

    def poll_queue(self, callbacks: Dict[str, Callable] = {}, delayed: str = KEY_PREFIX + 'delayed:default', enable_auto_zrem: bool = False, enable_queue: bool = False, enable_process_lock: bool = False, process_lock_key: Optional[str] = None, release_lock_when_launch: bool = True, release_lock_eth0_inet_addr: Optional[str] = None, release_lock_when_error: bool = True, delayed_logger: Optional[logging.Logger] = None, unlocked_warning_func: Optional[Callable] = None):
        """
        Consumer of delay execute.

        ``unlocked_warning_func`` indicates callback function when ``acquire_lock`` fail.

        ``enable_auto_zrem`` indicates whether enable auto zrem or not. ``True`` for at most once, ``False`` for at least once.

        ``enable_queue`` indicates whether enable queue or not.

        ``enable_process_lock`` indicates whether enable process lock or not.

        ``process_lock_key`` indicates acquire process lock key.

        ``release_lock_when_launch`` indicates whether release lock when launch or not. This is for ``restart``.

        ``release_lock_eth0_inet_addr`` indicates eth0 inet addr, which can release lock when ``release_lock_when_launch``.

        ``release_lock_when_error`` indicates whether release lock when error or not.
        """
        callbacks = {k: self.__callable_func(v) for k, v in callbacks.items()}
        callbacks = {k: v for k, v in callbacks.items() if v}

        final_process_lock_key = 'process:lock:{0}'.format(process_lock_key or delayed)
        final_logger = delayed_logger or logger

        final_logger.info('>>> Available callbacks ({0}):'.format(len(callbacks)))
        for k, v in callbacks.items():
            final_logger.info('  * {0}: {1}'.format(k, v))

        self.__release_lock_when_launch(release_lock_when_launch, release_lock_eth0_inet_addr, final_process_lock_key, delayed, final_logger)

        process_lock = None
        while POLL_QUEUE_CONTINUE_FLAG:
            if enable_process_lock:
                # Release process lock
                if process_lock:
                    self.release_lock(final_process_lock_key, process_lock)

                # Acquire process lock
                process_lock = self.acquire_lock(final_process_lock_key)
                if not process_lock:
                    continue

            item = self.zrange(delayed, 0, 0, withscores=True)

            if not item or item[0][1] > mod_time.time():
                mod_time.sleep(.01)
                continue

            final_logger.info(item)

            item = item[0][0]
            identifier, queue, name, args = json.loads(item)

            item_lock = self.acquire_lock(identifier)
            if not item_lock:
                # Call ``unlocked_warning_func`` if exists when ``acquire_lock`` fail
                if unlocked_warning_func:
                    unlocked_warning_func(queue, name, args)
                continue

            # At most once 最多消费一次
            if enable_auto_zrem and self.zrem(delayed, item):
                if enable_queue:
                    self.rpush(self.__queue_key(queue), item)
                self.release_lock(identifier, item_lock)

            # Callbacks
            if queue in callbacks:
                try:
                    callbacks[queue](name, args)
                except Exception as e:
                    final_logger.error(e)
                    if not release_lock_when_error:
                        continue

            # At least once 最少消费一次
            if not enable_auto_zrem and self.zrem(delayed, item):
                if enable_queue:
                    self.rpush(self.__queue_key(queue), item)

            self.release_lock(identifier, item_lock)

    # HotKey Section
    def hotkey(self, gfunc: Optional[Callable] = None, gargs: Optional[Dict[str, Any]] = None, gkwargs: Optional[Dict[str, Any]] = None, sfunc: Optional[Callable] = None, sargs: Optional[Dict[str, Any]] = None, skwargs: Optional[Dict[str, Any]] = None, update_timeout: int = 1000, short_uuid: bool = False):
        data = gfunc and gfunc(*(gargs or ()), **(gkwargs or {}))
        if not data:
            name = self.__uuid(short_uuid)
            locked = self.acquire_lock(name)
            if locked:
                data = sfunc(*(sargs or ()), **(skwargs or {}))
                self.release_lock(name, locked)
            else:
                end = mod_time.time() + update_timeout
                while mod_time.time() < end:
                    data = gfunc and gfunc(*(gargs or ()), **(gkwargs or {}))
                    if data:
                        return data
                    mod_time.sleep(.001)
        return data

    # For rename official function
    def georem(self, name: KeyT, *values: FieldT) -> ResponseT:
        return self.zrem(name, *values)

    def geomembers(self, name: KeyT, start: int = 0, end: int = -1, desc: bool = False, withscores: bool = False, score_cast_func: Union[type, Callable] = float) -> ResponseT:
        """
        zrange(name, 0, -1) == georadius(name, 0, 0, '+inf', unit='m')
        """
        return self.zrange(name, start=start, end=end, desc=desc, withscores=withscores, score_cast_func=score_cast_func)

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
    lpushnx = pushnx = push_nx = lpush_nx
    rpushnx = rpush_nx
    multilpop = multipop = multi_pop = multi_lpop
    multirpop = multi_rpop
    multilpopdelete = multipopdelete = multi_pop_delete = multi_lpop_delete
    multirpopdelete = multi_rpop_delete
    trimlpush = trimpush = trim_push = trim_lpush
    trimrpush = trim_rpush
    deletelpush = deletepush = delete_push = delete_lpush
    deleterpush = delete_rpush
    lpushex = lpush_ex
    lrangeex = lrange_ex
    sortedpop = sorted_pop
    bpopv = blpopv
    deletesadd = delete_sadd
    multispop = multi_spop
    srandshuffle = srandmembershuffle = srandmember_shuffle
    # INT
    getint = get_int
    hgetint = hget_int
    hmgetint = hmget_int
    hvalsint = hvals_int
    hgetallint = hgetall_int
    # FLOAT
    getfloat = get_float
    hgetfloat = hget_float
    hmgetfloat = hmget_float
    hvalsfloat = hvals_float
    hgetallfloat = hgetall_float
    # STR
    getstr = get_str
    hgetstr = hget_str
    hmgetstr = hmget_str
    hvalsstr = hvals_str
    hgetallstr = hgetall_str
    # JSON
    setlist = set_list = setdict = set_dict = setjson = set_json
    setexlist = setex_list = setexdict = setex_dict = setexjson = setex_json
    setnxlist = setnx_list = setnxdict = setnx_dict = setnxjson = setnx_json
    getdict = get_dict = getjson = get_json
    getlist = get_list
    hsetlist = hset_list = hsetdict = hset_dict = hsetjson = hset_json
    hsetnxlist = hsetnx_list = hsetnxdict = hsetnx_dict = hsetnxjson = hsetnx_json
    hmsetlist = hmset_list = hmsetdict = hmset_dict = hmsetjson = hmset_json
    hgetdict = hget_dict = hgetjson = hget_json
    hgetlist = hget_list
    hmgetdict = hmget_dict = hmgetjson = hmget_json
    hmgetlist = hmget_list
    hvalsdict = hvals_dict = hvalsjson = hvals_json
    hvalslist = hvals_list
    hgetalldict = hgetall_dict = hgetalljson = hgetall_json
    hgetalllist = hgetall_list

    lpushjson = pushjson = push_json = lpush_json
    rpushjson = rpush_json

    lpushxjson = pushxjson = pushx_json = lpushx_json
    rpushxjson = rpushx_json

    lpushnxjson = pushnxjson = pushnx_json = lpushnx_json
    rpushnxjson = rpushnx_json

    popjson = lpopjson = lpop_json
    rpopjson = rpop_json

    bpopjson = blpopjson = blpop_json
    brpopjson = brpop_json

    # For backwards compatibility
    zgte = zge
    zlte = zle
    vcode_status = vcode_exists
    lock_exists = exists_lock

    # Delete => Del
    delkeys = del_keys = delete_keys
    getdel = get_del = get_delete
    multilpopdel = multi_lpop_del = multipopdel = multi_pop_del = multi_lpop_delete
    multirpopdel = multi_rpop_del = multi_rpop_delete
    dellpush = del_lpush = delpush = del_push = delete_lpush
    delrpush = del_rpush = delete_rpush
    delsadd = del_sadd = delete_sadd
