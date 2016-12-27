================
redis-extensions
================

Redis-extensions is a collection of custom extensions for Redis-py.

Installation
============

::

    pip install redis-extensions


Usage
=====

::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: r.zaddwithstamps('sorted_set', 1, 'a', 2, 'b')
    Out[3]: 1

    In [4]: r.zrange('sorted_set', 0, 100, withscores=True)
    Out[4]: [('a', 11472205032192.0), ('b', 21472205032192.0)]

    In [5]: r.zrange('sorted_set', 0, 100, withscores=True, score_cast_func=r.rawscore)
    Out[5]: [('a', 1.0), ('b', 2.0)]

    In [6]: r.zincrbywithstamps('sorted_set', 'a')
    Out[6]: 0

    In [7]: r.zrange('sorted_set', 0, 100, withscores=True)
    Out[7]: [('b', 21472205032192.0), ('a', 21472205071514.0)]

    In [8]: r.zrange('sorted_set', 0, 100, withscores=True, score_cast_func=r.rawscore)
    Out[8]: [('b', 2.0), ('a', 2.0)]


Expired
=======

::

    In [1]: from redis_extensions import RedisExpires as exp

    In [2]: exp.REDIS_EXPIRED
    exp.REDIS_EXPIRED_HALF_HOUR  exp.REDIS_EXPIRED_ONE_HOUR   exp.REDIS_EXPIRED_ONE_WEEK
    exp.REDIS_EXPIRED_ONE_DAY    exp.REDIS_EXPIRED_ONE_MONTH  exp.REDIS_EXPIRED_ONE_YEAR

    In [2]: exp.REDIS_EXPIRED_ONE_HOUR
    Out[2]: 3600

    In [3]:

    In [3]: import redis_extensions as redis

    In [4]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [5]: r.REDIS_EXPIRED
    r.REDIS_EXPIRED_HALF_HOUR  r.REDIS_EXPIRED_ONE_HOUR   r.REDIS_EXPIRED_ONE_WEEK
    r.REDIS_EXPIRED_ONE_DAY    r.REDIS_EXPIRED_ONE_MONTH  r.REDIS_EXPIRED_ONE_YEAR

    In [5]: r.REDIS_EXPIRED_ONE_HOUR
    Out[5]: 3600


Solutions
=========

Lock::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: r.acquire_lock('redis_extensions')
    Out[3]: '026ad2a7-2b58-435f-8ba2-467458a687f1'

    In [4]: r.acquire_lock('redis_extensions')
    Out[4]: False

    In [5]: r.release_lock('redis_extensions', '026ad2a7-2b58-435f-8ba2-467458a687f1')
    Out[5]: True

    In [6]: r.acquire_lock('redis_extensions', ex=10)
    Out[6]: '84f6b991-7c30-4210-947a-deb56bbc769a'

    In [7]: r.exists('redis:extensions:lock:redis_extensions')
    Out[7]: True

    In [8]: # 10 Seconds Later

    In [9]: r.exists('redis:extensions:lock:redis_extensions')
    Out[9]: False


Signin::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: r.signin_status('redis_extensions')
    Out[3]:
    {'delta_days': 10394,  # Signin Interval, Check Duplicate Signin
     'signed_today': False,  # Signed Today Or Not
     'signin_date': '1988-06-15',  # Last Signin Date
     'signin_days': 0,  # Continuous Signin Days
     'signin_longest_days': 0,  # Longest Continuous Signin Days In History
     'signin_total_days': 0}  # Total Signin Days

    In [4]: r.signin('redis_extensions')
    Out[4]:
    {'delta_days': 10394,
     'signed_today': True,
     'signin_date': '2016-11-29',
     'signin_days': 1,
     'signin_longest_days': 1,
     'signin_total_days': 1}

    In [5]: r.signin_status('redis_extensions')
    Out[5]:
    {'delta_days': 0,
     'signed_today': True,
     u'signin_date': u'2016-11-29',
     u'signin_days': 1,
     u'signin_longest_days': 1,
     u'signin_total_days': 1}

    In [6]: r.signin('redis_extensions')
    Out[6]:
    {'delta_days': 0,  # Duplicate Signin
     'signed_today': True,
     u'signin_date': u'2016-11-29',
     u'signin_days': 1,
     u'signin_longest_days': 1,
     u'signin_total_days': 1}


Token::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: phone = '18888888888'

    In [4]: r.token(phone)
    Out[4]: '8bde88aa-71e9-4dea-846c-b1684a02b0f5'

    In [5]: r.token_exists(phone, '8bde88aa-71e9-4dea-846c-b1684a02b0f5')
    Out[5]: True


Verification Code::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: phone = '18888888888'

    In [4]: r.vcode(phone)
    Out[4]: ('678366', False, False)

    In [5]: r.vcode_exists(phone, '678366')
    Out[5]: True

