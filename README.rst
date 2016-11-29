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


Solutions
=========

Locks::

    In [1]: import redis_extensions as redis

    In [2]: r = redis.StrictRedisExtensions(host='localhost', port=6379, db=0)

    In [3]: r.acquire_lock('redis_extensions')
    Out[3]: '026ad2a7-2b58-435f-8ba2-467458a687f1'

    In [4]: r.acquire_lock('redis_extensions')
    Out[4]: False

    In [5]: r.release_lock('redis_extensions', '026ad2a7-2b58-435f-8ba2-467458a687f1')
    Out[5]: True


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

