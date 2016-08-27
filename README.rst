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

