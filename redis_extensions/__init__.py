import redis
from redis import *

from redis_extensions.expires import BaseRedisExpires, RedisExpires
from redis_extensions.extensions import RedisExtensions, StrictRedisExtensions


__all__ = redis.__all__ + ['RedisExtensions', 'StrictRedisExtensions', 'BaseRedisExpires', 'RedisExpires']
