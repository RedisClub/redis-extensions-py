# -*- coding: utf-8 -*-

import redis
from redis import *

from redis_extensions.expires import (
    BaseRedisExpires,
    RedisExpires
)

from redis_extensions.extensions import (
    StrictRedisExtensions
)

__all__ = redis.__all__ + ['StrictRedisExtensions', 'BaseRedisExpires', 'RedisExpires']
