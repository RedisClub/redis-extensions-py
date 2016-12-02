# -*- coding: utf-8 -*-


class BaseRedisExpires(object):

    def __init__(self, *args, **kwargs):
        self.REDIS_EXPIRED_HALF_HOUR = 1800  # 30 * 60
        self.REDIS_EXPIRED_ONE_HOUR = 3600  # 60 * 60
        self.REDIS_EXPIRED_ONE_DAY = 86400  # 24 * 60 * 60
        self.REDIS_EXPIRED_ONE_WEEK = 604800  # 7 * 24 * 60 * 60
        self.REDIS_EXPIRED_ONE_MONTH = 2678400  # 31 * 24 * 60 * 60
        self.REDIS_EXPIRED_ONE_YEAR = 31622400  # 366 * 24 * 60 * 60
        super(BaseRedisExpires, self).__init__(*args, **kwargs)


RedisExpires = BaseRedisExpires()
