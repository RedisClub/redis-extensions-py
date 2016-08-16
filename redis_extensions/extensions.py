# -*- coding: utf-8 -*-


class RedisExtensions(object):

    def get_delete(self, conn, key):
        pipe = conn.pipeline()
        pipe.get(key)
        pipe.delete(key)
        return pipe.execute()

    def get_rename(self, conn, key, suffix='del'):
        pipe = conn.pipeline()
        pipe.get(key)
        pipe.rename(key, '{}_{}'.format(key, suffix)) if conn.exists(key) else pipe.exists(key)
        return pipe.execute()

    def multi_pop(self, conn, key, num):
        if num <= 0:
            return [[], False, 0]
        pipe = conn.pipeline()
        pipe.lrange(key, 0, num - 1)
        pipe.ltrim(key, num, -1)
        pipe.llen(key)
        return pipe.execute()

    def trim_lpush(self, conn, key, num, *values):
        pipe = conn.pipeline()
        pipe.lpush(key, *values)
        pipe.ltrim(key, 0, num - 1)
        pipe.llen(key)
        return pipe.execute()

    def trim_rpush(self, conn, key, num, *values):
        pipe = conn.pipeline()
        pipe.rpush(key, *values)
        pipe.ltrim(key, -num, - 1)
        pipe.llen(key)
        return pipe.execute()

_global_instance = RedisExtensions()
get_delete = _global_instance.get_delete
get_rename = _global_instance.get_rename
multi_pop = _global_instance.multi_pop
trim_lpush = _global_instance.trim_lpush
trim_rpush = _global_instance.trim_rpush
