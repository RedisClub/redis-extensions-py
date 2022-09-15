from redis_extensions import RedisExpires as exp


class TestRedisExpiresCommands(object):

    def test_expires1(self):
        assert exp.REDIS_EXPIRED_HALF_HOUR == 1800

    def test_expires2(self, r):
        assert r.REDIS_EXPIRED_HALF_HOUR == 1800
