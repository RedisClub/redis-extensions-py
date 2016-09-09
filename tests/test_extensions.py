# -*- coding: utf-8 -*-

import pytest


class TestRedisExtensionsCommands(object):

    def test_delete_keys(self, r):
        r['a:x'] = 'foo'
        r['a:y'] = 'bar'
        result = r.delete_keys('a:*')
        assert result == 2
        assert not r.exists('a:x')
        assert not r.exists('a:y')

    def test_get_multi(self, r):
        r['a'] = 'foo'
        r['b'] = 'bar'
        result = r.get_multi('a', 'b')
        assert isinstance(result, list)
        assert result[0] == 'foo'
        assert result[1] == 'bar'

    def test_get_delete(self, r):
        result = r.get_delete('a')
        assert isinstance(result, list)
        r['a'] = 'foo'
        result = r.get_delete('a')
        assert result[0] == 'foo'

    def test_get_rename(self, r):
        result = r.get_rename('a')
        assert isinstance(result, list)
        r['a'] = 'foo'
        result = r.get_rename('a')
        assert result[0] == 'foo'
        assert r.exists('a_del')

    def test_get_or_set(self, r):
        result = r.get_or_set('a', 'foo')
        assert isinstance(result, list)
        assert result[0] == 'foo'

    def test_get_or_setex(self, r):
        result = r.get_or_setex('a', 60, 'foo')
        assert isinstance(result, list)
        assert result[0] == 'foo'
        assert 0 < r.ttl('a') <= 60

    def test_lpush_nx(self, r):
        r.lpush('a', 'foo')
        r.lpush('a', 'bar')
        r.lpush_nx('a', 'foo', force=False)
        result = r.lrange('a', 0, -1)
        assert result == ['bar', 'foo']
        r.lpush_nx('a', 'foo', force=True)
        result = r.lrange('a', 0, -1)
        assert result == ['foo', 'bar']

    def test_rpush_nx(self, r):
        r.rpush('a', 'foo')
        r.rpush('a', 'bar')
        r.rpush_nx('a', 'foo', force=False)
        result = r.lrange('a', 0, -1)
        assert result == ['foo', 'bar']
        r.rpush_nx('a', 'foo', force=True)
        result = r.lrange('a', 0, -1)
        assert result == ['bar', 'foo']

    def test_multi_lpop(self, r):
        r.rpush('a', *range(10))
        result = r.multi_lpop('a', 3)
        assert isinstance(result, list)
        assert len(result[0]) == 3
        assert result[-1] == 7
        with pytest.raises(ValueError):
            r.multi_lpop('a', -1)

    def test_multi_rpop(self, r):
        r.rpush('a', *range(10))
        result = r.multi_rpop('a', 3)
        assert isinstance(result, list)
        assert len(result[0]) == 3
        assert result[-1] == 7
        with pytest.raises(ValueError):
            r.multi_rpop('a', -1)

    def test_multi_lpop_delete(self, r):
        r.rpush('a', *range(10))
        result = r.multi_lpop_delete('a', 3)
        assert isinstance(result, list)
        assert len(result[0]) == 3
        assert result[-1] == 1
        assert not r.exists('a')
        with pytest.raises(ValueError):
            r.multi_lpop('a', -1)

    def test_multi_rpop_delete(self, r):
        r.rpush('a', *range(10))
        result = r.multi_rpop_delete('a', 3)
        assert isinstance(result, list)
        assert len(result[0]) == 3
        assert result[-1] == 1
        assert not r.exists('a')
        with pytest.raises(ValueError):
            r.multi_rpop('a', -1)

    def test_trim_lpush(self, r):
        r.trim_lpush('a', 3, *range(10))
        assert r.llen('a') == 3

    def test_trim_rpush(self, r):
        r.trim_rpush('a', 3, *range(10))
        assert r.llen('a') == 3
