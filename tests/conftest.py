# -*- coding: utf-8 -*-

import pytest

import redis_extensions as redis


def _get_client(cls, request=None, **kwargs):
    params = {'host': 'localhost', 'port': 6379, 'db': 9}
    params.update(kwargs)
    client = cls(**params)
    client.flushdb()
    if request:
        def teardown():
            client.flushdb()
            client.connection_pool.disconnect()
        request.addfinalizer(teardown)
    return client


def _get_client2(cls, request=None, **kwargs):
    params = {'host': 'localhost', 'port': 6379, 'db': 9}
    params.update(kwargs)
    client = cls(connection_pool=redis.ConnectionPool(**params), timezone='Asia/Shanghai')
    client.flushdb()
    if request:
        def teardown():
            client.flushdb()
            client.connection_pool.disconnect()
        request.addfinalizer(teardown)
    return client


@pytest.fixture()
def r(request, **kwargs):
    """
    redis.StrictRedisExtensions(host='localhost', port=6379, db=0)
    """
    return _get_client(redis.StrictRedisExtensions, request, **kwargs)


@pytest.fixture()
def r2(request, **kwargs):
    """
    redis.StrictRedisExtensions(host='localhost', port=6379, db=0, timezone='Asia/Shanghai')
    """
    return _get_client(redis.StrictRedisExtensions, request, **dict(kwargs, **{'timezone': 'Asia/Shanghai'}))


@pytest.fixture()
def r3(request, **kwargs):
    """
    redis.StrictRedisExtensions(connection_pool=redis.ConnectionPool(host='localhost', port=6379, db=0), timezone='Asia/Shanghai')
    """
    return _get_client2(redis.StrictRedisExtensions, request, **kwargs)
