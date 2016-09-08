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


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(redis.StrictRedisExtensions, request, **kwargs)
