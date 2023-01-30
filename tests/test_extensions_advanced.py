import json
import time

import pytest


class TestRedisExtensionsCommands(object):

    # Verification Codes Section

    def test_vcode(self, r):
        phone = '18888888888'
        code, overtop, blacklist = r.vcode(phone)
        assert len(code) == 6
        assert not overtop
        assert r.exists('r:vcode:phone:quota:' + phone)
        code, overtop, blacklist = r.vcode(phone, quota=1, req_interval=0)
        assert not code
        assert overtop
        code, overtop, blacklist = r.vcode(phone, quota=0, req_interval=0)
        assert len(code) == 6
        assert not overtop
        code, overtop, blacklist = r.vcode(phone, quota=0, req_interval=0, ndigits=4)
        assert len(code) == 4

    def test_vcode_quota(self, r):
        phone = '18888888888'
        ipaddr = 'localhost'
        _, _, _ = r.vcode(phone)
        assert r.vcode_quota(phone) == 1
        assert r.vcode_quota(ipaddr=ipaddr) == 0
        phone_quota, ipaddr_quota = r.vcode_quota(phone, ipaddr=ipaddr)
        assert phone_quota == 1
        assert ipaddr_quota == 0

    def test_vcode_exists(self, r):
        phone = '18888888888'
        code, overtop, blacklist = r.vcode(phone)
        assert r.vcode_exists(phone, code, keep=True)
        assert r.vcode_exists(phone, code)
        code, overtop, blacklist = r.vcode(phone, req_interval=0, code_cast_func=int)
        assert r.vcode_exists(phone, code, keep=True)
        assert not r.vcode_exists(phone, '4321')
        code, _, _ = r.vcode(phone, req_interval=0)
        assert r.vcode_exists(phone, code)
        assert not r.vcode_exists(phone, code)
        code, _, _ = r.vcode(phone, req_interval=0)
        assert not r.vcode_exists(phone, '4321', quota=1)
        assert not r.vcode_exists(phone, '4321', quota=1)
        assert not r.vcode_exists(phone, code)
        code, _, _ = r.vcode(phone, req_interval=0)
        assert r.vcode_exists(phone, code)
        code, _, _ = r.vcode(phone)
        assert r.vcode_exists(phone, code)

    def test_vcode_delete(self, r):
        phone = '18888888888'
        code, _, _ = r.vcode(phone)
        assert r.vcode_exists(phone, code)
        r.vcode_delete(phone)
        assert not r.vcode_exists(phone, code)

    # Graphic Verification Codes Section

    def __gvcode_test_key(self):
        return 'r:graphic:vcode:a'

    def test_gvcode_add(self, r):
        assert r.gvcode_add(10) == 10
        assert r.scard(r._gvcode_key()) == 10

        with pytest.raises(ValueError):
            r.gvcode_add(0)

    def test_gvcode_cut(self, r):
        assert r.gvcode_add(10) == 10
        assert r.gvcode_cut(5) == 5
        assert r.gvcode_cut(5) == 4

        with pytest.raises(ValueError):
            r.gvcode_cut(0)

    def test_gvcode_refresh(self, r):
        assert r.gvcode_refresh(1) == (0, 1)
        assert r.gvcode_add(9) == 9
        assert r.gvcode_refresh(10) == (9, 10)

        with pytest.raises(ValueError):
            r.gvcode_refresh(0)

    def test_gvcode_b64str(self, r):
        b64str = r.gvcode_b64str('a')
        assert isinstance(b64str, str)
        assert r.exists(self.__gvcode_test_key())

    def test_gvcode_exists(self, r):
        b64str = r.gvcode_b64str('a')
        code = r.get(self.__gvcode_test_key())
        assert r.gvcode_exists('a', code)
        assert r.gvcode_exists('a', code.lower())
        assert r.gvcode_exists('a', code.upper())
