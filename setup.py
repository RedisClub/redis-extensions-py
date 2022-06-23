# -*- coding: utf-8 -*-

from setuptools import setup


version = '3.0.0'


setup(
    name='redis-extensions',
    version=version,
    keywords='Redis Extensions',
    description='Redis-extensions is a collection of custom extensions for Redis-py.',
    long_description=open('README.rst').read(),

    url='https://github.com/redisclub/redis-extensions-py',

    author='Hackathon',
    author_email='kimi.huang@brightcells.com',

    packages=['redis_extensions'],
    py_modules=[],
    install_requires=['CodeConvert', 'TimeConvert', 'redis>=3.0.0,<=3.5.3', 'shortuuid', 'verification-code', 'graphic-verification-code>=1.0.3'],

    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
