from itertools import chain

from setuptools import setup


version = '4.0.12'


EXTRAS_REQUIRE = {
    'base': [],
    'vcode': ['verification-code'],
    'gvcode': ['graphic-verification-code<=1.0.3'],
    'advanced': ['verification-code', 'graphic-verification-code<=1.0.3'],
}
EXTRAS_REQUIRE['full'] = list(set(chain(*EXTRAS_REQUIRE.values())))


setup(
    name='redis-extensions',
    version=version,
    keywords='Redis Extensions',
    description='Redis-extensions is a collection of custom extensions for Redis-py.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',

    url='https://github.com/RedisClub/redis-extensions-py/tree/redis4.x+py3.6',

    author='Hackathon',
    author_email='kimi.huang@brightcells.com',

    packages=['redis_extensions'],
    py_modules=[],
    python_requires='>=3.6',
    install_requires=['TimeConvert', 'redis>=4.0.2', 'shortuuid'],
    extras_require=EXTRAS_REQUIRE,

    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
