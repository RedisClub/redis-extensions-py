from setuptools import setup


version = '4.1.1'


setup(
    name='redis-extensions',
    version=version,
    keywords='Redis Extensions',
    description='Redis-extensions is a collection of custom extensions for Redis-py.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',

    url='https://github.com/redisclub/redis-extensions-py',

    author='Hackathon',
    author_email='kimi.huang@brightcells.com',

    packages=['redis_extensions'],
    py_modules=[],
    python_requires='>=3.7',
    install_requires=['CodeConvert', 'TimeConvert', 'redis>=4.0.2', 'shortuuid', 'verification-code', 'graphic-verification-code>=1.0.3'],

    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
