#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

try:
    __import__('multiprocessing')
except ImportError:
    pass

if 'nosetests' in sys.argv:
    setup_requires = ['nose3']
else:
    setup_requires = []

tests_require = [
    'mock',
    'nose3',
    'pylibmc',
    'redis',
]

install_requires = [
    'six',
]

setup(
    name='nydus',
    version='0.12.0',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='https://github.com/disqus/nydus',
    description='Connection utilities',
    packages=find_packages(exclude=('tests',)),
    zip_safe=False,
    setup_requires=setup_requires,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={'test': tests_require},
    test_suite='nose.collector',
    include_package_data=True,
    license='Apache License 2.0',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
