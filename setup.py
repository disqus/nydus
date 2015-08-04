#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

try:
    __import__('multiprocessing')
except:
    pass

if 'nosetests' in sys.argv:
    setup_requires = ['nose']
else:
    setup_requires = []

PY3 = sys.version_info[0] == 3

tests_require = [
    'mock',
    'nose',
    'pylibmc',
    'redis',
    'riak',
    'thoonk',
    'unittest2',
]

if not PY3:
    tests_require += ['pycassa', ]

dependency_links = [
    'https://github.com/andyet/thoonk.py/tarball/master#egg=thoonk',
]


install_requires = [
]

setup(
    name='nydus',
    version='0.11.0',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='https://github.com/disqus/nydus',
    description='Connection utilities',
    packages=find_packages(exclude=('tests',)),
    zip_safe=False,
    setup_requires=setup_requires,
    install_requires=install_requires,
    dependency_links=dependency_links,
    tests_require=tests_require,
    extras_require={'test': tests_require},
    test_suite='nose.collector',
    include_package_data=True,
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
