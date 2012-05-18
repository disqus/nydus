#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

if 'nosetests' in sys.argv:
    setup_requires = ['nose']
else:
    setup_requires = []

tests_require = [
    'Django>=1.2,<1.5',
    'mock',
    'nose',
    'pep8',
    'psycopg2',
    'pyflakes',
    'redis',
    'riak',
    'thoonk==1.0.1.0',
    'unittest2',
]

dependency_links = [
    'https://github.com/andyet/thoonk.py/tarball/master#egg=thoonk',
    'https://github.com/dcramer/pyflakes/tarball/master#egg=pyflakes',
]


install_requires = [
]

setup(
    name='nydus',
    version='0.9.0',
    author='David Cramer',
    author_email='dcramer@gmail.com',
    url='http://github.com/disqus/nydus',
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
