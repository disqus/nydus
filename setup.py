#!/usr/bin/env python

from setuptools import setup, find_packages

tests_require = [
    'nose',
    'unittest2',
    'mock',
    'redis',
    'Django>=1.2,<1.4',
    'psycopg2',
]

install_requires = [
]

setup(
    name='nydus',
    version='0.6.3',
    author='David Cramer',
    author_email='dcramer@gmail.com',
    url='http://github.com/disqus/nydus',
    description='Connection utilities',
    packages=find_packages(exclude=('tests',)),
    zip_safe=False,
    install_requires=install_requires,
    dependency_links=[],
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
