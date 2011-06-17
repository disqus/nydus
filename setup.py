#!/usr/bin/env python

# Copyright 2011 DISQUS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

tests_require = [
    'nose',
    'unittest2',
    'dingus',
    'redis',
    'Django>=1.2,<1.4',
    'psycopg2',
]

install_requires = [
]

setup(
    name='nydus',
    version='0.1',
    author='David Cramer',
    author_email='dcramer@gmail.com',
    url='http://github.com/disqus/nydus',
    description = 'Connection utilities',
    packages=find_packages(),
    zip_safe=False,
    install_requires=install_requires,
    dependency_links=[],
    tests_require=tests_require,
    extras_require={'test': tests_require},
    test_suite='nose.collector',
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
