"""
tests.test_backends.test_django
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from .. import BaseTest

from django.conf import settings

if not settings.configured:
    settings.configure(
        DATABASES={
            'default': {
                'ENGINE': 'nydus.contrib.django.backend',
                'NAME': 'django/sqlite3',
            },
            # 'psycopg2': {
            #     'ENGINE': 'nydus.contrib.django.backend',
            #     'NAME': 'django/psycopg2',
            # },
        },
        NYDUS_CONFIG={
            'CONNECTIONS': {
                'django/sqlite3': {
                    'engine': 'nydus.contrib.django.DjangoDatabase',
                    'hosts': {
                        0: {'backend': 'django.db.backends.sqlite3', 'name': ':memory:'},
                    },
                },
                # 'django/psycopg2': {
                #     'engine': 'nydus.contrib.django.DjangoDatabase',
                #     'hosts': {
                #         0: {'backend': 'django.db.backends.postgresql_psycopg2', 'name': 'nydus'},
                #     },
                # },
            },
        },
        # HACK: this fixes our threaded runserver remote tests
        # DATABASE_NAME='test_sentry',
        # TEST_DATABASE_NAME='test_sentry',
        INSTALLED_APPS=[
            'nydus.contrib.django',

            'tests',
        ],
        ROOT_URLCONF='',
        DEBUG=False,
        SITE_ID=1,
        TEMPLATE_DEBUG=True,
    )

from nydus.contrib.django import DjangoDatabase
from nydus.db import Cluster

class DjangoConnectionsTest(BaseTest):
    def test_simple(self):
        from django.db import connections
        
        cursor = connections['default'].execute('SELECT 1')
        self.assertEquals(cursor.fetchone(), (1,))

class DjangoSQLiteTest(BaseTest):
    def setUp(self):
        from django.db.backends import sqlite3

        self.db = DjangoDatabase(sqlite3, name=':memory:', num=0)

    def test_proxy(self):
        cursor = self.db.execute('SELECT 1')
        self.assertEquals(cursor.fetchone(), (1,))
    
    def test_with_cluster(self):
        p = Cluster(
            hosts={0: self.db},
        )
        cursor = p.execute('SELECT 1')
        self.assertEquals(cursor.fetchone(), (1,))

    def test_provides_identififer(self):
        self.assertEqual(
            "django.db.backends.sqlite3NAME=:memory: PORT=None HOST=None USER=None TEST_NAME=None PASSWORD=None OPTIONS={}",
            self.db.identifier
        )

# class DjangoPsycopg2Test(BaseTest):
#     def setUp(self):
#         from django.db.backends import postgresql_psycopg2
# 
#         self.db = DjangoDatabase(postgresql_psycopg2, name='nydus_test', num=0)
# 
#     def test_proxy(self):
#         cursor = self.db.execute('SELECT 1')
#         self.assertEquals(cursor.fetchone(), (1,))
#     
#     def test_with_cluster(self):
#         p = Cluster(
#             hosts={0: self.db},
#         )
#         cursor = p.execute('SELECT 1')
#         self.assertEquals(cursor.fetchone(), (1,))
