# -*- coding: utf-8 -*-
"""Unit tests of DrillDialect_sadrill methods"""

import os
import unittest
from sqlalchemy_drill.sadrill import DrillDialect_sadrill
from sqlalchemy_drill.drilldbapi import connect

"""
COMMAND LINE EXAMPLES FOR TEST START FROM sqlalchemy-drill base folder:
GENERIC : python -m unittest -v -f test.<test module>
USING PARAMETERS :
# TEST LIBRARY AS STANDALONE
HOST=<drillhost> PORT=<drillport> DB=<workspace> FILE=<workspace_file> SSL=<if ssl> python -m unittest -v -f test.base
"""


class UrlObject(object):
    """Mock of url object"""
    def __init__(self, database, host, port, username, password, query):
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.query = query


class TestDrillDialect(unittest.TestCase):
    """Tests for methods of DrillDialect_sadrill class"""
    HOST = os.environ.get('HOST')
    PORT = os.environ.get('PORT')
    DB = os.environ.get('DB')
    FILE = os.environ.get('FILE')
    SSL = os.environ.get('SSL')
    USERNAME = os.environ.get('USERNAME')
    PASSWORD = os.environ.get('PASSWORD')
    AUTH = ''
    if USERNAME and PASSWORD:
        AUTH = '{usr}:{pwd}@'.format(usr=USERNAME, pwd=PASSWORD)
    URL = 'drill+sadrill://{auth}{host}:{port}/{db}?use_ssl={ssl}'.format(
        auth=AUTH,
        host=HOST,
        port=PORT,
        db=DB,
        ssl=SSL
    )
    TABLE = '{db}.`{file}`'.format(db=DB, file=FILE)

    def connection(self):
        """Connection to Drill"""
        conn = connect(
            self.HOST,
            port=self.PORT,
            db=None,
            use_ssl=False,
            drilluser=None,
            drillpass=None,
            verify_ssl=False,
            ca_certs=None
        )
        self.assertTrue(conn.is_connected())
        cur = conn.cursor()
        self.assertIsNotNone(cur)
        return {
            'connection': conn,
            'cursor': cur
        }

    def create_connect_args(self):
        """Can parse the connection parameters from url string?"""
        self.assertIsNotNone(self.URL)
        dburl = UrlObject(
            database=self.URL,
            host=self.HOST,
            port=self.PORT,
            username=self.USERNAME,
            password=self.PASSWORD,
            query={
                'query': 'SHOW FILES'
            }
        )
        conn_args = self.drill.create_connect_args(dburl)
        self.assertEqual(type(conn_args), tuple)
        return conn_args

    def close_connection(self):
        """Close the connection to Drill"""
        self.conn.close_connection()

    def setUp(self):
        """Instantiate the main class of DrillDialect and check the connection"""
        self.drill = DrillDialect_sadrill()
        self.conn_args = self.create_connect_args()
        self.assertTrue(self.drill is not None)
        conn = self.connection()
        self.conn = conn['connection']
        self.cur = conn['cursor']

    def test_get_selected_workspace(self):
        """Does it hold the drill workspace?"""
        workspace = self.drill.get_selected_workspace()
        self.close_connection()
        self.assertIsNotNone(workspace)

    def test_get_selected_storage_plugin(self):
        """Can it select a storage plugin?"""
        storage_plugin = self.drill.get_selected_storage_plugin()
        self.close_connection()
        self.assertIsNotNone(storage_plugin)

    def test_get_schema_names(self):
        """Does it take the schema names?"""
        schema_names = self.drill.get_schema_names(self.cur)
        self.close_connection()
        self.assertIsNotNone(schema_names)

    def test_get_table_names(self):
        """Does it show the files inside a workspace?"""
        self.drill.db = self.DB
        result = self.drill.get_table_names(self.cur)
        self.close_connection()
        self.assertEqual(type(result), tuple)

    def test_get_columns(self):
        """Does it show the columns?"""
        result = self.drill.get_columns(self.cur, self.TABLE)
        self.close_connection()
        self.assertIsNotNone(result)

    def test_has_tables(self):
        """Has it got the tables?"""
        result = self.drill.has_table(self.cur, self.TABLE)
        self.close_connection()
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
