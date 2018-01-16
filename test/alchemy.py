# -*- coding: utf-8 -*-
"""Unit tests of DrillDialect_sadrill methods"""

import os
import unittest
from sqlalchemy import create_engine, inspect

"""
COMMAND LINE EXAMPLES FOR TEST START FROM sqlalchemy-drill base folder:
GENERIC : python -m unittest -v -f test.<test module>
USING PARAMETERS :
# TEST LIBRARY WITH SQLALCHEMY
HOST=<drillhost> PORT=<drillport> DB=<workspace> FILE=<workspace_file> SSL=<if ssl> python -m unittest -v -f test.alchemy
"""


class TestDialectWithSQLAlchemy(unittest.TestCase):
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
        engine = create_engine(self.URL, echo=False)
        self.assertIsNotNone(engine)
        inspector = inspect(engine)
        self.assertIsNotNone(inspector)
        conn = engine.connect()
        self.assertIsNotNone(conn)
        return {
            'connection': conn,
            'engine': engine,
            'inspector': inspector
        }

    def close_connection(self):
        """Close the connection to SQLAlchemy"""
        self.conn.close()

    def setUp(self):
        """Instantiate the main class of DrillDialect and check the connection"""
        conn = self.connection()
        self.conn = conn['connection']
        self.engine = conn['engine']
        self.inspector = conn['inspector']
        self.assertIsNotNone(self.conn)

    def test_execuate_a_query(self):
        """Can it execute a query?"""
        rs = self.conn.execute("SELECT * FROM {table} LIMIT 10".format(table=self.TABLE))
        keys = rs.keys()
        res = [row for row in rs]
        self.close_connection()
        self.assertIsNotNone(rs)
        self.assertIsNotNone(keys)
        self.assertIsNotNone(res)

    def test_get_column_descriptions(self):
        """Does it take the description of the columns?"""
        schema_names = self.inspector.get_schema_names()
        self.close_connection()
        self.assertIsNotNone(schema_names)

    def test_get_schema_names(self):
        """Does it take the schema names?"""
        schema_names = self.inspector.get_schema_names()
        self.close_connection()
        self.assertIsNotNone(schema_names)

    def test_get_table_names(self):
        """Does it show the files inside a workspace?"""
        result = self.inspector.get_table_names()
        self.close_connection()
        self.assertIsNotNone(result)
        self.assertEqual(type(result), tuple)

    def test_get_columns(self):
        """Can it get the columns of a table?"""
        cols = self.inspector.get_columns(self.TABLE)
        self.close_connection()
        self.assertIsNotNone(cols)

    def test_has_tables(self):
        """Has it got the tables?"""
        result = self.engine.dialect.has_table(self.conn, self.TABLE)
        self.close_connection()
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
