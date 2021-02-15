import unittest
from ddt import ddt, idata, unpack
from mysql2bq import convert_column, convert_schema
from sqlalchemy.dialects.mysql import TINYINT, TIMESTAMP, INTEGER
# for failure test
from sqlalchemy.dialects.postgresql import INET


def columns_generator():
    pairs = [
        ({'name': 'id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False, 'autoincrement': True},
         {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'}),
        ({'name': 'main', 'type': TINYINT(), 'default': None, 'comment': None, 'nullable': False,
          'autoincrement': False},
         {'name': 'main', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}),
        ({'name': 'created_at', 'type': TIMESTAMP(), 'default': None, 'comment': None, 'nullable': False},
         {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
         )
    ]
    for p in pairs:
        yield p


@ddt
class TestMySqlToBq(unittest.TestCase):

    def test_convert_schema(self):
        schema = [
            {'name': 'id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False,
             'autoincrement': True},
            {'name': 'post_id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False,
             'autoincrement': False},
            {'name': 'main', 'type': TINYINT(), 'default': None, 'comment': None, 'nullable': False,
             'autoincrement': False},
            {'name': 'created_at', 'type': TIMESTAMP(), 'default': None, 'comment': None, 'nullable': False},
            {'name': 'updated_at', 'type': TIMESTAMP(), 'default': None, 'comment': None, 'nullable': False}
        ]

        self.assertEqual([
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'main', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ], convert_schema(schema))

    @idata(columns_generator())
    @unpack
    def test_convert_column(self, mysql_column, expected_bq_column):
        bq_column = convert_column(mysql_column)

        self.assertEqual(expected_bq_column, bq_column)

    def test_convert_column_fails(self):
        # INET is a PostgreSQL specific type
        column = {'name': 'address', 'type': INET(), 'default': None, 'comment': None, 'nullable': False,
                  'autoincrement': True}
        with self.assertRaises(ValueError) as context:
            convert_column(column)


if __name__ == '__main__':
    unittest.main()
