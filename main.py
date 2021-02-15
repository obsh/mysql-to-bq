import json
import argparse

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from mysql2bq import convert_schema, generate_select_statement
from bq import BqClient

if __name__ == '__main__':
    """
     Script checks MySQL tables and creates corresponding BQ tables.
     Notes:
     - just skips table if it already exists without notifying;
     - uses first time(TIMESTAMP, DATE, DATETIME) field for partitioning.
    """
    parser = argparse.ArgumentParser(description='Helper tool to import all tables from a MySQL db into BQ.')
    parser.add_argument('connection_string',
                        help='SQLAlchemy connection string to MySQL DB, '
                             'e.g. \'mysql+mysqldb://user:password@127.0.0.1/database\'')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--dataset', required=True, help='BQ dataset')
    parser.add_argument('--salt', required=True, help='Salt applied to sensitive data hashing')
    parser.add_argument('--dry_run', dest='dry_run', action='store_true')

    args = parser.parse_args()

    engine = create_engine(args.connection_string)
    inspector = reflection.Inspector.from_engine(engine)
    table_names = inspector.get_table_names()

    bq = BqClient(args.project)
    for table in table_names:
        schema = inspector.get_columns(table)
        bq_schema = convert_schema(schema)
        print('Creating: "{}" table'.format(table))

        bq.create_table('{}.{}.{}'.format(args.project, args.dataset, table), bq_schema, args.dry_run)

        select_statement = generate_select_statement(table, bq_schema, args.salt)
        # TODO: generate `gcloud dataflow` commands for data import
