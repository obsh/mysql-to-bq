from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioningType, TimePartitioning


class BqClient:
    def __init__(self, project):
        # Construct a BigQuery client object.
        self.client = bigquery.Client(project=project)

    def create_table(self, table_id, plain_schema, dry_run=False):
        """
        Creates table in BQ. Auto-selects partitioning column.
        :param table_id: in format "your-project.your_dataset.your_table_name"
        :param plain_schema: list with dicts. e.g [ {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'}, ...]
        :param dry_run: if True - prints table summary
        :return:
        """
        time_fields = [p for p in plain_schema if p['type'] in ('TIMESTAMP', 'DATETIME', 'DATE')]

        schema = [bigquery.SchemaField(column['name'], column['type'], mode=column['mode']) for column in plain_schema]

        table = bigquery.Table(table_id, schema=schema)
        if len(time_fields) > 0:
            partitioning_field = time_fields[0]
            table.time_partitioning = TimePartitioning(TimePartitioningType.DAY, field=partitioning_field['name'])

        print(table)
        print(table.schema)
        print(table.time_partitioning)

        if dry_run:
            print('Dry run, table not created.')
        else:
            # Make an API request.
            table = self.client.create_table(table, exists_ok=True)
            print(
                "Table created or exists: {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
