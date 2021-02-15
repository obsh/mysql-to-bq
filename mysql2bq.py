from sqlalchemy.dialects.mysql import TINYINT, INTEGER, TIMESTAMP, VARCHAR, TEXT

__TYPES_CONVERSION = {
    INTEGER: 'INTEGER',
    TINYINT: 'BOOLEAN',
    TIMESTAMP: 'TIMESTAMP',
    VARCHAR: 'STRING',
    TEXT: 'STRING'
}


def convert_schema(schema):
    """
    Converts MySQL schema to BQ schema.

    For example:
    - schema:
    [
        {'name': 'id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False, 'autoincrement': True},
        {'name': 'post_id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False, 'autoincrement': False},
        {'name': 'main', 'type': TINYINT(), 'default': None, 'comment': None, 'nullable': False, 'autoincrement': False},
        {'name': 'created_at', 'type': TIMESTAMP(), 'default': None, 'comment': None, 'nullable': False},
        {'name': 'updated_at', 'type': TIMESTAMP(), 'default': None, 'comment': None, 'nullable': False}
    ]
    returns:
    [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'post_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'main', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]
    """
    return [convert_column(column) for column in schema]


def convert_column(column):
    """
    Converts MySQL column description to BQ column description.

    For example:
    - columns: {'name': 'id', 'type': INTEGER(), 'default': None, 'comment': None, 'nullable': False, 'autoincrement': True}
    - returns: {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    """
    return {
        'name': column['name'],
        'type': __convert_type(column['type']),
        'mode': 'NULLABLE'
    }


def __convert_type(mysql_type):
    """
    Converts MySQL column type to BQ column description.

    For example:
    - type: INTEGER()
    - returns: 'INTEGER'
    """
    for known_type, bq_type in __TYPES_CONVERSION.items():
        if type(mysql_type) is known_type:
            return bq_type

    raise ValueError('{} is not a known type'.format(mysql_type))
