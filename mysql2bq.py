from sqlalchemy.dialects.mysql import TINYINT, SMALLINT, INTEGER, \
    TIMESTAMP, DATETIME, \
    VARCHAR, TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT, ENUM

__TYPES_CONVERSION = {
    TINYINT: 'BOOLEAN',
    SMALLINT: 'INTEGER',
    INTEGER: 'INTEGER',
    TIMESTAMP: 'TIMESTAMP',
    DATETIME: 'DATETIME',
    VARCHAR: 'STRING',
    TINYTEXT: 'STRING',
    MEDIUMTEXT: 'STRING',
    TEXT: 'STRING',
    LONGTEXT: 'STRING',
    ENUM: 'STRING'
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


def generate_select_statement(table_name, table_schema, salt, sensitive_fields=None, ):
    """
    Return SQL select statement string applying UNIX_TIMESTAMP to TIMESTAMP fields and hash sensitive with salt
    """

    if sensitive_fields is None:
        sensitive_fields = ['email', 'username', 'first_name', 'last_name']

    schema = convert_schema(table_schema)

    def prep_field(field):
        f_name = field['name']
        f_type = field['type']

        if f_type == 'TIMESTAMP':
            return f'UNIX_TIMESTAMP({f_name})'
        if f_name in sensitive_fields:
            return f'SHA2({f_name},{salt})'

        return f_name

    field_list = [prep_field(f) for f in schema]
    fields = ', '.join(field_list)

    statement = f'SELECT {fields} FROM {table_name}'

    return statement
