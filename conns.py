from enum import Enum

try:  # Assume we're a sub-module in a package.
    from connectors.files import (
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
        CsvFile,
        TsvFile,
    )
    from connectors.databases import (
        AbstractDatabase,
        PostgresDatabase,
        ClickhouseDatabase,
        Table,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .connectors.files import (
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
        CsvFile,
        TsvFile,
    )
    from .connectors.databases import (
        AbstractDatabase,
        PostgresDatabase,
        ClickhouseDatabase,
        Table,
    )


CONN_CLASSES = (
    AbstractDatabase, Table,
    PostgresDatabase, ClickhouseDatabase,
    LocalFolder, AbstractFile,
    TextFile, JsonFile, CsvFile, TsvFile,
)
DATABASE_TYPES = [PostgresDatabase.__class__.__name__, ClickhouseDatabase.__class__.__name__]
DICT_EXT_TO_TYPE = {'txt': TextFile, 'json': JsonFile, 'csv': CsvFile, 'tsv': TsvFile}


class ConnType(Enum):
    LocalFolder = 'LocalFolder'
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'
    PostgresDatabase = 'PostgresDatabase'
    ClickhouseDatabase = 'ClickhouseDatabase'
    Table = 'Table'


def get_class(conn_type):
    if conn_type in CONN_CLASSES:
        return conn_type
    elif isinstance(conn_type, str):
        conn_type = ConnType(conn_type)
    message = 'conn_type must be an instance of ConnType (but {} as type {} received)'
    assert isinstance(conn_type, ConnType), TypeError(message.format(conn_type, type(conn_type)))
    if conn_type == ConnType.LocalFolder:
        return LocalFolder
    elif conn_type == ConnType.TextFile:
        return TextFile
    elif conn_type == ConnType.JsonFile:
        return JsonFile
    elif conn_type == ConnType.CsvFile:
        return CsvFile
    elif conn_type == ConnType.TsvFile:
        return CsvFile
    elif conn_type == ConnType.PostgresDatabase:
        return PostgresDatabase
    elif conn_type == ConnType.ClickhouseDatabase:
        return ClickhouseDatabase
    elif conn_type == ConnType.Table:
        return Table


def is_conn(obj):
    return isinstance(obj, CONN_CLASSES)


def is_file(obj):
    return isinstance(obj, (TextFile, JsonFile, CsvFile, TsvFile))


def is_folder(obj):
    return obj.__class__.__name__ == 'LocalFolder'


def is_database(obj):
    return obj.__class__.__name__ in DATABASE_TYPES
