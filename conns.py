from enum import Enum

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import arguments as arg
    from connectors.files import (
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
    )
    from connectors.databases import (
        AbstractDatabase,
        PostgresDatabase,
        ClickhouseDatabase,
        Table,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import fluxes as fx
    from .utils import arguments as arg
    from .connectors.files import (
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
    )
    from .connectors.databases import (
        AbstractDatabase,
        PostgresDatabase,
        ClickhouseDatabase,
        Table,
    )


class ConnType(Enum):
    LocalFolder = 'LocalFolder'
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    PostgresDatabase = 'PostgresDatabase'
    ClickhouseDatabase = 'ClickhouseDatabase'
    Table = 'Table'


def get_class(conn_type):
    if isinstance(conn_type, str):
        conn_type = ConnType(conn_type)
    message = 'conn_type must be an instance of ConnType (but {} as type {} received)'
    assert isinstance(conn_type, ConnType), TypeError(message.format(conn_type, type(conn_type)))
    if conn_type == ConnType.LocalFolder:
        return LocalFolder
    elif conn_type == ConnType.TextFile:
        return TextFile
    elif conn_type == ConnType.JsonFile:
        return JsonFile
    elif conn_type == ConnType.PostgresDatabase:
        return PostgresDatabase
    elif conn_type == ConnType.ClickhouseDatabase:
        return ClickhouseDatabase
    elif conn_type == ConnType.Table:
        return Table
