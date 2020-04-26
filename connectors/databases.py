from abc import ABC, abstractmethod
import requests
import psycopg2

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import (
        arguments as arg,
        functions as fs,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import (
        arguments as arg,
        functions as fs,
    )


AUTO = arg.DEFAULT
COMMON_PROPS = ['verbose', ]
TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'


class AbstractDatabase(ABC):
    def __init__(self, host, port, db, login, password, verbose=arg.DEFAULT, context=None):
        self.host = host
        self.port = port
        self.db = db
        self.login = login
        self.password = password
        self.connection = None
        self.tables = dict()
        self.verbose = verbose
        self.context = context
        if context is not None:
            for prop in COMMON_PROPS:
                if hasattr(context, prop):
                    setattr(self, prop, getattr(context, prop))

    def get_context(self):
        return self.context

    def get_table(self, name, **kwargs):
        table = self.tables.get(name)
        if table:
            assert not kwargs, 'table connection {} is already registered'.format(name)
        else:
            table = Table(name, database=self, **kwargs)
            self.tables[name] = table
        return table

    @abstractmethod
    def execute(self, query, get_data=AUTO, commit=AUTO, verbose=arg.DEFAULT):
        pass

    def create_table(self, name, schema, drop_if_exists=False, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if isinstance(schema, str):
            schema_str = schema
        else:
            schema_str = ['{} {}'.format(c[0], c[1]) for c in schema]
        if drop_if_exists:
            self.drop_table(name, verbose=verbose)
        message = 'Creating table:'
        query = 'CREATE TABLE {name} ({schema});'.format(
            name=name,
            schema=schema_str,
        )
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )
        self.post_create_action(name, verbose=verbose)
        if verbose:
            print('Table {name} is created.'.format(name=name))

    def post_create_action(self, name, **kwargs):
        pass

    def drop_table(self, name, verbose=arg.DEFAULT):
        query = 'DROP TABLE IF EXISTS {name};'.format(name=name)
        self.execute(query, verbose)

    def copy_table(self, old, new, verbose=arg.DEFAULT):
        query = 'CREATE TABLE {new} AS TABLE {old};'.format(new=new, old=old)
        self.execute(query, verbose)

    def rename_table(self, old, new, verbose=arg.DEFAULT):
        query = 'ALTER TABLE {old} RENAME TO {new};'.format(old=old, new=new)
        self.execute(query, verbose)

    def select(self, table_name, fields, filters=None, verbose=arg.DEFAULT):
        fields_str = fields if isinstance(fields, str) else ', '.join(fields)
        filters_str = filters if isinstance(filters, str) else ' AND '.join(filters) if filters is not None else ''
        if filters:
            query = 'SELECT {fields} FROM {table} WHERE {filters};'.format(
                table=table_name,
                fields=fields_str,
                filters=filters_str,
            )
        else:
            query = 'SELECT {fields} FROM {table};'.format(
                table=table_name,
                fields=fields_str,
            )
        return self.execute(query, get_data=True, commit=False, verbose=verbose)

    def select_count(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='COUNT(*)', verbose=verbose)

    def select_all(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='*', verbose=verbose)

    @abstractmethod
    def insert_rows(self, table, rows, columns, step, skip_errors=False, verbose=arg.DEFAULT):
        pass

    def insert_schematized_flux(self, table, flux, skip_errors=False, verbose=arg.DEFAULT):
        schema = flux.get_schema()
        columns = [c[0] for c in schema]
        final_count = flux.apply(
            lambda a: self.insert_rows(table, rows=a, columns=columns, skip_errors=skip_errors, verbose=verbose),
        ).final_count()
        return final_count

    def insert_data(self, table, data, schema=tuple(), skip_lines=0, skip_errors=False, verbose=arg.DEFAULT):
        if fx.is_flux(data):
            fx_input = data
        elif isinstance(data, str):
            fx_input = fx.RowsFlux.from_csv_file(
                filename=data,
                verbose=verbose,
            )
        else:
            fx_input = fx.AnyFlux(data)
        if skip_lines:
            fx_input = fx_input.skip(skip_lines)
        if fx_input.flux_type() != fx.FluxType.SchemaFlux:
            fx_input = fx_input.schematize(
                schema,
                skip_bad_rows=True,
                verbose=True,
            )
        initial_count = fx_input.count
        final_count = self.insert_schematized_flux(table, fx_input, skip_errors=skip_errors, verbose=verbose)
        return initial_count, final_count

    def write_table(
            self,
            table, schema, data,
            skip_lines=0, skip_errors=False, max_error_percent=0.0,
            verbose=arg.DEFAULT,
    ):
        tmp_name = table + '_tmp_upload'
        bak_name = table + '_bak'
        verbose = arg.undefault(verbose, self.verbose)
        if not skip_lines:
            self.create_table(tmp_name, schema=schema, drop_if_exists=True, verbose=verbose)
        initial_count, write_count = self.insert_data(
            tmp_name, schema=schema, data=data,
            skip_lines=skip_lines, skip_errors=skip_errors,
        )
        write_count += skip_lines
        result_count = self.select_count(tmp_name)
        if verbose:
            print('Check counts:', initial_count, write_count, result_count)
        assert (write_count - result_count) / write_count < max_error_percent, 'Too many errors or skipped lines'
        self.drop_table(bak_name, verbose=verbose)
        self.rename_table(table, bak_name, verbose=verbose)
        self.rename_table(tmp_name, table, verbose=verbose)


class PostgresDatabase(AbstractDatabase):
    def __init__(self, host, port, db, login, password, context=None):
        super().__init__(
            host=host,
            port=port,
            db=db,
            login=login,
            password=password,
            context=context,
        )

    def is_connected(self):
        return self.connection is not None

    def get_connection(self, connect=False):
        if connect and not self.connection:
            self.connect()
        return self.connection

    def connect(self):
        self.disconnect(True)
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            db=self.db,
            login=self.login,
            password=self.password,
        )
        return self.connection

    def disconnect(self, skip_errors=False, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if skip_errors:
            try:
                self.connection.close()
            except psycopg2.OperationalError:
                if verbose:
                    print('Connection to {} already closed.'.format(self.host))
        else:
            self.connection.close()

    def execute(self, query, get_data=AUTO, commit=AUTO, data=None, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if get_data == AUTO:
            if 'SELECT' in query and 'GRANT' not in query:
                get_data, commit = True, False
            else:
                get_data, commit = False, True
        if verbose:
            message = verbose if isinstance(verbose, str) else 'Execute:'
            print(message, query)
        has_connection = self.is_connected()
        cur = self.get_connection().cursor()
        if data:
            result = cur.execute(query, data)
        else:
            result = cur.execute(query, data)
        if commit:
            self.get_connection().commit()
        cur.close()
        if not has_connection:
            self.connection.close()
        if get_data:
            return result

    def grant_permission(self, name, permission='SELECT', group=DEFAULT_GROUP, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        message = 'Grant access:'
        query = 'GRANT {permission} ON {name} TO {group};'.format(
            name=name,
            permission=permission,
            group=group,
        )
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )

    def post_create_action(self, name, verbose=arg.DEFAULT):
        self.grant_permission(name, verbose=verbose)

    def insert_rows(self, table, rows, columns, step, skip_errors=False, expected_count=None, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
        conn = self.get_connection(connect=True)
        cur = conn.cursor()
        placeholders = ['%s' for _ in columns]
        query = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=table,
            columns=', '.join(columns),
            values=', '.join(placeholders),
        )
        n = 0
        for n, row in enumerate(rows):
            if skip_errors:
                try:
                    cur.execute(query, row)
                except TypeError as e:  # TypeError: not all arguments converted during string formatting
                    if verbose:
                        print('Error line:', str(row)[:80], '...')
                    print(e.__class__.__name__, e)
            else:
                cur.execute(query, row)
            if (n + 1) % step == 0:
                if verbose:
                    message = verbose if isinstance(verbose, str) else 'Committing {step} lines into {table}'.format(
                        step=step, table=table,
                    )
                    if count:
                        percent = fs.percent(str)((n + 1) / count)
                        message = '{}: {} ({}/{}) lines processed'.format(message, percent, n + 1, count)
                    else:
                        message = '{}: {} lines processed'.format(message, n + 1)
                    print(' ' * 80, end='\r')
                    print(message, end='\r')
                conn.commit()
        if verbose:
            message = 'Committing last {count} lines into {table}...'.format(count=n % step, table=table)
            print(message, end='\r')
        conn.commit()
        if verbose:
            message = 'Done. {count} lines has written into {table}...'.format(count=n, table=table)
            print(message)
        return rows


class ClickhouseDatabase(AbstractDatabase):
    def __init__(
            self,
            host='localhost',
            port=8443,
            db='public',
            login=arg.DEFAULT,
            password=arg.DEFAULT,
            cert_filename=None,
            context=None
    ):
        super().__init__(
            host=host,
            port=port,
            db=db,
            login=login,
            password=password,
            context=context,
        )
        self.cert_filename = cert_filename

    def execute(self, query=TEST_QUERY, get_data=AUTO, commit=AUTO, verbose=True):
        url = 'https://{host}:{port}/?database={db}&query={query}'.format(
            host=self.host,
            port=self.port,
            db=self.db,
            query=query,
        )
        auth = {
            'X-ClickHouse-User': self.login,
            'X-ClickHouse-Key': self.password,
        }
        request_props = {'headers': auth}
        if self.cert_filename:
            request_props['verify'] = self.cert_filename
        if verbose:
            print('Execute query:', query)
        res = requests.get(
            url,
            **request_props
        )
        res.raise_for_status()
        if get_data:
            return res.text

    def insert_rows(self, table, rows, columns, step, skip_errors=False, expected_count=None, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
        query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=table,
            columns=', '.join(columns),
            values='{}',
        )
        for n, row in enumerate(rows):
            values = ', '.format(row)
            cur_query = query_template.format(values)
            if skip_errors:
                try:
                    self.execute(cur_query)
                except requests.RequestException as e:
                    if verbose:
                        print('Error line:', str(row)[:80], '...')
                    print(e.__class__.__name__, e)
            else:
                self.execute(cur_query)
            if (n + 1) % step == 0:
                if verbose:
                    message = verbose if isinstance(verbose, str) else 'Inserting into {table}'.format(
                        table=table,
                    )
                    if count:
                        percent = fs.percent(str)((n + 1) / count)
                        message = '{}: {} ({}/{}) lines processed'.format(message, percent, n + 1, count)
                    else:
                        message = '{}: {} lines processed'.format(message, n + 1)
                    print(' ' * 80, end='\r')
                    print(message, end='\r')
        if verbose:
            message = 'Done. {count} lines has written into {table}...'.format(count=n, table=table)
            print(message)
        return rows


class Table:
    def __init__(
            self,
            name,
            schema,
            database,
            **kwargs
    ):
        self.name = name
        self.schema = schema
        assert isinstance(database, AbstractDatabase)
        self.database = database
        self.meta = kwargs

    def get_context(self):
        return self.database.context

    def get_data(self, verbose=arg.DEFAULT):
        return self.database.select_all(self.name, verbose=verbose)

    def get_count(self, verbose=arg.DEFAULT):
        return self.database.select_count(self.name, verbose=verbose)

    def create(self, drop_if_exists, verbose=arg.DEFAULT):
        return self.database.create_table(
            self.name,
            schema=self.schema,
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(self, data, skip_lines=0, skip_errors=False, verbose=arg.DEFAULT):
        return self.database.insert_data(
            self.name,
            data=data,
            schema=self.schema,
            skip_lines=skip_lines,
            skip_errors=skip_errors,
            verbose=verbose,
        )
