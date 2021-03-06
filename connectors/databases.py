from abc import abstractmethod
from enum import Enum
import gc
import requests
import psycopg2
import psycopg2.extras

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    import conns as cs
    from connectors import abstract as ac
    from utils import (
        arguments as arg,
        schema as sh,
        functions as fs,
        mappers as ms,
        log_progress,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from .. import conns as cs
    from ..connectors import abstract as ac
    from ..utils import (
        arguments as arg,
        schema as sh,
        functions as fs,
        mappers as ms,
        log_progress,
    )


AUTO = arg.DEFAULT
TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'
DEFAULT_STEP = 1000
DEFAULT_ERRORS_THRESHOLD = 0.05


class DatabaseType(Enum):
    PostgresDatabase = 'pg'
    ClickhouseDatabase = 'ch'


class AbstractDatabase(ac.AbstractStorage):
    def __init__(self, name, host, port, db, user, password, verbose=AUTO, context=None, **kwargs):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn_kwargs = kwargs
        self.connection = None

    def get_default_child_class(self):
        return Table

    def get_tables(self):
        return self.get_items()

    def table(self, name, schema=None, **kwargs):
        table = self.get_tables().get(name)
        if table:
            assert not kwargs, 'table connection {} is already registered'.format(name)
        else:
            assert schema is not None, 'for create table schema must be defined'
            table = Table(name, schema=schema, database=self, **kwargs)
            self.get_tables()[name] = table
        return table

    def close(self):
        if hasattr(self, 'disconnect'):
            return self.disconnect()

    def get_links(self):
        for item in self.get_items():
            yield from item.get_links()

    @classmethod
    def need_connection(cls):
        return hasattr(cls, 'connection')

    @abstractmethod
    def get_dialect_name(self):
        pass

    @abstractmethod
    def exists_table(self, name, verbose=arg.DEFAULT):
        pass

    @abstractmethod
    def execute(self, query, get_data=AUTO, commit=AUTO, verbose=arg.DEFAULT):
        pass

    def execute_if_exists(
            self, query, table,
            message_if_yes=None, message_if_no=None, stop_if_no=False, verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, message_if_yes or message_if_no)
        table_exists = self.exists_table(table, verbose=verbose)
        if table_exists:
            if '{}' in query:
                query = query.format(table)
            result = self.execute(query, verbose=verbose)
            if message_if_yes:
                if '{}' in message_if_yes:
                    message_if_yes = message_if_yes.format(table)
                self.log(message_if_yes, verbose=verbose)
            return result
        else:
            if message_if_no and '{}' in message_if_no:
                message_if_no = message_if_no.format(table)
            if stop_if_no:
                raise ValueError(message_if_no)
            else:
                if message_if_no:
                    self.log(message_if_no, verbose=verbose)

    def create_table(self, name, schema, drop_if_exists=False, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if isinstance(schema, sh.SchemaDescription):
            schema_str = schema.get_schema_str(dialect=self.get_dialect_name())
        elif isinstance(schema, str):
            schema_str = schema
            message = 'String Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=log_progress.LoggingLevel.Warning)
        else:
            schema_str = ', '.join(['{} {}'.format(c[0], c[1]) for c in schema])
            message = 'Tuple Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=log_progress.LoggingLevel.Warning)
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
        self.log('Table {name} is created.'.format(name=name), verbose=verbose)

    def post_create_action(self, name, **kwargs):
        pass

    def drop_table(self, name, if_exists=True, verbose=arg.DEFAULT):
        self.execute_if_exists(
            query='DROP TABLE IF EXISTS {};',
            table=name,
            message_if_yes='Table {} has been dropped',
            message_if_no='Table {} did not exists before, nothing dropped.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def copy_table(self, old, new, if_exists=False, verbose=arg.DEFAULT):
        cat_old, name_old = old.split('.')
        cat_new, name_new = new.split('.') if '.' in new else cat_old, new
        assert cat_new == cat_old, 'Can copy within same scheme (folder) only'
        new = name_new
        self.execute_if_exists(
            query='CREATE TABLE {new} AS TABLE {old};'.format(new=new, old=old),
            table=old,
            message_if_yes='Table {old} is copied to {new}'.format(old=old, new=new),
            message_if_no='Can not copy table {}: not exists',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def rename_table(self, old, new, if_exists=False, verbose=arg.DEFAULT):
        cat_old, name_old = old.split('.')
        cat_new, name_new = new.split('.') if '.' in new else (cat_old, new)
        assert cat_new == cat_old, 'Can copy within same scheme (folder) only (got {} and {})'.format(cat_new, cat_old)
        new = name_new
        self.execute_if_exists(
            query='ALTER TABLE {old} RENAME TO {new};'.format(old=old, new=new),
            table=old,
            message_if_yes='Table {old} is renamed to {new}'.format(old=old, new=new),
            message_if_no='Can not rename table {}: not exists.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

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
        return self.select(table, fields='COUNT(*)', verbose=verbose)[0][0]

    def select_all(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='*', verbose=verbose)

    @abstractmethod
    def insert_rows(
            self, table, rows, columns,
            step=DEFAULT_STEP, skip_errors=False,
            expected_count=arg.DEFAULT, return_count=True,
            verbose=arg.DEFAULT,
    ):
        pass

    def insert_schematized_flux(self, table, flux, skip_errors=False, step=DEFAULT_STEP, verbose=arg.DEFAULT):
        columns = flux.get_columns()
        expected_count = flux.count
        final_count = flux.calc(
            lambda a: self.insert_rows(
                table, rows=a, columns=columns,
                step=step, expected_count=expected_count,
                skip_errors=skip_errors, return_count=True,
                verbose=verbose,
            ),
        )
        return final_count

    def insert_data(
            self, table, data, schema=tuple(),
            encoding=None, skip_first_line=False,
            skip_lines=0, skip_errors=False, step=DEFAULT_STEP,
            verbose=arg.DEFAULT,
    ):
        if not isinstance(schema, sh.SchemaDescription):
            message = 'Schema as {} is deprecated, use sh.SchemaDescription instead'.format(type(schema))
            self.log(msg=message, level=log_progress.LoggingLevel.Warning)
            schema = sh.SchemaDescription(schema)
        if fx.is_flux(data):
            fx_input = data
        elif cs.is_file(data):
            fx_input = data.to_schema_flux()
            assert fx_input.get_columns() == schema.get_columns()
        elif isinstance(data, str):
            fx_input = fx.RowsFlux.from_csv_file(
                filename=data,
                encoding=encoding,
                skip_first_line=skip_first_line,
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
            ).update_meta(
                count=fx_input.count,
            )
        initial_count = fx_input.count + skip_lines
        final_count = self.insert_schematized_flux(
            table, fx_input,
            skip_errors=skip_errors, step=step,
            verbose=verbose,
        )
        return initial_count, final_count

    def force_upload_table(
            self,
            table, schema, data,
            encoding=None,
            step=DEFAULT_STEP,
            skip_lines=0, skip_first_line=False, max_error_rate=0.0,
            verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, self.verbose)
        if not skip_lines:
            self.create_table(table, schema=schema, drop_if_exists=True, verbose=verbose)
        skip_errors = (max_error_rate is None) or (max_error_rate > DEFAULT_ERRORS_THRESHOLD)
        initial_count, write_count = self.insert_data(
            table, schema=schema, data=data,
            encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, skip_errors=skip_errors,
            verbose=verbose,
        )
        write_count += skip_lines
        result_count = self.select_count(table)
        error_rate = (write_count - result_count) / write_count
        message = 'Check counts: {} initial, {} uploaded, {} written, {} error_rate'
        self.log(message.format(initial_count, write_count, result_count, error_rate), verbose=verbose)
        if max_error_rate is not None:
            message = 'Too many errors or skipped lines ({} > {})'.format(error_rate, max_error_rate)
            assert error_rate < max_error_rate, message

    def safe_upload_table(
            self,
            table, schema, data,
            encoding=None,
            step=DEFAULT_STEP,
            skip_lines=0, skip_first_line=False, max_error_rate=0.0,
            verbose=arg.DEFAULT,
    ):
        tmp_name = '{}_tmp_upload'.format(table)
        bak_name = '{}_bak'.format(table)
        verbose = arg.undefault(verbose, self.verbose)
        self.force_upload_table(
            table=tmp_name, schema=schema, data=data, encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, max_error_rate=max_error_rate,
            verbose=verbose,
        )
        self.drop_table(bak_name, if_exists=True, verbose=verbose)
        self.rename_table(table, bak_name, if_exists=True, verbose=verbose)
        self.rename_table(tmp_name, table, if_exists=True, verbose=verbose)


class PostgresDatabase(AbstractDatabase):
    def __init__(self, name, host, port, db, user, password, context=None, **kwargs):
        super().__init__(
            name=name,
            host=host,
            port=port,
            db=db,
            user=user,
            password=password,
            context=context,
            **kwargs
        )

    def is_connected(self):
        return (self.connection is not None) and not self.connection.closed

    def get_connection(self, connect=False):
        if connect and not self.connection:
            self.connect()
        return self.connection

    def connect(self, reconnect=True):
        if self.is_connected() and reconnect:
            self.disconnect(True)
        if not self.is_connected():
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db,
                user=self.user,
                password=self.password,
                **self.conn_kwargs
            )
        return self.connection

    def disconnect(self, skip_errors=False, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if self.is_connected():
            if skip_errors:
                try:
                    self.connection.close()
                except psycopg2.OperationalError:
                    message = 'Connection to {} already closed.'.format(self.host)
                    self.log(message, level=log_progress.LoggingLevel.Warning, verbose=verbose)
            else:
                self.connection.close()
            self.connection = None
            return 1

    def execute(self, query=TEST_QUERY, get_data=AUTO, commit=AUTO, data=None, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        message = verbose if isinstance(verbose, str) else 'Execute:'
        level = log_progress.LoggingLevel.Debug
        self.log([message, ms.remove_extra_spaces(query)], level=level, end='\r', verbose=verbose)
        if get_data == AUTO:
            if 'SELECT' in query and 'GRANT' not in query:
                get_data, commit = True, False
            else:
                get_data, commit = False, True
        has_connection = self.is_connected()
        cur = self.connect(reconnect=False).cursor()
        if data:
            cur.execute(query, data)
        else:
            cur.execute(query)
        if get_data:
            result = cur.fetchall()
        if commit:
            self.get_connection().commit()
        cur.close()
        if not has_connection:
            self.connection.close()
        self.log([message, 'successful'], end='\r', verbose=bool(verbose))
        if get_data:
            return result

    def execute_batch(self, query, batch, step=DEFAULT_STEP, cursor=AUTO):
        if cursor == AUTO:
            cursor = self.connect().cursor()
        psycopg2.extras.execute_batch(cursor, query, batch, page_size=step)

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

    def exists_table(self, name, verbose=arg.DEFAULT):
        schema, table = name.split('.')
        query = """
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = '{schema}'
            AND    c.relname = '{table}'
            AND    c.relkind = 'r'
        """.format(schema=schema, table=table)
        return bool(self.execute(query, verbose))

    def insert_rows(
            self,
            table, rows, columns,
            step=DEFAULT_STEP, skip_errors=False,
            expected_count=None, return_count=True,
            verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
        conn = self.connect(reconnect=True)
        cur = conn.cursor()
        use_fast_batch_method = not skip_errors
        query_args = dict(table=table)
        if use_fast_batch_method:
            query_template = 'INSERT INTO {table} VALUES ({values});'
            placeholders = ['%({})s'.format(c) for c in columns]
        elif skip_errors:
            query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'
            placeholders = ['%s' for _ in columns]
            query_args['columns'] = ', '.join(columns)
        query_args['values'] = ', '.join(placeholders)
        query = query_template.format(**query_args)
        message = verbose if isinstance(verbose, str) else 'Committing {}-rows batches into {}'.format(step, table)
        progress = log_progress.Progress(
            message, count=count, verbose=verbose, logger=self.get_logger(), context=self.context,
        )
        progress.start()
        records_batch = list()
        n = 0
        for n, row in enumerate(rows):
            if use_fast_batch_method:
                current_record = {k: v for k, v in zip(columns, row)}
                records_batch.append(current_record)
            elif skip_errors:
                try:
                    cur.execute(query, row)
                except TypeError or IndexError as e:  # TypeError: not all arguments converted during string formatting
                    self.log(['Error line:', str(row)], level=log_progress.LoggingLevel.Debug, verbose=verbose)
                    self.log([e.__class__.__name__, e], level=log_progress.LoggingLevel.Error)
            if (n + 1) % step == 0:
                if use_fast_batch_method:
                    self.execute_batch(query, records_batch, step, cursor=cur)
                    records_batch = list()
                if not progress.position:
                    progress.update(0)
                conn.commit()
                progress.update(n)
                gc.collect()
        if use_fast_batch_method:
            self.execute_batch(query, records_batch, step, cursor=cur)
        conn.commit()
        progress.finish(n)
        if return_count:
            return n

    def get_dialect_name(self):
        return DatabaseType.PostgresDatabase.value


class ClickhouseDatabase(AbstractDatabase):
    def __init__(
            self,
            name,
            host='localhost',
            port=8443,
            db='public',
            user=arg.DEFAULT,
            password=arg.DEFAULT,
            context=None,
            **kwargs
    ):
        super().__init__(
            name=name,
            host=host,
            port=port,
            db=db,
            user=user,
            password=password,
            context=context,
            **kwargs
        )

    def execute(self, query=TEST_QUERY, get_data=AUTO, commit=AUTO, verbose=True):
        url = 'https://{host}:{port}/?database={db}&query={query}'.format(
            host=self.host,
            port=self.port,
            db=self.db,
            query=query,
        )
        auth = {
            'X-ClickHouse-User': self.user,
            'X-ClickHouse-Key': self.password,
        }
        request_props = {'headers': auth}
        cert_filename = self.conn_kwargs.get('cert_filename') or self.conn_kwargs.get('verify')
        if cert_filename:
            request_props['verify'] = cert_filename
        self.log('Execute query: {}'. format(query), verbose=verbose)
        res = requests.get(
            url,
            **request_props
        )
        res.raise_for_status()
        if get_data:
            return res.text

    def exists_table(self, name, verbose=arg.DEFAULT):
        query = 'EXISTS TABLE {}'.format(name)
        answer = self.execute(query, verbose)
        return answer[0] == '1'

    def insert_rows(
            self, table, rows, columns,
            step=DEFAULT_STEP, skip_errors=False,
            expected_count=None, return_count=True,
            verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
        if count == 0:
            message = 'Rows are empty, nothing to insert into {}.'.format(table)
            if skip_errors:
                self.log(message, verbose=verbose)
            else:
                raise ValueError(message)
        query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=table,
            columns=', '.join(columns),
            values='{}',
        )
        message = verbose if isinstance(verbose, str) else 'Inserting into {table}'.format(table=table)
        progress = log_progress.Progress(
            message, count=count, verbose=verbose, logger=self.get_logger(), context=self.context,
        )
        progress.start()
        n = 0
        for n, row in enumerate(rows):
            values = ', '.format(row)
            cur_query = query_template.format(values)
            if skip_errors:
                try:
                    self.execute(cur_query)
                except requests.RequestException as e:
                    self.log(['Error line:', str(row)], level=log_progress.LoggingLevel.Debug, verbose=verbose)
                    self.log([e.__class__.__name__, e], level=log_progress.LoggingLevel.Error)
            else:
                self.execute(cur_query)
            if (n + 1) % step == 0:
                progress.update(n)
        progress.finish(n)
        if return_count:
            return n

    def get_dialect_name(self):
        return DatabaseType.ClickhouseDatabase.value


class Table(ac.LeafConnector):
    def __init__(
            self,
            name,
            schema,
            database,
            reconnect=True,
            **kwargs
    ):
        super().__init__(
            name=name,
            parent=database,
        )
        self.schema = schema
        if not isinstance(schema, sh.SchemaDescription):
            message = 'Schema as {} is deprecated. Use schema.SchemaDescription instead.'.format(type(schema))
            self.log(msg=message, level=log_progress.LoggingLevel.Warning)
        self.meta = kwargs
        if reconnect:
            if hasattr(self.get_database(), 'connect'):
                self.get_database().connect(reconnect=True)
        self.links = list()

    def get_database(self):
        return self.parent

    def get_count(self, verbose=arg.DEFAULT):
        return self.database.select_count(self.name, verbose=verbose)

    def get_data(self, verbose=arg.DEFAULT):
        return self.database.select_all(self.name, verbose=verbose)

    def get_flux(self):
        count = self.get_count()
        flux = fx.RowsFlux(
            self.get_data(),
            count=count,
            # source=self,
            context=self.get_context(),
        )
        self.links.append(flux)
        return flux

    def set_schema(self, schema):
        if schema is None:
            self.schema = None
        elif isinstance(schema, sh.SchemaDescription):
            self.schema = schema
        elif isinstance(schema, (list, tuple)):
            if max([isinstance(f, (list, tuple)) for f in schema]):
                self.schema = sh.SchemaDescription(schema)
            else:
                self.schema = sh.detect_schema_by_title_row(schema)
        elif schema == AUTO:
            if self.first_line_is_title:
                self.schema = self.detect_schema_by_title_row()
            else:
                self.schema = None
        else:
            message = 'schema must be SchemaDescription or tuple with fields_description (got {})'.format(type(schema))
            raise TypeError(message)

    def create(self, drop_if_exists, verbose=arg.DEFAULT):
        return self.database.create_table(
            self.name,
            schema=self.schema,
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self, data,
            encoding=None, skip_first_line=False,
            skip_lines=0, max_error_rate=0.0,
            verbose=arg.DEFAULT
    ):
        return self.database.safe_upload_table(
            self.name,
            data=data,
            schema=self.schema,
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )
