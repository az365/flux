from abc import ABC
from enum import Enum
import gzip as gz
import csv

try:  # Assume we're a sub-module in a package.
    import context as fc
    import fluxes as fx
    import conns as cs
    from utils import (
        arguments as arg,
        functions as fs,
        schema as sh,
        selection,
        log_progress,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import context as fc
    from .. import fluxes as fx
    from .. import conns as cs
    from ..utils import (
        arguments as arg,
        functions as fs,
        schema as sh,
        selection,
        log_progress,
    )


AUTO = arg.DEFAULT
CHUNK_SIZE = 8192


class FileType(Enum):
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'


class LocalFolder:
    def __init__(
            self,
            path,
            context=None,
            verbose=True,
    ):
        self.path = path
        self.files = dict()
        self.context = context
        self.verbose = verbose

    def get_context(self):
        return self.context

    def get_logger(self):
        if self.get_context():
            return self.get_context().get_logger()
        else:
            return log_progress.get_logger()

    def log(self, msg, level=AUTO, end=AUTO, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_path(self):
        return self.path

    def file(self, name, filetype=None, **kwargs):
        file = self.files.get(name)
        if file:
            assert not kwargs, 'file connection {} is already registered'.format(name)
        else:
            file_class = cs.get_class(filetype)
            file = file_class(name, folder=self, **kwargs)
            self.files[name] = file
        return file

    def add_file(self, name, file):
        assert cs.is_file(file), 'file must be an instance of *File (got {})'.format(type(file))
        assert name not in self.files, 'file with name {} is already registered'.format(name)
        self.files[name] = file

    def get_items(self):
        return self.files

    def get_links(self):
        for item in self.files:
            yield from item.get_links()

    def close(self, name=None):
        closed_count = 0
        if name:
            file = self.files.get(name)
            if file:
                closed_count += file.close()
        else:
            for file in self.files.values():
                closed_count += file.close()
        return closed_count

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('files')
        return meta

class AbstractFile(ABC):
    def __init__(
            self,
            filename,
            folder=None,
            context=None,
            verbose=AUTO
    ):
        self.filename = filename
        self.fileholder = None
        if folder:
            message = 'only LocalFolder supported for *File instances (got {})'.format(type(folder))
            assert isinstance(folder, LocalFolder), message
            self.folder = folder
        elif context:
            assert isinstance(context, fc.FluxContext)
            self.folder = context.get_job_folder()
            self.folder.add_file(filename, self)
        else:
            self.folder = None
        self.verbose = arg.undefault(verbose, self.folder.verbose if self.folder else True)
        self.links = list()

    def get_context(self):
        if self.folder:
            return self.folder.get_context()

    def get_logger(self):
        if self.folder:
            return self.folder.get_logger()
        else:
            return log_progress.get_logger()

    def log(self, msg, level=AUTO, end=AUTO, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_links(self):
        return self.links

    def add_to_folder(self, folder, name=arg.DEFAULT):
        assert isinstance(folder, LocalFolder), 'Folder must be a LocalFolder (got {})'.format(type(folder))
        name = arg.undefault(name, self.filename)
        folder.add_file(self, name)

    @staticmethod
    def get_flux_type():
        return fx.FluxType.AnyFlux

    @classmethod
    def get_flux_class(cls):
        return fx.get_class(cls.get_flux_type())

    def is_directly_in_parent_folder(self):
        return '/' in self.filename

    def has_path_from_root(self):
        return self.filename.startswith('/') or ':' in self.filename

    def get_path(self):
        if self.has_path_from_root() or not self.folder:
            return self.filename
        else:
            folder_path = self.folder.get_path()
            if '{}' in folder_path:
                return folder_path.format(self.filename)
            elif folder_path.endswith('/'):
                return folder_path + self.filename
            elif folder_path:
                return '{}/{}'.format(folder_path, self.filename)
            else:
                return self.filename

    def get_list_path(self):
        return self.get_path().split('/')

    def get_folder_path(self):
        return '/'.join(self.get_list_path()[:-1])

    def is_inside_folder(self, folder=AUTO):
        folder_obj = arg.undefault(folder, self.folder)
        folder_path = folder_obj.get_path() if isinstance(folder_obj, LocalFolder) else folder_obj
        return self.get_folder_path() in folder_path

    def get_name(self):
        return self.get_list_path()[-1]

    def is_opened(self):
        return self.fileholder is not None

    def close(self):
        if self.is_opened():
            self.fileholder.close()
            return 1

    def open(self, mode='r', reopen=False):
        if self.is_opened():
            if reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.filename))
        else:
            self.fileholder = open(self.filename, 'r')

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('fileholder')
        return meta


class TextFile(AbstractFile):
    def __init__(
            self,
            filename,
            gzip=False,
            encoding='utf8',
            end='\n',
            expected_count=AUTO,
            folder=None,
            context=None,
            verbose=AUTO,
    ):
        super().__init__(
            filename=filename,
            folder=folder,
            context=context,
            verbose=verbose
        )
        self.gzip = gzip
        self.encoding = encoding
        self.end = end
        self.count = expected_count
        self.fileholder = None

    def open(self, mode='r', reopen=False):
        if self.is_opened():
            if reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.filename))
        if self.gzip:
            self.fileholder = gz.open(self.filename, mode)
        else:
            params = dict()
            if self.encoding:
                params['encoding'] = self.encoding
            self.fileholder = open(self.filename, mode, **params) if self.encoding else open(self.filename, 'r')

    def count_lines(self, reopen=False, chunk_size=CHUNK_SIZE, verbose=AUTO):
        verbose = arg.undefault(verbose, self.verbose)
        self.log('Counting lines in {}...'.format(self.filename), end='\r', verbose=verbose)
        self.open(reopen=reopen)
        count_n = sum(chunk.count('\n') for chunk in iter(lambda: self.fileholder.read(chunk_size), ''))
        self.count = count_n + 1
        self.close()
        self.log('Detected {} lines in {}.'.format(self.count, self.filename), end='\r', verbose=verbose)
        return self.count

    def get_count(self, reopen=False):
        if (self.count is None or self.count == AUTO) and not self.gzip:
            self.count = self.count_lines(reopen=reopen)
        return self.count

    def get_next_lines(self, count=None, skip_first=False, close=False):
        assert self.is_opened()
        for n, row in enumerate(self.fileholder):
            if skip_first and n == 0:
                continue
            if isinstance(row, bytes):
                row = row.decode(self.encoding) if self.encoding else row.decode()
            if self.end:
                row = row.rstrip(self.end)
            yield row
            if (count or 0) > 0 and (n + 1 == count):
                break
        if close:
            self.close()

    def get_lines(self, count=None, skip_first=False, check=True, verbose=AUTO, step=AUTO):
        if check and not self.gzip:
            assert self.get_count(reopen=True) > 0
        self.open(reopen=True)
        lines = self.get_next_lines(count=count, skip_first=skip_first, close=True)
        if arg.undefault(verbose, self.verbose):
            message = 'Reading {}'.format(self.get_name())
            lines = self.get_logger().progress(lines, name=message, count=self.count, step=step)
        return lines

    def get_items(self, verbose=AUTO, step=AUTO):
        verbose = arg.undefault(verbose, self.verbose)
        if (self.get_count() or 0) > 0:
            self.log('Expecting {} lines in file {}...'.format(self.get_count(), self.get_name()), verbose=verbose)
        return self.get_lines(verbose=verbose, step=step)

    @staticmethod
    def get_flux_type():
        return fx.FluxType.LinesFlux

    def get_flux(self, to=AUTO, verbose=AUTO):
        to = arg.undefault(to, self.get_flux_type())
        return self.to_flux_class(
            flux_class=fx.get_class(to),
            verbose=verbose,
        )

    def flux_kwargs(self, verbose=AUTO, step=AUTO, **kwargs):
        verbose = arg.undefault(verbose, self.verbose)
        result = dict(
            count=self.get_count(),
            data=self.get_items(verbose=verbose, step=step),
            source=self,
            context=self.get_context(),
        )
        result.update(kwargs)
        return result

    def to_flux_class(self, flux_class, **kwargs):
        return flux_class(
            **self.flux_kwargs(**kwargs)
        )

    def to_lines_flux(self, **kwargs):
        return fx.LinesFlux(
            **self.flux_kwargs(**kwargs)
        )

    def to_any_flux(self, **kwargs):
        return fx.AnyFlux(
            **self.flux_kwargs(**kwargs)
        )


class JsonFile(TextFile):
    def __init__(
            self,
            filename,
            encoding='utf8',
            gzip=False,
            expected_count=AUTO,
            schema=AUTO,
            default_value=None,
            folder=None,
            context=None,
            verbose=AUTO,
    ):
        super().__init__(
            filename=filename,
            encoding=encoding,
            gzip=gzip,
            expected_count=expected_count,
            folder=folder,
            context=context,
            verbose=verbose,
        )
        self.schema = schema
        self.default_value = default_value

    @staticmethod
    def get_flux_type():
        return fx.FluxType.AnyFlux

    def get_items(self, verbose=AUTO, step=AUTO):
        return self.to_lines_flux(
            verbose=verbose
        ).parse_json(
            default_value=self.default_value
        ).get_items()

    def to_records_flux(self, verbose=AUTO):
        return fx.RecordsFlux(
            self.get_items(verbose=verbose),
            count=self.count,
            context=self.get_context(),
        )


class CsvFile(TextFile):
    def __init__(
            self,
            filename,
            gzip=False,
            encoding='utf8',
            end='\n',
            delimiter=',',
            first_line_is_title=True,
            expected_count=AUTO,
            schema=AUTO,
            folder=None,
            context=None,
            verbose=AUTO
    ):
        super().__init__(
            filename=filename,
            gzip=gzip,
            encoding=encoding,
            end=end,
            expected_count=expected_count,
            folder=folder,
            context=context,
            verbose=verbose,
        )
        self.delimiter = delimiter
        self.first_line_is_title = first_line_is_title
        self.schema = None
        self.set_schema(schema)

    def get_schema(self):
        return self.schema

    def set_schema(self, schema):
        if schema is None:
            self.schema = None
        elif isinstance(schema, sh.SchemaDescription):
            self.schema = schema
        elif isinstance(schema, (list, tuple)):
            self.schema = sh.SchemaDescription(schema)
        elif schema == AUTO:
            self.schema = None
        else:
            message = 'schema must be SchemaDescription of tuple with fields_description (got {})'.format(type(schema))
            raise TypeError(message)

    def get_rows(self):
        for item in self.get_items():
            row = csv.reader(item)
            if self.schema is not None:
                assert isinstance(self.schema, sh.SchemaDescription)
                row = sh.SchemaRow(row, self.schema).data
            yield row

    def get_schema_rows(self):
        assert self.schema is not None, 'For getting schematized rows schema must be defined.'
        for row in self.get_rows():
            yield sh.SchemaRow(row, schema=self.schema)

    def get_records(self):
        for item in self.get_schema_rows():
            yield item.get_record()


class TsvFile(CsvFile):
    def __init__(
            self,
            filename,
            gzip=False,
            encoding='utf8',
            end='\n',
            delimiter='\t',
            first_line_is_title=True,
            expected_count=AUTO,
            schema=AUTO,
            folder=None,
            context=None,
            verbose=AUTO
    ):
        super().__init__(
            filename=filename,
            gzip=gzip,
            encoding=encoding,
            end=end,
            delimiter=delimiter,
            first_line_is_title=first_line_is_title,
            expected_count=expected_count,
            schema=schema,
            folder=folder,
            context=context,
            verbose=verbose,
        )
