import sys
import json
import csv
import gzip as gz


try:
    import fluxes as fx
    from utils import (
        functions as fs,
        arguments as arg,
        readers
    )
except ImportError:
    from .. import fluxes as fx
    from ..utils import (
        functions as fs,
        arguments as arg,
        readers
    )

max_int = sys.maxsize
while True:  # To prevent _csv.Error: field larger than field limit (131072)
    try:  # decrease the max_int value by factor 10 as long as the OverflowError occurs.
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


def is_line(line):
    return isinstance(line, str)


def check_lines(lines, skip_errors=False):
    for i in lines:
        if is_line(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_lines(): this item is not a line: {}'.format(i))
        yield i


class LinesFlux(fx.AnyFlux):
    def __init__(
            self,
            items,
            count=None,
            check=True,
            source=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
            context=None,
    ):
        super().__init__(
            check_lines(items) if check else items,
            count=count,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
            context=context,
        )
        self.check = check
        self.source = source

    @staticmethod
    def is_valid_item(item):
        return is_line(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_lines(items, skip_errors)

    def parse_json(self, default_value=None, to='RecordsFlux'):
        if isinstance(to, str):
            to = fx.FluxType(to)

        def json_loads(line):
            try:
                return json.loads(line)
            except json.JSONDecodeError as err:
                if default_value is not None:
                    return default_value
                else:
                    raise json.JSONDecodeError(err.msg, err.doc, err.pos)
        return self.map(
            json_loads,
            to=to,
        ).set_meta(
            count=self.count,
        )

    @classmethod
    def from_file(
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_n=None,
            check=arg.DEFAULT,
            verbose=False, step=readers.VERBOSE_STEP,
    ):
        fx_lines = readers.from_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_n=max_n,
            check=check,
            verbose=verbose, step=step,
        )
        is_inherited = fx_lines.flux_type() != cls.__name__
        if is_inherited:
            fx_lines = fx_lines.map(function=fs.same(), to=cls.__name__)
        return fx_lines

    def lazy_save(
            self,
            filename,
            encoding=None, gzip=False,
            end='\n', check=arg.DEFAULT,
            verbose=True, immediately=False,
    ):
        def write_and_yield(fh, lines):
            n = 0
            for n, i in enumerate(lines):
                if n > 0:
                    fh.write(end.encode(encoding) if gzip else end)
                fh.write(str(i).encode(encoding) if gzip else str(i))
                yield i
            fh.close()
            if verbose:
                print('Done. {} rows has written into {}'.format(n + 1, filename))
        if immediately:
            self.to_file(
                filename,
                encoding=encoding,
                end=end,
                check=check,
                verbose=verbose,
                return_flux=True,
            )
        else:
            if gzip:
                fileholder = gz.open(filename, 'w')
            else:
                fileholder = open(filename, 'w', encoding=encoding) if encoding else open(filename, 'w')
            return LinesFlux(
                write_and_yield(fileholder, self.items),
                **self.get_meta()
            )

    def to_file(
            self,
            filename,
            encoding=None, gzip=False,
            end='\n', check=arg.DEFAULT,
            verbose=True, return_flux=True
    ):
        saved_flux = self.lazy_save(
            filename,
            encoding=encoding,
            gzip=gzip,
            end=end,
            verbose=verbose,
            immediately=False,
        )
        saved_flux.pass_items()
        meta = self.get_meta()
        meta.pop('count')
        if return_flux:
            return readers.from_file(
                filename,
                encoding=encoding,
                gzip=gzip,
                check=check,
                verbose=verbose,
            ).update_meta(
                **meta
            )

    def to_rows(self, delimiter=None):
        lines = self.items
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return fx.RowsFlux(
            rows,
            self.count,
        )

    def to_pairs(self, delimiter=None):
        lines = self.items
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return fx.RowsFlux(
            rows,
            self.count,
        ).to_pairs()
