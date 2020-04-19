try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import selection
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import selection


def is_row(row):
    return isinstance(row, (list, tuple))


def check_rows(rows, skip_errors=False):
    for i in rows:
        if is_row(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not row: {}'.format(i))
        yield i


class RowsFlux(fx.AnyFlux):
    def __init__(
            self,
            items,
            count=None,
            check=True,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
    ):
        super().__init__(
            items=check_rows(items) if check else items,
            count=count,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def is_valid_item(item):
        return is_row(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_rows(items, skip_errors)

    def select(self, *columns):
        return self.native_map(
            lambda r: selection.get_columns(r, *columns),
        )

    def to_records(self, function=None, columns=[]):
        def get_records(rows, cols):
            for r in rows:
                yield {k: v for k, v in zip(cols, r)}
        if function:
            records = map(function, self.items)
        elif columns:
            records = get_records(self.items, columns)
        else:
            records = map(lambda r: dict(row=r), self.items)
        return fx.RecordsFlux(
            records,
            **self.get_meta()
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        return fx.SchemaFlux(
            self.items,
            **self.get_meta(),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    def to_lines(self, delimiter='\t'):
        return fx.LinesFlux(
            map(lambda r: '\t'.join([str(c) for c in r]), self.items),
            count=self.count,
        )
