try:  # Assume we're a sub-module in a package.
    import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx


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


def select_value(row, description):
    if callable(description):
        return description(row)
    elif isinstance(description, (list, tuple)):
        function, columns = fx.process_selector_description(description)
        values = [row[f] for f in columns]
        return function(*values)
    elif isinstance(description, int):
        return row[description]
    else:
        raise TypeError('selector description must be int, callable or tuple ({} as {} given)'.format(
            description, type(description)
        ))


def select_columns(row_in, *columns):
    row_out = [None] * len(columns)
    c = 0
    for d in columns:
        if d == '*':
            row_out = row_out[:c] + list(row_in) + row_out[c + 1:]
            c += len(row_in)
        else:
            row_out[c] = select_value(row_in, d)
            c += 1
    return tuple(row_out)


class RowsFlux(fx.AnyFlux):
    def __init__(self, items, count=None, check=True):
        super().__init__(
            items=check_rows(items) if check else items,
            count=count,
        )
        self.check = check

    def get_meta(self):
        return dict(
            count=self.count,
            check=self.check,
        )

    @staticmethod
    def is_valid_item(item):
        return is_row(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_rows(items, skip_errors)

    def select(self, *columns):
        return self.native_map(
            lambda r: select_columns(r, *columns),
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
