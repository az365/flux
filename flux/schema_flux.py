try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2
TYPE_CONV_FUNCS = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)


def is_row(row):
    return isinstance(row, (list, tuple))


def is_valid(row, schema):
    if is_row(row):
        if schema is not None:
            for value, description in zip(row, schema):
                field_type = description[TYPE_POS]
                if field_type in TYPE_CONV_FUNCS.values():
                    return isinstance(value, field_type)
                elif field_type == TYPE_CONV_FUNCS.keys():
                    selected_type = TYPE_CONV_FUNCS[field_type]
                    return isinstance(value, selected_type)
        else:
            return True


def check_rows(rows, schema, skip_errors=False):
    for r in rows:
        if is_valid(r, schema=schema):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not valid record for schema {}: {}'.format(schema, r))
        yield r


def get_cast_function(field_type):
    return TYPE_CONV_FUNCS[field_type]


def cast(value, field_type, default_int=0):
    cast_function = get_cast_function(field_type)
    if value in (None, 'None', '') and field_type in ('int', int):
        value = default_int
    return cast_function(value)


def apply_schema_to_row(row, schema, skip_bad_values=False, verbose=True):
    for c, (value, description) in enumerate(zip(row, schema)):
        field_type = description[TYPE_POS]
        try:
            new_value = cast(value, field_type)
        except ValueError as e:
            field_name = description[NAME_POS]
            if verbose:
                print(
                    'Error while casting field {} ({}) with value {} into type {}'.format(
                        field_name, c,
                        value, field_type,
                    )
                )
            if not skip_bad_values:
                if verbose:
                    print('Error in row:', str(list(zip(row, schema)))[:80], '...')
                raise e
        row[c] = new_value
    return row


class SchemaFlux(fx.RowsFlux):
    def __init__(self, items, count=None, check=True, schema=None):
        super().__init__(
            items=check_rows(items, schema) if check else items,
            count=count,
            check=check,
        )
        self.schema = schema or list()

    def meta(self):
        return dict(
            count=self.count(),
            check=self.check(),
            schema=self.schema(),
        )

    def is_valid_item(self, item):
        return is_valid(
            item,
            schema=self.schema,
        )

    def valid_items(self, items, skip_errors=False):
        return check_rows(
            items,
            self.schema,
            skip_errors,
        )

    def set_schema(self, schema, check=True):
        return SchemaFlux(
            items=check_rows(self.items, schema=schema) if check else self.items,
            count=self.count,
            schema=schema,
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        def apply_schema_to_rows(rows):
            for r in rows:
                if skip_bad_rows:
                    try:
                        yield apply_schema_to_row(r, schema)
                    except ValueError:
                        if verbose:
                            print('Skip bad row:', str(r)[:80], '...')
                else:
                    yield apply_schema_to_row(r, schema, skip_bad_values=skip_bad_values, verbose=verbose)
        return SchemaFlux(
            apply_schema_to_rows(self.items),
            count=None if skip_bad_rows else self.count,
            check=False,
            schema=schema,
        )
