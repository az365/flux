try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import functions as fs


NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # schema fields


def is_row(row):
    return isinstance(row, (list, tuple))


def is_valid(row, schema):
    if is_row(row):
        if schema is not None:
            for value, description in zip(row, schema):
                field_type = description[TYPE_POS]
                if field_type in fs.DICT_CAST_TYPES.values():
                    return isinstance(value, field_type)
                elif field_type == fs.DICT_CAST_TYPES.keys():
                    selected_type = fs.DICT_CAST_TYPES[field_type]
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


def apply_schema_to_row(row, schema, skip_bad_values=False, verbose=True):
    for c, (value, description) in enumerate(zip(row, schema)):
        field_type = description[TYPE_POS]
        try:
            cast_function = fs.cast(field_type)
            new_value = cast_function(value)
        except ValueError as e:
            field_name = description[NAME_POS]
            if verbose:
                print(
                    'Error while casting field {} ({}) with value {} into type {}'.format(
                        field_name, c,
                        value, field_type,
                    )
                )
            if skip_bad_values:
                if verbose:
                    print('Error in row:', str(list(zip(row, schema)))[:80], '...')
                new_value = None
            else:
                print('Error in row:', str(list(zip(row, schema)))[:80], '...')
                raise e
        row[c] = new_value
    return row


class SchemaFlux(fx.RowsFlux):
    def __init__(
            self,
            items,
            count=None,
            check=True,
            schema=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
            context=None,
    ):
        super().__init__(
            items=check_rows(items, schema) if check else items,
            count=count,
            check=check,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
            context=context,
        )
        self.schema = schema or list()

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
