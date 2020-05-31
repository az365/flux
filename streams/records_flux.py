import pandas as pd

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    import conns as cs
    from utils import (
        arguments as arg,
        functions as fs,
        selection,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from .. import conns as cs
    from ..utils import (
        arguments as arg,
        functions as fs,
        selection,
    )


def is_record(item):
    return isinstance(item, dict)


def check_records(records, skip_errors=False):
    for r in records:
        if is_record(r):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not record: {}'.format(r))
        yield r


def get_key_function(descriptions, take_hash=False):
    if len(descriptions) == 0:
        raise ValueError('key must be defined')
    elif len(descriptions) == 1:
        key_function = fs.partial(selection.value_from_record, descriptions[0])
    else:
        key_function = fs.partial(selection.tuple_from_record, descriptions)
    if take_hash:
        return lambda r: hash(key_function(r))
    else:
        return key_function


class RecordsFlux(fx.AnyFlux):
    def __init__(
            self,
            data,
            count=None,
            check=True,
            source=None,
            context=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
    ):
        super().__init__(
            check_records(data) if check else data,
            count=count,
            source=source,
            context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def is_valid_item(item):
        return is_record(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_records(items, skip_errors)

    def get_records(self, skip_errors=False, raise_errors=True):
        if skip_errors or raise_errors:
            return check_records(self.data, skip_errors)
        else:
            return self.data

    def enumerated_records(self, field='#', first=1):
        for n, r in enumerate(self.data):
            r[field] = n + first
            yield r

    def enumerate(self, native=False):
        props = self.get_meta()
        if native:
            target_class = self.__class__
            enumerated = self.enumerated_records()
        else:
            target_class = fx.PairsFlux
            enumerated = self.enumerated_items()
            props['secondary'] = fx.FluxType(self.class_name())
        return target_class(
            enumerated,
            **props
        )

    def select(self, *fields, **expressions):
        logger_for_cyclic_dependencies = self.get_logger()
        descriptions = selection.flatten_descriptions(
            *fields,
            logger=logger_for_cyclic_dependencies,
            **expressions
        )
        return self.native_map(
            lambda r: selection.record_from_record(r, *descriptions),
        )

    def filter(self, *fields, **expressions):
        extended_filters_list = list(fields) + list(expressions.items())

        def filter_function(r):
            for f in extended_filters_list:
                if not selection.value_from_record(r, f):
                    return False
            return True
        props = self.get_meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.get_items())
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        return self.__class__(
            filtered_items,
            **props
        )

    def sort(
            self,
            *keys,
            reverse=False,
            step=arg.DEFAULT,
            verbose=True,
    ):
        key_function = get_key_function(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if self.is_in_memory() or (step is None) or (self.count is not None and self.count <= step):
            return self.memory_sort(key_function, reverse, verbose=verbose)
        else:
            return self.disk_sort(key_function, reverse, step=step, verbose=verbose)

    def sorted_group_by(self, *keys, as_pairs=True):
        keys = arg.update(keys)

        def get_groups():
            key_function = get_key_function(keys)
            accumulated = list()
            prev_k = None
            for r in self.get_items():
                k = key_function(r)
                if (k != prev_k) and accumulated:
                    yield (prev_k, accumulated) if as_pairs else accumulated
                    accumulated = list()
                prev_k = k
                accumulated.append(r)
            yield (prev_k, accumulated) if as_pairs else accumulated
        if as_pairs:
            fx_groups = fx.PairsFlux(
                get_groups(),
                secondary=fx.FluxType.RowsFlux,
            )
        else:
            fx_groups = fx.RowsFlux(
                get_groups(),
                check=False,
            )
        return fx_groups.to_memory() if self.is_in_memory() else fx_groups

    def group_by(self, *keys, step=arg.DEFAULT, as_pairs=True, verbose=True):
        keys = arg.update(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if not as_pairs:
            keys = [
                get_key_function(keys, take_hash=True),
            ]
        sorted_fx = self.sort(
            *keys,
            step=step,
            verbose=verbose,
        )
        grouped_fx = sorted_fx.sorted_group_by(
            keys,
            as_pairs=as_pairs
        )
        return grouped_fx

    def get_dataframe(self, columns=None):
        dataframe = pd.DataFrame(self.data)
        if columns:
            dataframe = dataframe[columns]
        return dataframe

    def to_lines(self, columns, add_title_row=False, delimiter='\t'):
        return fx.LinesFlux(
            self.to_rows(columns, add_title_row=add_title_row),
            self.count,
        ).map(
            delimiter.join,
        )

    def to_rows(self, *columns, **kwargs):
        add_title_row = kwargs.pop('add_title_row', None)
        columns = arg.update(columns, kwargs.pop('columns', None))
        if kwargs:
            raise ValueError('to_rows(): {} arguments are not supported'.format(kwargs.keys()))

        def get_rows(columns_list):
            if add_title_row:
                yield columns_list
            for r in self.get_items():
                yield [r.get(f) for f in columns_list]
        if self.count is None:
            count = None
        else:
            count = self.count + (1 if add_title_row else 0)
        return fx.RowsFlux(
            get_rows(list(columns)),
            count,
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        return self.to_rows(
            columns=schema.get_columns(),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    def to_pairs(self, key, value=None):
        def get_pairs():
            for i in self.get_items():
                k = selection.value_from_record(i, key)
                v = i if value is None else selection.value_from_record(i, value)
                yield k, v
        return fx.PairsFlux(
            list(get_pairs()) if self.is_in_memory() else get_pairs(),
            count=self.count,
            secondary=fx.FluxType.RecordsFlux if value is None else fx.FluxType.AnyFlux,
            check=False,
        )

    def to_tsv_file(
            self,
            file,
            verbose=True,
            return_flux=True,
    ):
        assert cs.is_file(file)
        meta = self.get_meta()
        if not file.gzip:
            meta.pop('count')
        file.write_flux(self, verbose=verbose)
        if return_flux:
            return file.to_records_flux(verbose=verbose).update_meta(**meta)

    def to_csv_file(
            self,
            filename,
            columns,
            delimiter='\t',
            add_title_row=True,
            encoding=arg.DEFAULT,
            gzip=False,
            check=True,
            verbose=True,
            return_flux=True,
    ):
        encoding = arg.undefault(encoding, self.tmp_files_encoding)
        meta = self.get_meta()
        if not gzip:
            meta.pop('count')
        fx_csv_file = self.to_rows(
            columns=columns,
            add_title_row=add_title_row,
        ).to_lines(
            delimiter=delimiter,
        ).to_file(
            filename,
            encoding=encoding,
            gzip=gzip,
            check=check,
            verbose=verbose,
            return_flux=return_flux,
        )
        if return_flux:
            return fx_csv_file.skip(
                1 if add_title_row else 0,
            ).to_rows(
                delimiter=delimiter,
            ).to_records(
                columns=columns,
            ).update_meta(
                **meta
            )

    @classmethod
    def from_csv_file(
            cls,
            filename,
            columns,
            delimiter='\t',
            skip_first_line=True,
            encoding=arg.DEFAULT,
            gzip=False,
            check=arg.DEFAULT,
            expected_count=arg.DEFAULT,
            verbose=True,
    ):
        encoding = arg.undefault(encoding, fx.TMP_FILES_ENCODING)
        return fx.LinesFlux.from_file(
            filename,
            skip_first_line=skip_first_line,
            encoding=encoding,
            gzip=gzip,
            check=check,
            expected_count=expected_count,
            verbose=verbose,
        ).to_rows(
            delimiter=delimiter,
        ).to_records(
            columns=columns,
        )

    @classmethod
    def from_json_file(
            cls,
            filename,
            encoding=None,
            gzip=False,
            default_value=None,
            max_count=None,
            check=True,
            verbose=False,
    ):
        parsed_flux = fx.LinesFlux.from_file(
            filename,
            encoding=encoding,
            gzip=gzip,
            max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            default_value=default_value,
            to=fx.FluxType.RecordsFlux,
        )
        return parsed_flux

    def get_dict(self, key, value=None, of_lists=False):
        return self.to_pairs(
            key,
            value,
        ).get_dict(
            of_lists,
        )
