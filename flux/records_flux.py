import pandas as pd

try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


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


def topologically_sorted(selectors):
    ordered_fields = list()
    unordered_fields = list()
    unresolved_dependencies = dict()
    for field, description in selectors.items():
        unordered_fields.append(field)
        _, dependencies = fx.process_selector_description(description)
        unresolved_dependencies[field] = [d for d in dependencies if d in selectors.keys() and d != field]
    while unordered_fields:  # Kahn's algorithm
        for field in unordered_fields:
            if not unresolved_dependencies[field]:
                ordered_fields.append(field)
                unordered_fields.remove(field)
                for f in unordered_fields:
                    if field in unresolved_dependencies[f]:
                        unresolved_dependencies[f].remove(field)
    return [(f, selectors[f]) for f in ordered_fields]


def select_value(record, description):
    if callable(description):
        return description(record)
    elif isinstance(description, (list, tuple)):
        function, fields = fx.process_selector_description(description)
        values = [record.get(f) for f in fields]
        return function(*values)
    else:
        return record.get(description)


def select_fields(rec_in, *descriptions):
    record = rec_in.copy()
    fields_out = list()
    for desc in descriptions:
        if desc == '*':
            fields_out += list(rec_in.keys())
        elif isinstance(desc, (list, tuple)):
            if len(desc) > 1:
                f_out = desc[0]
                fs_in = desc[1] if len(desc) == 2 else desc[1:]
                record[f_out] = select_value(record, fs_in)
                fields_out.append(f_out)
            else:
                raise ValueError('incorrect selector: {}'.format(desc))
        else:
            if desc not in record:
                record[desc] = None
            fields_out.append(desc)
    return {f: record[f] for f in fields_out}


def get_key_function(descriptions, take_hash=False):
    if len(descriptions) == 0:
        raise ValueError('key must be defined')
    elif len(descriptions) == 1:
        key_function = lambda r: select_value(r, descriptions[0])
    else:
        key_function = lambda r: tuple([select_value(r, d) for d in descriptions])
    if take_hash:
        return lambda r: hash(key_function(r))
    else:
        return key_function


class RecordsFlux(fx.AnyFlux):
    def __init__(self, items, count=None, check=True):
        super().__init__(
            items=check_records(items) if check else items,
            count=count,
        )
        self.check = check

    def meta(self):
        return dict(
            count=self.count,
            check=self.check,
        )

    @staticmethod
    def is_valid_item(item):
        return is_record(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_records(items, skip_errors)

    def get_records(self, skip_errors=False, raise_errors=True):
        if skip_errors or raise_errors:
            return check_records(self.items, skip_errors)
        else:
            return self.items

    def enumerated_records(self, field='#', first=1):
        for n, r in enumerate(self.items):
            r[field] = n + first
            yield r

    def enumerate(self, native=False):
        props = self.meta()
        if native:
            target_class = self.__class__
            enumerated = self.enumerated_records()
        else:
            target_class = fx.PairsFlux
            enumerated = self.enumerated_items()
            props['secondary'] = fx.FluxType(self.class_name())
        return target_class(
            items=enumerated,
            **props
        )

    def select(self, *fields, **selectors):
        descriptions = list(fields)
        for k, v in topologically_sorted(selectors):
            if isinstance(v, (list, tuple)):
                descriptions.append([k] + list(v)),
            else:
                descriptions.append([k] + [v])
        return self.native_map(
            lambda r: select_fields(r, *descriptions),
        )

    def filter(self, *fields):
        def filter_function(r):
            for f in fields:
                if not select_value(r, f):
                    return False
            return True
        props = self.meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.items)
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
            step=fx.MAX_ITEMS_IN_MEMORY, tmp_file_template='merge_sort_{}', encoding='utf8',
            verbose=True,
    ):
        key_function = get_key_function(keys)
        if self.is_in_memory() or (step is None) or (self.count is not None and self.count <= step):
            return self.memory_sort(key_function, reverse)
        else:
            return self.disk_sort(key_function, reverse, step, tmp_file_template, encoding, verbose)

    def sorted_group_by(self, *keys, as_pairs=True):
        keys = fx.update_arg(keys)

        def get_groups():
            key_function = get_key_function(keys)
            accumulated = list()
            prev_k = None
            for r in self.items:
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

    def group_by(self, *keys, step=None, as_pairs=True, verbose=True):
        keys = fx.update_arg(keys)
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
        dataframe = pd.DataFrame(self.items)
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
        columns = fx.update_arg(columns, kwargs.pop('columns', None))
        if kwargs:
            raise AttributeError('to_rows(): {} arguments are not supported'.format(kwargs.keys()))

        def get_rows(columns_list):
            if add_title_row:
                yield columns_list
            for r in self.items:
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
        return fx.SchemaFlux(
            self.items,
            **self.meta()
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    def to_pairs(self, key, value=None):
        def get_pairs():
            for i in self.items:
                k = select_value(i, key)
                v = i if value is None else select_value(i, value)
                yield k, v
        return fx.PairsFlux(
            list(get_pairs()) if self.is_in_memory() else get_pairs(),
            count=self.count,
            secondary=fx.FluxType.RecordsFlux if value is None else fx.FluxType.AnyFlux,
            check=False,
        )

    def get_dict(self, key, value=None, of_lists=False):
        return self.to_pairs(
            key,
            value,
        ).get_dict(
            of_lists,
        )
