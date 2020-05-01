from itertools import chain, tee
from datetime import datetime
import inspect
import json

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import (
        arguments as arg,
        functions as fs,
        log_progress as log,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import (
        arguments as arg,
        functions as fs,
        log_progress as log,
    )


VERBOSE_STEP = 1000


def merge_iter(iterables, key_function, reverse=False):
    iterators_count = len(iterables)
    finished = [False] * iterators_count
    take_next = [True] * iterators_count
    item_from = [None] * iterators_count
    key_from = [None] * iterators_count
    choice_function = max if reverse else min
    while not min(finished):
        for n in range(iterators_count):
            if take_next[n] and not finished[n]:
                try:
                    item_from[n] = next(iterables[n])
                    key_from[n] = key_function(item_from[n])
                    take_next[n] = False
                except StopIteration:
                    finished[n] = True
        if not min(finished):
            chosen_key = choice_function([k for f, k in zip(finished, key_from) if not f])
            for n in range(iterators_count):
                if key_from[n] == chosen_key and not finished[n]:
                    yield item_from[n]
                    take_next[n] = True


class AnyFlux:
    def __init__(
            self,
            data,
            count=None,
            max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=fx.TMP_FILES_TEMPLATE,
            tmp_files_encoding=fx.TMP_FILES_ENCODING,
            context=None,
    ):
        self.data = data
        self.count = count
        self.max_items_in_memory = max_items_in_memory
        self.tmp_files_template = tmp_files_template
        self.tmp_files_encoding = tmp_files_encoding
        self.context = context

    def get_context(self):
        return self.context

    def get_logger(self):
        if self.context is not None:
            return self.context.get_logger()
        else:
            return log.get_logger()

    def get_items(self):
        return self.data

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('data')
        return meta

    def set_meta(self, **meta):
        return self.__class__(
            self.data,
            **meta
        )

    def update_meta(self, **meta):
        props = self.get_meta()
        props.update(meta)
        return self.__class__(
            self.data,
            **props
        )

    def fill_meta(self, check=True, **meta):
        props = self.get_meta()
        if check:
            unsupported = [k for k in meta if k not in props]
            assert not unsupported, 'class {} does not support these properties: {}'.format(
                self.flux_type(),
                unsupported,
            )
        for key, value in props.items():
            if value is None or value == arg.DEFAULT:
                props[key] = meta.get(key)
        return self.__class__(
            self.data,
            **props
        )

    def class_name(self):
        return self.__class__.__name__

    def flux_type(self):
        return fx.get_class(self.class_name())

    def get_class(self, other=None):
        if other is None:
            return self.__class__
        elif isinstance(other, (fx.FluxType, str)):
            return fx.get_class(fx.FluxType(other))
        elif inspect.isclass(other):
            return other
        else:
            raise TypeError('"other" parameter must be class or FluxType (got {})'.format(type(other)))

    def get_property(self, name, *args, **kwargs):
        if callable(name):
            value = name(self)
        elif isinstance(name, str):
            meta = self.get_meta()
            if name in meta:
                value = meta.get(name)
            else:
                try:
                    value = self.__getattribute__(name)(*args, **kwargs)
                except AttributeError:
                    value = None
        else:
            raise TypeError('property name must be function, meta-field or attribute name')
        return value

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        log.log(
            logger=self.get_logger(),
            msg=msg, level=level,
            end=end, verbose=verbose,
        )

    @classmethod
    def from_json_file(
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_n=None,
            check=arg.DEFAULT,
            verbose=False,
    ):
        parsed_flux = fx.LinesFlux.from_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_n,
            check=check,
            verbose=verbose,
        ).parse_json(
            to=cls.__name__,
        )
        return parsed_flux

    @staticmethod
    def is_valid_item(item):
        return True

    @staticmethod
    def valid_items(items, **kwargs):
        return items

    def validated(self, skip_errors=False):
        return self.__class__(
            self.valid_items(self.get_items(), skip_errors=skip_errors),
            **self.get_meta()
        )

    def iterable(self):
        yield from self.get_items()

    def next(self):
        return next(
            self.iterable(),
        )

    def one(self):
        for i in self.get_items():
            return i

    def expected_count(self):
        return self.count

    def final_count(self):
        result = 0
        for _ in self.get_items():
            result += 1
        return result

    def tee(self, n=2):
        return [
            self.__class__(
                i,
                count=self.count,
            ) for i in tee(
                self.get_items(),
                n,
            )
        ]

    def copy(self):
        if self.is_in_memory():
            copy_data = self.get_list().copy()
        else:
            self.data, copy_data = tee(self.iterable(), 2)
        return self.__class__(
            copy_data,
            **self.get_meta()
        )

    def calc(self, function):
        return function(self.data)

    def apply(self, function, native=True, save_count=False):
        if native:
            target_class = self.__class__
            props = self.get_meta()
            if not save_count:
                props.pop('count')
        else:
            target_class = AnyFlux
            props = dict(count=self.count) if save_count else dict()
        return target_class(
            self.calc(function),
            **props
        )

    def native_map(self, function):
        return self.__class__(
            map(function, self.get_items()),
            self.count,
        )

    def map_to_any(self, function):
        return AnyFlux(
            map(function, self.get_items()),
            self.count,
        )

    def map_to_records(self, function=None):
        def get_record(i):
            if function is None:
                return i if isinstance(i, dict) else dict(item=i)
            else:
                return function(i)
        return fx.RecordsFlux(
            map(get_record, self.get_items()),
            count=self.count,
            check=True,
        )

    def map(self, function=lambda i: i, to=None):
        fx_class = self.get_class(to)
        new_props_keys = fx_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        items = map(function, self.iterable())
        if self.is_in_memory():
            items = list(items)
        return fx_class(
            items,
            **props
        )

    def flat_map(self, function=lambda i: i, to=None):
        def get_items():
            for i in self.iterable():
                yield from function(i)
        fx_class = self.get_class(to)
        new_props_keys = fx_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        props.pop('count')
        return fx_class(
            get_items(),
            **props
        )

    def filter(self, *functions):
        def filter_function(item):
            for f in functions:
                if not f(item):
                    return False
            return True
        props = self.get_meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.iterable())
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        return self.__class__(
            filtered_items,
            **props
        )

    def enumerated_items(self):
        for n, i in enumerate(self.get_items()):
            yield n, i

    def enumerate(self, native=False):
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = fx.PairsFlux
            props['secondary'] = fx.FluxType(self.class_name())
        return target_class(
            self.enumerated_items(),
            **props
        )

    def progress(self, expected_count=arg.DEFAULT, step=VERBOSE_STEP, message='Progress'):
        count = arg.undefault(expected_count, self.count)
        return self.__class__(
            log.progress(self.data, name=message, count=count, step=step, logger=self.get_logger()),
            **self.get_meta()
        )

    def take(self, max_count=1):
        def take_items(m):
            for n, i in self.enumerated_items():
                yield i
                if n + 1 >= m:
                    break
        props = self.get_meta()
        props['count'] = min(self.count, max_count) if self.count else None
        return self.__class__(
            take_items(max_count),
            **props
        )

    def skip(self, count=1):
        def skip_items(c):
            for n, i in self.enumerated_items():
                if n >= c:
                    yield i
        if self.count and count >= self.count:
            next_items = []
        else:
            next_items = self.get_items()[count:] if self.is_in_memory() else skip_items(count)
        props = self.get_meta()
        props['count'] = self.count - count if self.count else None
        return self.__class__(
            next_items,
            **props
        )

    def pass_items(self):
        for _ in self.get_items():
            pass

    def add(self, flux_or_items, before=False, **kwargs):
        if isinstance(flux_or_items, AnyFlux):
            return self.add_flux(
                flux_or_items,
                before=before,
            )
        else:
            return self.add_items(
                flux_or_items,
                before=before,
            )

    def add_items(self, items, before=False):
        old_items = self.iterable()
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        props = self.get_meta()
        props['count'] = None
        return self.__class__(
            chain_records,
            **props
        )

    def add_flux(self, flux, before=False):
        old_count = self.count
        new_count = flux.count
        if old_count is not None or new_count is not None:
            total_count = new_count + old_count
        else:
            total_count = None
        return self.add_items(
            flux.get_items(),
            before=before,
        ).update_meta(
            count=total_count,
        )

    def count_to_items(self):
        return self.add_items(
            [self.count],
            before=True,
        )

    def separate_count(self):
        return (
            self.count,
            self,
        )

    def separate_first(self):
        items = self.iterable()
        props = self.get_meta()
        if props.get('count'):
            props['count'] -= 1
        title_item = next(items)
        data_flux = self.__class__(
            items,
            **props
        )
        return (
            title_item,
            data_flux,
        )

    def split_by_pos(self, pos):
        first_flux, second_flux = self.tee(2)
        return (
            first_flux.take(pos),
            second_flux.skip(pos),
        )

    def split_by_list_pos(self, list_pos):
        count_limits = len(list_pos)
        cloned_fluxes = self.tee(count_limits + 1)
        filtered_fluxes = list()
        prev_pos = 0
        for n, cur_pos in enumerate(list_pos):
            count_items = cur_pos - prev_pos
            filtered_fluxes.append(
                cloned_fluxes[n].skip(
                    prev_pos,
                ).take(
                    count_items,
                ).update_meta(
                    count=count_items,
                )
            )
            prev_pos = cur_pos
        filtered_fluxes.append(
            cloned_fluxes[-1].skip(
                list_pos[-1],
            )
        )
        return filtered_fluxes

    def split_by_numeric(self, func, count):
        return [
            f.filter(
                lambda i, n=n: func(i) == n,
            ) for n, f in enumerate(
                self.tee(count),
            )
        ]

    def split_by_boolean(self, func):
        return self.split_by_numeric(
            func=lambda f: int(bool(func(f))),
            count=2,
        )

    def split(self, by, count=None):
        if isinstance(by, int):
            return self.split_by_pos(by)
        elif isinstance(by, (list, tuple)):
            return self.split_by_list_pos(by)
        elif callable(by):
            if count:
                return self.split_by_numeric(by, count)
            else:
                return self.split_by_boolean(by)
        else:
            raise TypeError('split(by): by-argument must be int, list, tuple or function, {} received'.format(type(by)))

    def split_to_disk_by_step(
            self,
            step=arg.DEFAULT,
            file_template=arg.DEFAULT, encoding=arg.DEFAULT,
            sort_each_by=None, reverse=False,
            verbose=True,
    ):
        count, total_fx = self.count, self
        step = arg.undefault(step, self.max_items_in_memory)
        file_template = arg.undefault(file_template, self.tmp_files_template)
        encoding = arg.undefault(encoding, self.tmp_files_encoding)
        if count is None:
            total_fn = file_template.format('total')
            self.log('Collecting input into {}'.format(total_fn), verbose=verbose)
            count, total_fx = self.to_json().to_file(
                total_fn,
                encoding=encoding,
            ).map_to_any(
                json.loads,
            ).separate_count()
        part_start, part_no, sorted_parts = 0, None, list()
        while part_start < count:
            part_no = int(part_start / step)
            part_fn = file_template.format(part_no)
            self.log('Sorting part {} and saving into {} ... '.format(part_no, part_fn), verbose=verbose)
            part_fx = total_fx.take(step)
            if sort_each_by is not None:
                part_fx = part_fx.memory_sort(key=sort_each_by, reverse=reverse, verbose=verbose)
            self.log('Writing {} ...'.format(part_fn), end='\r', verbose=verbose)
            part_fx = part_fx.to_json().to_file(part_fn, encoding=encoding, verbose=verbose).map_to_any(json.loads)
            sorted_parts.append(part_fx)
            part_start = part_start + step
        return sorted_parts

    def split_to_iter_by_step(self, step):
        def take_items():
            output_items = list()
            for n, i in self.enumerated_items():
                output_items.append(i)
                if n + 1 >= step:
                    break
            return output_items
        items = take_items()
        props = self.get_meta()
        while items:
            props['count'] = len(items)
            yield self.__class__(
                items,
                **props
            )
            items = take_items()

    def memory_sort(self, key=lambda i: i, reverse=False, verbose=False):
        list_to_sort = self.get_list()
        count = len(list_to_sort)
        self.log('Sorting {} items in memory...'.format(count), end='\r', verbose=verbose)
        sorted_items = sorted(
            list_to_sort,
            key=key,
            reverse=reverse,
        )
        self.log('Sorting has been finished.', end='\r', verbose=verbose)
        self.count = len(sorted_items)
        return self.__class__(
            sorted_items,
            **self.get_meta()
        )

    def disk_sort(self, key=lambda i: i, reverse=False, step=arg.DEFAULT, verbose=False):
        step = arg.undefault(step, self.max_items_in_memory)
        flux_parts = self.split_to_disk_by_step(
            step=step,
            sort_each_by=key, reverse=reverse,
            verbose=verbose,
        )
        assert flux_parts, 'streams must be non-empty'
        iterables = [f.iterable() for f in flux_parts]
        counts = [f.count for f in flux_parts]
        props = self.get_meta()
        props['count'] = sum(counts)
        self.log('Merging {} parts... '.format(len(iterables)), verbose=verbose)
        return self.__class__(
            merge_iter(iterables, key, reverse),
            **props
        )

    def sort(self, *keys, reverse=False, step=arg.DEFAULT, verbose=True):
        keys = arg.update(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if len(keys) == 0:
            key = fs.same()
        elif len(keys) == 1:
            key = keys[0]
        else:
            key = fs.composite_key(keys)
        if self.is_in_memory() or (step is None) or (self.count is not None and self.count <= step):
            return self.memory_sort(key, reverse=reverse, verbose=verbose)
        else:
            return self.disk_sort(key, reverse=reverse, step=step, verbose=verbose)

    def get_list(self):
        return list(self.get_items())

    def is_in_memory(self):
        return isinstance(self.data, list)

    def to_memory(self):
        items_as_list_in_memory = self.get_list()
        props = self.get_meta()
        props['count'] = len(items_as_list_in_memory)
        if 'check' in props:
            props['check'] = False
        return self.__class__(
            items_as_list_in_memory,
            **props
        )

    def to_any(self):
        return fx.AnyFlux(
            self.get_items(),
            count=self.count,
        )

    def to_lines(self, **kwargs):
        return fx.LinesFlux(
            self.map_to_any(str).get_items(),
            count=self.count,
            check=True,
        )

    def to_json(self, **kwargs):
        return self.map_to_any(
            json.dumps
        ).to_lines()

    def to_rows(self, *args, **kwargs):
        function = kwargs.pop('function', None)
        if kwargs:
            raise AttributeError(
                'to_rows(): kwargs {} are not supported for class {}'.format(kwargs.keys(), self.class_name())
            )
        if args:
            raise AttributeError(
                'to_rows(): positional arguments are not supported for class {}'.format(self.class_name())
            )
        return fx.RowsFlux(
            map(function, self.get_items()) if function is not None else self.get_items(),
            count=self.count,
        )

    def to_pairs(self, **kwargs):
        return fx.PairsFlux(
            self.get_items(),
            count=self.count,
        )

    def to_records(self, **kwargs):
        function = kwargs.get('function')
        return self.map_to_records(function)

    def show(self, count=3):
        self.log([self.class_name(), self.get_meta(), '\n'], verbose=True)
        if self.is_in_memory():
            for i in self.get_items()[:count]:
                self.log(i, verbose=True)
        else:
            self.log(self.one(), verbose=True)

    def print(self, flux_function='count', *args, **kwargs):
        value = self.get_property(flux_function, *args, **kwargs)
        self.log(value, verbose=True)
        return self

    def submit(self, external_object=print, flux_function='count', key=None, show=False):
        value = self.get_property(flux_function)
        if key is not None:
            value = {key: value}
        self.log(value, verbose=show)

        if callable(external_object):
            external_object(value)
        elif isinstance(external_object, list):
            external_object.append(value)
        elif isinstance(external_object, dict):
            if isinstance(value, dict):
                external_object.update(value)
            else:
                cur_time = datetime.now().isoformat()
                external_object[cur_time] = value
        else:
            raise TypeError('external_object must be callable, list or dict')
        return self
