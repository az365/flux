try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import (
        arguments as arg,
        mappers as ms,
        selection,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import (
        arguments as arg,
        mappers as ms,
        selection,
    )


DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
ZERO_VALUES = (None, 'None', '', '-', 0)


def partial(function, *args, **kwargs):
    def new_func(item):
        return function(item, *args, **kwargs)
    return new_func


def same():
    def func(item):
        return item
    return func


def const(value):
    def func(_):
        return value
    return func


def cast(field_type, default_int=0):
    def func(value):
        cast_function = DICT_CAST_TYPES.get(field_type, field_type)
        if value in (None, 'None', '') and field_type in ('int', int, float):
            value = default_int
        return cast_function(value)
    return func


def percent(field_type=float, round_digits=1, default_value=None):
    def func(value):
        if value is None:
            return default_value
        else:
            cast_function = DICT_CAST_TYPES.get(field_type, field_type)
            value = round(100 * value, round_digits)
            value = cast_function(value)
            if cast_function == str:
                value += '%'
            return value
    return func


def defined():
    def func(value):
        return value is not None
    return func


def nonzero(zero_values=ZERO_VALUES):
    def func(value):
        return value not in zero_values
    return func


def equal(other):
    def func(value):
        return value == other
    return func


def not_equal(other):
    def func(value):
        return value != other
    return func


def is_in(*list_values):
    list_values = arg.update(list_values)

    def func(value):
        return value in list_values
    return func


def not_in(*list_values):
    list_values = arg.update(list_values)

    def func(value):
        return value not in list_values
    return func


def is_in_sample(sample_rate, sample_bucket=1, as_str=True, hash_func=hash):
    def func(elem_id):
        if as_str:
            elem_id = str(elem_id)
        return hash_func(elem_id) % sample_rate == sample_bucket
    return func


def between(min_value, max_value, including=False):
    def func(value):
        if including:
            return min_value <= value <= max_value
        else:
            return min_value < value < max_value
    return func


def more_than(other, including=False):
    def func(value):
        if including:
            return value >= other
        else:
            return value > other
    return func


def at_least(number):
    return more_than(number, including=True)


def safe_more_than(other, including=False):
    def func(value):
        first, second = value, other
        if type(first) != type(second):
            if not (isinstance(first, (int, float)) and isinstance(second, (int, float))):
                first = str(type(first))
                second = str(type(second))
        if including:
            return first >= second
        else:
            return first > second
    return func


def top(count=10, output_values=False):
    def func(keys, values=None):
        if values:
            pairs = sorted(zip(keys, values), key=lambda i: i[1], reverse=True)
        else:
            dict_counts = dict()
            for k in keys:
                dict_counts[k] = dict_counts.get(k, 0)
            pairs = sorted(dict_counts.items())
        top_n = pairs[:count]
        if output_values:
            return top_n
        else:
            return [i[0] for i in top_n]
    return func


def is_ordered(reverse=False, including=True):
    def func(previous, current):
        if current == previous:
            return including
        elif reverse:
            return safe_more_than(current)(previous)
        else:
            return safe_more_than(previous)(current)
    return func


def maybe(*conditions):
    conditions = arg.update(conditions)

    def func(value):
        for c in conditions:
            if c(value):
                return True
        return False
    return func


def never(*conditions):
    conditions = arg.update(conditions)

    def func(value):
        for c in conditions:
            if c(value):
                return False
        return True
    return func


def apply_dict(dictionary, default=None):
    def func(key):
        return dictionary.get(key, default)
    return func


def elem_no(position, default=None):
    def func(array):
        if isinstance(array, (list, tuple)) and 0 <= position < len(array):
            return array[position]
        else:
            return default
    return func


def value_by_key(key, default=None):
    def func(item):
        if isinstance(item, dict):
            return item.get(key, default)
        elif isinstance(item, (list, tuple)):
            return item[key] if isinstance(key, int) and 0 <= key <= len(item) else None
    return func


def values_by_keys(keys, default=None):
    def func(item):
        return [value_by_key(k, default)(item) for k in keys]
    return func


def uniq():
    def func(array):
        if isinstance(array, (set, list, tuple)):
            result = list()
            for i in array:
                if i not in result:
                    result.append(i)
            return result
    return func


def composite_key(*functions):
    key_functions = arg.update(functions)

    def func(item):
        result = list()
        for f in key_functions:
            if callable(f):
                value = f(item)
            else:
                if fx.is_record(item):
                    value = selection.value_from_record(item, f)
                elif fx.is_row(item):
                    value = selection.value_from_row(item, f)
                else:
                    value = selection.value_from_any(item, f)
            result.append(value)
        return tuple(result)
    return func


def shifted_func(func):
    def func_(x, y):
        assert len(x) == len(y)
        shift_max = len(x) - 1
        result = list()
        for shift in range(-shift_max + 2, 0):
            shifted_x = x[0: shift_max + shift]
            shifted_y = y[- shift: shift_max]
            stat = func(shifted_x, shifted_y)
            result.append(stat)
        for shift in range(0, shift_max - 1):
            shifted_x = x[shift: shift_max]
            shifted_y = y[0: shift_max - shift]
            stat = func(shifted_x, shifted_y)
            result.append(stat)
        return result
    return func_


def unfold_lists(fields, number_field='n', default_value=0):
    def func(record):
        yield from ms.unfold_lists(record, fields=fields, number_field=number_field, default_value=default_value)
    return func


def compare_lists(a_field='a_only', b_field='b_only', ab_field='common', as_dict=True):
    def func(list_a, list_b):
        items_common, items_a_only, items_b_only = list(), list(), list()
        for item in list_a:
            if item in list_b:
                items_common.append(item)
            else:
                items_a_only.append(item)
        for item in list_b:
            if item not in list_a:
                items_b_only.append(item)
        result = ((a_field, items_a_only), (b_field, items_b_only), (ab_field, items_common))
        if as_dict:
            return dict(result)
        else:
            return result
    return func


def list_minus():
    def func(list_a, list_b):
        return [i for i in list_a if i not in list_b]
    return func
