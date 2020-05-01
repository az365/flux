try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
ZERO_VALUES = (None, 'None', '', 0)


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
        if value in (None, 'None', '') and field_type in ('int', int):
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


def is_in(list_values):
    list_values = arg.update(list_values)

    def func(value):
        return value in list_values
    return func


def is_in_sample(sample_rate, sample_bucket=1, as_str=True, hash_func=hash):
    def func(elem_id):
        if as_str:
            elem_id = str(elem_id)
        return hash_func(elem_id) % sample_rate == sample_bucket
    return func


def more_than(number, including=False):
    def func(value):
        if including:
            return value >= number
        else:
            return value > number
    return func


def at_least(number):
    return more_than(number, including=True)


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
        if 0 <= position < len(array):
            return array[position]
        else:
            return default
    return func


def composite_key(*functions, ignore_errors=False):
    key_functions = arg.update(functions)

    def func(item):
        result = list()
        for f in key_functions:
            if callable(f):
                value = f(item)
            else:
                if isinstance(item, dict):
                    value = item.get(f)
                elif isinstance(item, (list, tuple)) and isinstance(f, int) and 0 <= f < len(item):
                    value = item[f]
                else:
                    if ignore_errors:
                        value = None
                    else:
                        raise ValueError('Field {} is not a correct column number for row {}.'.format(f, item))
            result.append(value)
        return tuple(result)
    return func
