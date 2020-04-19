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


def cast(field_type, default_int=0):
    def func(value):
        cast_function = DICT_CAST_TYPES[field_type]
        if value in (None, 'None', '') and field_type in ('int', int):
            value = default_int
        return cast_function(value)
    return func


def defined():
    def func(value):
        return value is not None
    return func


def nonzero(zero_values=ZERO_VALUES):
    def func(value):
        return value not in zero_values
    return func


def equals(other):
    def func(value):
        return value == other
    return func


def is_in(list_values):
    list_values = arg.update(list_values)

    def func(value):
        return value in list_values
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
