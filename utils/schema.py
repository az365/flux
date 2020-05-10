from enum import Enum
from datetime import date, time, datetime
import json

try:
    import conns as cs
except ImportError:
    from .. import conns as cs


class FieldType(Enum):
    Any = 'any'
    Json = 'json'
    Str = 'str'
    Str16 = 'str16'
    Str64 = 'str64'
    Str256 = 'str256'
    Int = 'int'
    Float = 'float'
    IsoDate = 'date'
    IsoTime = 'time'
    IsoDatetime = 'datetime'
    Bool = 'bool'
    Tuple = 'tuple'
    Dict = 'dict'


def any_to_bool(value):
    if isinstance(value, str):
        return value not in ('False', 'false', 'None', 'none', 'no', '0', '')
    else:
        return bool(value)


def safe_converter(converter, default_value=0):
    def func(value):
        try:
            return converter(value)
        except ValueError:
            return default_value
    return func


DIALECTS = ('str', 'py', 'pg', 'ch')
FIELD_TYPES = {
    FieldType.Any.value: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Json.value: dict(py=dict, pg='text', ch='String', str_to_py=json.loads, py_to_str=json.dumps),
    FieldType.Str.value: dict(py=str, pg='text', ch='String', str_to_py=str),
    FieldType.Str16.value: dict(py=str, pg='varchar(16)', ch='FixedString(16)', str_to_py=str),
    FieldType.Str64.value: dict(py=str, pg='varchar(64)', ch='FixedString(64)', str_to_py=str),
    FieldType.Str256.value: dict(py=str, pg='varchar(256)', ch='FixedString(256)', str_to_py=str),
    FieldType.Int.value: dict(py=int, pg='int', ch='Int32', str_to_py=safe_converter(int)),
    FieldType.Float.value: dict(py=float, pg='numeric', ch='Float32', str_to_py=safe_converter(float)),
    FieldType.IsoDate.value: dict(py=date, pg='date', ch='Date', str_to_py=date.fromisoformat),
    FieldType.IsoTime.value: dict(py=date, pg='time', str_to_py=time.fromisoformat),
    FieldType.IsoDatetime.value: dict(py=date, pg='timestamp', ch='Datetime', str_to_py=datetime.fromisoformat),
    FieldType.Bool.value: dict(py=bool, pg='bool', ch='UInt8', str_to_py=any_to_bool, py_to_ch=safe_converter(int)),
    FieldType.Tuple.value: dict(py=tuple, pg='text', str_to_py=eval),
    FieldType.Dict.value: dict(py=dict, pg='text', str_to_py=eval),
}
AGGR_HINTS = (None, 'id', 'cat', 'measure')
HEURISTIC_SUFFIX_TO_TYPE = {
    'id': FieldType.Int,
    'count': FieldType.Int,
    'sum': FieldType.Float,
    'share': FieldType.Float,
    'is': FieldType.Bool,
    'has': FieldType.Bool,
    'ids': FieldType.Tuple,
    'names': FieldType.Tuple,
    'hist': FieldType.Dict,
    None: FieldType.Str,
}


def get_canonic_type(field_type, ignore_absent=False):
    if isinstance(field_type, FieldType):
        return field_type
    elif field_type in FieldType.__dict__.values():
        return FieldType(field_type)
    else:
        for canonic_type, dict_names in sorted(FIELD_TYPES.items(), key=lambda i: i[0], reverse=True):
            for dialect, type_name in dict_names.items():
                if field_type == type_name:
                    return FieldType(canonic_type)
    if not ignore_absent:
        raise ValueError('Unsupported field type: {}'.format(field_type))


def get_dialect_for_conn_type(db_obj):
    if isinstance(db_obj, cs.CONN_CLASSES):
        db_class = db_obj
    elif isinstance(db_obj, cs.ConnType):
        db_class = cs.get_class(db_obj)
    elif isinstance(db_obj, str):
        db_class = (cs.get_class(cs.ConnType(db_obj)))
    elif db_obj is None:
        db_class = None
    else:
        raise ValueError
    if db_class is None:
        return 'py'
    elif db_class == cs.PostgresDatabase:
        return 'pg'
    elif db_class == cs.ClickhouseDatabase:
        return 'ch'
    else:
        return 'str'


def detect_schema_by_title_row(title_row):
    schema = SchemaDescription([])
    for name in title_row:
        name_parts = name.split('_')
        field_type = HEURISTIC_SUFFIX_TO_TYPE[None]
        for suffix in HEURISTIC_SUFFIX_TO_TYPE:
            if suffix in name_parts:
                field_type = HEURISTIC_SUFFIX_TO_TYPE[suffix]
                break
        schema.append_field(
            FieldDescription(name, field_type)
        )
    return schema


class FieldDescription:
    def __init__(
            self,
            name,
            field_type=FieldType.Any,
            nullable=False,
            aggr_hint=None,
    ):
        self.name = name
        self.field_type = get_canonic_type(field_type)
        assert isinstance(nullable, bool)
        self.nullable = nullable
        assert aggr_hint in AGGR_HINTS
        self.aggr_hint = aggr_hint

    def get_type_in(self, dialect):
        if dialect is None:
            return self.field_type.value
        else:
            assert dialect in DIALECTS
            return FIELD_TYPES.get(self.field_type.value, {}).get(dialect)

    def get_converter(self, source, target):
        converter_name = '{}_to_{}'.format(source, target)
        return FIELD_TYPES.get(self.field_type.value, {}).get(converter_name, str)

    def check_value(self, value):
        py_type = self.get_type_in('py')
        return isinstance(value, py_type)


class SchemaDescription:
    def __init__(
            self,
            fields_descriptions,
    ):
        assert isinstance(fields_descriptions, (list, tuple))
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def append_field(self, field):
        if isinstance(field, FieldDescription):
            field_desc = field
        elif isinstance(field, str):
            field_desc = FieldDescription(field)
        elif isinstance(field, (list, tuple)):
            field_desc = FieldDescription(*field)
        elif isinstance(field, dict):
            field_desc = FieldDescription(**field)
        else:
            raise TypeError
        self.fields_descriptions.append(field_desc)

    def get_fields_count(self):
        return len(self.fields_descriptions)

    def get_schema_str(self, dialect):
        if dialect is not None and dialect not in DIALECTS:
            dialect = get_dialect_for_conn_type(dialect)
        field_strings = [
            '{} {}'.format(c.name, c.get_type_in(dialect))
            for c in self.fields_descriptions
        ]
        return ', '.join(field_strings)

    def get_columns(self):
        return [c.name for c in self.fields_descriptions]

    def get_field_position(self, name):
        return self.get_columns().index(name)

    def get_fields_positions(self, names):
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, from_='str', to_='py'):
        converters = list()
        for desc in self.fields_descriptions:
            converters.append(desc.get_converter(from_, to_))
        return tuple(converters)


class SchemaRow:
    def __init__(
            self,
            data,
            schema,
            check=True,
    ):
        if isinstance(schema, SchemaDescription):
            self.schema = schema
        else:
            self.schema = SchemaDescription(schema)
        if check:
            self.data = list()
            self.set_data(data, check)
        else:
            self.data = data

    def set_data(self, row, check=True):
        if check:
            assert isinstance(row, (list, tuple)), 'Row must be list or tuple (got {})'.format(type(row))
            expected_fields_count = self.schema.get_fields_count()
            assert len(row) == expected_fields_count, 'count of cells must match the schema ({} != {})'.format(
                len(row), expected_fields_count,
            )
            schematized_fields = list()
            for value, desc in zip(row, self.schema.fields_descriptions):
                if not desc.check_value(value):
                    converter = desc.get_converter('str', 'py')
                    value = converter(value)
                schematized_fields.append(value)
            self.data = schematized_fields
        else:
            self.data = row

    def get_record(self):
        return {k.name: v for k, v in zip(self.schema.fields_descriptions, self.data)}

    def get_line(self, dialect='str', delimiter='\t', need_quotes=False):
        assert dialect in DIALECTS
        list_str = list()
        for k, v in zip(self.schema.fields_descriptions, self.data):
            convert = k.get_converter('py', dialect)
            value = convert(v)
            if need_quotes:
                if not isinstance(value, (int, float, bool)):
                    value = '"{}"'.format(value)
            list_str.append(str(value))
        return delimiter.join(list_str)

    def get_value(self, name):
        position = self.schema.get_field_position(name)
        return self.data[position]

    def get_values(self, names):
        positions = self.schema.get_fields_positions(names)
        return [self.data[p] for p in positions]
