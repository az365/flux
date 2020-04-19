def process_description(d):
    if callable(d):
        function, inputs = d, list()
    elif isinstance(d, (list, tuple)):
        if callable(d[0]):
            function, inputs = d[0], d[1:]
        elif callable(d[-1]):
            inputs, function = d[:-1], d[-1]
        else:
            inputs, function = d, lambda *a: tuple(a)
    else:
        inputs, function = [d], lambda v: v
    return function, inputs


def topologically_sorted(selectors):
    ordered_fields = list()
    unordered_fields = list()
    unresolved_dependencies = dict()
    for field, description in selectors.items():
        unordered_fields.append(field)
        _, dependencies = process_description(description)
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


def value_from_row(row, description):
    if callable(description):
        return description(row)
    elif isinstance(description, (list, tuple)):
        function, columns = process_description(description)
        values = [row[f] for f in columns]
        return function(*values)
    elif isinstance(description, int):
        return row[description]
    else:
        raise TypeError('selector description must be int, callable or tuple ({} as {} given)'.format(
            description, type(description)
        ))


def value_from_record(record, description):
    if callable(description):
        return description(record)
    elif isinstance(description, (list, tuple)):
        function, fields = process_description(description)
        values = [record.get(f) for f in fields]
        return function(*values)
    else:
        return record.get(description)


def get_columns(row_in, *columns):
    row_out = [None] * len(columns)
    c = 0
    for d in columns:
        if d == '*':
            row_out = row_out[:c] + list(row_in) + row_out[c + 1:]
            c += len(row_in)
        else:
            row_out[c] = value_from_row(row_in, d)
            c += 1
    return tuple(row_out)


def get_fields(rec_in, *descriptions):
    record = rec_in.copy()
    fields_out = list()
    for desc in descriptions:
        if desc == '*':
            fields_out += list(rec_in.keys())
        elif isinstance(desc, (list, tuple)):
            if len(desc) > 1:
                f_out = desc[0]
                fs_in = desc[1] if len(desc) == 2 else desc[1:]
                record[f_out] = value_from_record(record, fs_in)
                fields_out.append(f_out)
            else:
                raise ValueError('incorrect selector: {}'.format(desc))
        else:
            if desc not in record:
                record[desc] = None
            fields_out.append(desc)
    return {f: record[f] for f in fields_out}
