import fluxes as fx
from utils import (
    mappers as ms,
    readers,
)


EXAMPLE_FILENAME = 'test_file.tmp'
EXAMPLE_INT_SEQUENCE = [1, 3, 5, 7, 9, 2, 4, 6, 8]
EXAMPLE_CSV_ROWS = [
    'a,1',
    'b,"2,22"',
    'c,3',
]
EXAMPLE_TSV_ROWS = [
    '1\t2\t3',
    '1\t4\t5',
    '1\t6\t7',
    '9\t4\t9',
]


def test_map():
    expected_types = ['AnyFlux', 'LinesFlux', 'LinesFlux', 'LinesFlux']
    received_types = list()
    expected_0 = [-i for i in EXAMPLE_INT_SEQUENCE]
    received_0 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).submit(
        received_types,
        lambda f: f.class_name(),
    ).get_list()
    assert received_0 == expected_0, 'test case 0'
    expected_1 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_1 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: str(-i),
        to=fx.LinesFlux,
    ).submit(
        received_types,
        lambda f: f.class_name(),
    ).get_list()
    assert received_1 == expected_1, 'test case 1'
    expected_2 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_2 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: str(-i),
        to=fx.FluxType.LinesFlux,
    ).submit(
        received_types,
        lambda f: f.class_name(),
    ).get_list()
    assert received_2 == expected_2, 'test case 2'
    expected_3 = [str(-i) for i in EXAMPLE_INT_SEQUENCE]
    received_3 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: str(-i),
        to='LinesFlux',
    ).submit(
        received_types,
        lambda f: f.class_name(),
    ).get_list()
    assert received_3 == expected_3, 'test case 3'
    assert received_types == expected_types, 'test for types'


def test_flat_map():
    expected = ['a', 'a', 'b', 'b']
    received = readers.from_list(
        ['a', 'b']
    ).flat_map(
        lambda i: [i, i],
    ).get_list()
    assert received == expected


def test_filter():
    expected = [7, 6, 8]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).filter(
        lambda i: i > 5,
        lambda i: i <= 8,
    ).get_list()
    assert received == expected


def test_take():
    expected = [1, 3, 5, 7, 9]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).take(
        5,
    ).get_list()
    assert received == expected


def test_skip():
    expected = [2, 4, 6, 8]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).skip(
        5,
    ).get_list()
    assert received == expected


def test_map_filter_take():
    expected = [-1, -3, -5]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).native_map(
        lambda i: -i,
    ).filter(
        lambda i: i % 2,
    ).take(
        3,
    ).get_list()
    assert received == expected


def test_select():
    expected_1 = [
        {'a': '1', 'd': None, 'e': None, 'f': '11', 'g': None, 'h': None},
        {'a': None, 'd': '2,22', 'e': None, 'f': 'NoneNone', 'g': None, 'h': None},
        {'a': None, 'd': None, 'e': '3', 'f': 'NoneNone', 'g': '3', 'h': '3'},
    ]
    received_1 = readers.from_list(
        EXAMPLE_CSV_ROWS,
    ).to_lines(
    ).to_rows(
        delimiter=',',
    ).map_to_records(
        lambda p: {p[0]: p[1]},
    ).select(
        'a',
        h='g',
        g='e',
        d='b',
        e=lambda r: r.get('c'),
        f=('a', lambda v: str(v)*2),
    ).get_list()
    assert received_1 == expected_1, 'test case 1: records'
    expected_2 = [
        (1.00, 'a', '1', 'a'),
        (2.22, 'b', '2.22', 'b'),
        (3.00, 'c', '3', 'c'),
    ]
    received_2 = readers.from_list(
        EXAMPLE_CSV_ROWS,
    ).to_lines(
    ).to_rows(
        delimiter=',',
    ).select(
        0,
        lambda s: s[1].replace(',', '.'),
    ).select(
        (float, 1),
        '*',
        0,
    ).get_list()
    assert received_2 == expected_2, 'test case 2: rows'


def test_enumerated():
    expected = list(enumerate(EXAMPLE_INT_SEQUENCE))
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).enumerate().get_list()
    assert received == expected


def test_save_and_read():
    expected = [str(i) for i in EXAMPLE_INT_SEQUENCE]
    received_0 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_lines(
    ).lazy_save(
        EXAMPLE_FILENAME,
    ).get_list()
    received_1 = readers.from_file(
        EXAMPLE_FILENAME
    ).get_list()
    assert received_0 == expected, 'test case 0'
    assert received_1 == expected, 'test case 1'
    readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_lines(
    ).to_file(
        EXAMPLE_FILENAME,
    )
    received_2 = readers.from_file(
        EXAMPLE_FILENAME,
    ).get_list()
    assert received_2 == expected, 'test case 2'


def test_add():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = EXAMPLE_INT_SEQUENCE + addition
    expected_2 = addition + EXAMPLE_INT_SEQUENCE
    received_1i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition
    ).get_list()
    assert received_1i == expected_1, 'test case 1i'
    received_2i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition,
        before=True,
    ).get_list()
    assert received_2i == expected_2, 'test case 2i'
    received_1f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
    ).get_list()
    assert received_1f == expected_1, 'test case 1f'
    received_2f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
        before=True,
    ).get_list()
    assert received_2f == expected_2, 'test case 2f'


def test_add_records():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = list(map(lambda v: dict(item=v), EXAMPLE_INT_SEQUENCE + addition))
    expected_2 = list(map(lambda v: dict(item=v), addition + EXAMPLE_INT_SEQUENCE))
    received_1 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map_to_records(
        lambda i: dict(item=i),
    ).add(
        readers.from_list(addition).to_records(),
    ).get_list()
    print(received_1)
    assert received_1 == expected_1, 'test case 1i'
    received_2 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_records(
    ).add(
        readers.from_list(addition).to_records(),
        before=True,
    ).get_list()
    print(received_2)
    assert received_2 == expected_2, 'test case 2i'


def test_separate_first():
    expected = [EXAMPLE_INT_SEQUENCE[0], EXAMPLE_INT_SEQUENCE[1:]]
    received = list(
        readers.from_list(
            EXAMPLE_INT_SEQUENCE,
        ).separate_first()
    )
    received[1] = received[1].get_list()
    assert received == expected


def test_split_by_pos():
    pos_1, pos_2 = 3, 5
    expected_1 = EXAMPLE_INT_SEQUENCE[:pos_1], EXAMPLE_INT_SEQUENCE[pos_1:]
    a, b = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        pos_1,
    )
    received_1 = a.get_list(), b.get_list()
    assert received_1 == expected_1, 'test case 1'
    expected_2 = (
        [pos_1] + EXAMPLE_INT_SEQUENCE[:pos_1],
        [pos_2 - pos_1] + EXAMPLE_INT_SEQUENCE[pos_1:pos_2],
        [len(EXAMPLE_INT_SEQUENCE) - pos_2] + EXAMPLE_INT_SEQUENCE[pos_2:],
    )
    a, b, c = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        (pos_1, pos_2),
    )
    received_2 = a.count_to_items().get_list(), b.count_to_items().get_list(), c.count_to_items().get_list()
    assert received_2 == expected_2, 'test case 2'


def test_split_by_func():
    expected = [1, 3, 2, 4], [5, 7, 9, 6, 8]
    a, b = readers.from_list(
        EXAMPLE_INT_SEQUENCE
    ).split(
        lambda i: i >= 5,
    )
    received = a.get_list(), b.get_list()
    assert received == expected


def test_split_by_step():
    expected = [
        [1, 3, 5, 7],
        [9, 2, 4, 6],
        [8],
    ]
    split_0 = readers.from_list(
        EXAMPLE_INT_SEQUENCE
    ).split_to_disk_by_step(
        step=4,
        tmp_file_template='test_split_by_step_{}.tmp',
    )
    received_0 = [f.get_list() for f in split_0]
    assert received_0 == expected, 'test case 0'
    split_1 = readers.from_list(
        EXAMPLE_INT_SEQUENCE
    ).split_to_iter_by_step(
        step=4,
    )
    received_1 = [f.get_list() for f in split_1]
    assert received_1 == expected, 'test case 1'


def test_memory_sort():
    expected = [7, 9, 8, 6, 5, 4, 3, 2, 1]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).memory_sort(
        key=lambda i: 777 if i == 7 else i,
        reverse=True,
    ).get_list()
    assert received == expected


def test_disk_sort_by_key():
    expected = [[k, str(k) * k] for k in range(1, 10)]
    received = readers.from_list(
        [(k, str(k) * k) for k in EXAMPLE_INT_SEQUENCE],
    ).to_pairs(
    ).disk_sort_by_key(
        step=5,
        tmp_file_template='test_disk_sort_by_key_{}.tmp',
    ).get_list()
    assert received == expected


def test_sort():
    expected_0 = list(reversed(range(1, 10)))
    received_0 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).sort(
        reverse=True,
        step=4,
        tmp_file_template='test_disk_sort_by_key_{}.tmp',
    ).get_list()
    assert received_0 == expected_0, 'test case 0'
    expected_1 = list(reversed(range(1, 10)))
    received_1 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).sort(
        lambda i: -i,
        reverse=False,
        step=4,
        tmp_file_template='test_disk_sort_by_key_{}.tmp',
    ).get_list()
    assert received_1 == expected_1, 'test case 1'
    expected_2 = list(reversed(range(1, 10)))
    received_2 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).sort(
        lambda i: 100,
        lambda i: -i,
        lambda i: i,
        reverse=False,
        step=4,
        tmp_file_template='test_disk_sort_by_key_{}.tmp',
    ).get_list()
    assert received_2 == expected_2, 'test case 2'


def test_sorted_group_by_key():
    example = [
        (1, 11), (1, 12),
        (2, 21),
        (3, 31), (3, 32), (3, 33),
    ]
    expected = [
        (1, [11, 12]),
        (2, [21]),
        (3, [31, 32, 33]),
    ]
    received = readers.from_list(
        example
    ).to_pairs(
    ).sorted_group_by_key(
    ).get_list()
    assert received == expected


def test_group_by():
    example = [
        (1, 11), (1, 12),
        (2, 21),
        (3, 31), (3, 32), (3, 33),
    ]
    expected = [
        [11, 12],
        [21],
        [31, 32, 33],
    ]
    received_0 = readers.from_list(example).to_rows().to_records(
        columns=('x', 'y'),
    ).group_by(
        'x',
        as_pairs=True,
    ).map(
        lambda a: [i.get('y') for i in a[1]],
        to=fx.FluxType.RowsFlux,
    ).get_list()
    assert received_0 == expected, 'test case 0'

    received_1 = readers.from_list(example).to_rows().to_records(
        columns=('x', 'y'),
    ).group_by(
        'x',
        as_pairs=False,
    ).map(
        lambda a: [i.get('y') for i in a],
        to=fx.FluxType.RowsFlux,
    ).get_list()
    assert received_1 == expected, 'test case 1'


def test_calc_histogram():
    expected = [
        ('x', {1: 3, 9: 1}),
        ('y', {2: 1, 4: 2, 6: 1})
    ]
    received = readers.from_list(
        EXAMPLE_TSV_ROWS,
    ).to_lines().to_rows(
        '\t',
    ).to_records(
        columns=('x', 'y', 'z'),
    ).select(
        '*',
        x=('x', int),
        y=('y', int),
        z=('z', int),
    ).apply(
        lambda a: ms.get_histograms(a, fields=['x', 'y']),
        native=False,
    ).get_list()
    assert received == expected


def test_norm_text():
    expected = 'абв gb'
    received = ms.norm_text(
        '\t Абв 123 Gb\n'
    )
    assert received == expected


def test_sum_by_keys():
    expected = [((2, 1), {'h': 3}), ((4, 3), {'h': 5})]
    received = readers.from_list(
        [
            {'a': 1, 'b': 2, 'h': 1},
            {'a': 3, 'b': 4, 'h': 5},
            {'a': 1, 'b': 2, 'h': 2},
        ],
    ).apply(
        lambda a: ms.sum_by_keys(
            a,
            keys=('b', 'a'),
            counters=('h', ),
        ),
    ).get_list()
    assert received == expected


def test_to_rows():
    expected = [['a', '1'], ['b', '2,22'], ['c', '3']]
    received = readers.from_list(
        EXAMPLE_CSV_ROWS,
    ).to_lines(
    ).to_rows(
        ',',
    ).get_list()
    assert received == expected


def test_parse_json():
    example = ['{"a": "b"}', 'abc', '{"d": "e"}']
    expected = [{'a': 'b'}, {'err': 'err'}, {'d': 'e'}]
    received = readers.from_list(
        example,
    ).to_lines(
    ).parse_json(
        default_value={'err': 'err'},
    ).get_list()
    assert received == expected


if __name__ == '__main__':
    test_map()
    test_flat_map()
    test_filter()
    test_take()
    test_skip()
    test_map_filter_take()
    test_enumerated()
    test_save_and_read()
    test_add()
    test_add_records()
    test_separate_first()
    test_split_by_pos()
    test_split_by_func()
    test_split_by_step()
    test_memory_sort()
    test_disk_sort_by_key()
    test_sort()
    test_sorted_group_by_key()
    test_group_by()
    test_calc_histogram()
    test_norm_text()
    test_sum_by_keys()
    test_to_rows()
    test_parse_json()
