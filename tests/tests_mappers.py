import fluxes as fx
from utils import mappers as ms


def test_norm_text():
    example = '\t Абв 123 Gb\n'
    expected = 'абв gb'
    received = ms.norm_text(example)
    assert received == expected


def test_calc_histogram():
    example = [
        '1\t2\t3',
        '1\t4\t5',
        '1\t6\t7',
        '9\t4\t9',
    ]
    expected = [
        ('x', {1: 3, 9: 1}),
        ('y', {2: 1, 4: 2, 6: 1})
    ]
    received = fx.LinesFlux(
        example,
    ).to_rows(
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


def test_sum_by_keys():
    example = [
        {'a': 1, 'b': 2, 'h': 1},
        {'a': 3, 'b': 4, 'h': 5},
        {'a': 1, 'b': 2, 'h': 2},
    ]
    expected = [((2, 1), {'h': 3}), ((4, 3), {'h': 5})]
    received = fx.AnyFlux(
        example,
    ).apply(
        lambda a: ms.sum_by_keys(
            a,
            keys=('b', 'a'),
            counters=('h', ),
        ),
    ).get_list()
    assert received == expected


if __name__ == '__main__':
    test_calc_histogram()
    test_norm_text()
    test_sum_by_keys()
