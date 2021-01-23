import fluxes as fx
from utils import dates as dt


def test_get_days_between():
    cases = [('2020-01-01', '2020-03-08'), ]
    expected = [67]
    received = [dt.get_days_between(*c) for c in cases]
    assert received == expected


def test_get_next_year_date():
    data = '2019-12-02'
    cases = [1, -2], [False, True]
    expected = ['2020-12-02', '2020-11-30', '2017-12-02', '2017-11-27']
    received = [dt.get_next_year_date(data, i, r) for i in cases[0] for r in cases[1]]
    assert received == expected


def test_get_yearly_dates():
    case = '2020-08-21', '2018-05-01', '2022-05-01'
    expected = ['2018-08-21', '2019-08-21', '2020-08-21', '2021-08-21']
    received = dt.get_yearly_dates(*case)
    assert received == expected


if __name__ == '__main__':
    test_get_days_between()
    test_get_next_year_date()
    test_get_yearly_dates()
