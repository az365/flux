from datetime import date, timedelta

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


DAYS_IN_WEEK = 7
WEEKS_IN_YEAR = 52

MIN_YEAR = 2010


def get_min_year():
    return MIN_YEAR


def set_min_year(year):
    global MIN_YEAR
    MIN_YEAR = year


def check_iso_date(d):
    if isinstance(d, str):
        return list(map(len, d.split('-'))) == [4, 2, 2]


def raise_date_type_error(d):
    raise TypeError('Argument must be date in iso-format as str or python date (got {})'.format(type(d)))


def get_date(d):
    is_iso_date = check_iso_date(d)
    if isinstance(d, date):
        return d
    elif is_iso_date:
        return date.fromisoformat(d)
    else:
        raise_date_type_error(d)


def to_date(d, as_iso_date=False):
    cur_date = get_date(d)
    if as_iso_date:
        return cur_date.isoformat()
    else:
        return cur_date


def get_month_from_date(d):
    if check_iso_date(d):
        return date.fromisoformat(d).month
    elif isinstance(d, date):
        return d.month
    else:
        raise_date_type_error(c)


def get_month_first_date(d):
    if check_iso_date(d):
        return d[:8] + '01'
    elif isinstance(d, date):
        return date(d.year, d.month, 1)
    else:
        raise_date_type_error(d)


def get_monday_date(d, as_iso_date=None):
    cur_date = get_date(d)
    if as_iso_date is None:
        as_iso_date = check_iso_date(d)
    monday_date = cur_date + timedelta(days=-cur_date.weekday())
    return to_date(monday_date, as_iso_date)


def get_year_start_monday(year, as_iso_date=True):
    year_start_date = date(year, 1, 1)
    year_start_monday = year_start_date + timedelta(days=-year_start_date.weekday())
    return to_date(year_start_monday, as_iso_date)


def get_next_year_date(d, increment=1, round_to_monday=False):
    is_iso_format = check_iso_date(d)
    if is_iso_format:
        dt = date.fromisoformat(d)
        dt = '{:04}-{:02}-{:02}'.format(dt.year + increment, dt.month, dt.day)
    elif isinstance(d, date):
        dt = date(d.year + increment, d.month, d.day)
    else:
        raise_date_type_error(d)
    if round_to_monday:
        return get_monday_date(dt, is_iso_format)
    else:
        return dt


def get_next_week_date(d, increment=1, round_to_monday=False):
    is_iso_format = check_iso_date(d)
    if is_iso_format:
        dt = date.fromisoformat(d)
    elif isinstance(d, date):
        dt = d
    else:
        raise_date_type_error(d)
    dt += timedelta(days=DAYS_IN_WEEK * increment)
    if round_to_monday:
        dt = get_monday_date(d)
    if is_iso_format:
        return to_date(dt, is_iso_format)
    else:
        return dt


def get_weeks_range(date_min, date_max):
    weeks_range = list()
    cur_date = get_monday_date(date_min)
    if cur_date < date_min:
        cur_date = get_next_week_date(cur_date, increment=1)
    while cur_date <= date_max:
        weeks_range.append(cur_date)
        cur_date = get_next_week_date(cur_date, increment=1)
    return weeks_range


def get_weeks_between(a, b, round_to_mondays=False):
    date_a = get_date(a)
    date_b = get_date(b)
    if round_to_mondays:
        date_a = get_monday_date(date_a, as_iso_date=False)
        date_b = get_monday_date(date_b, as_iso_date=False)
    days_since_year_start_monday = (date_b - date_a).days
    weeks = int(days_since_year_start_monday / DAYS_IN_WEEK)
    return weeks


def get_date_from_year_and_week(year, week, as_iso_date=True):
    year_start_monday = get_year_start_monday(year, as_iso_date=False)
    delta_days = week * DAYS_IN_WEEK
    cur_date = year_start_monday + timedelta(days=delta_days)
    return to_date(cur_date, as_iso_date)


def get_year_and_week_from_date(d):
    cur_date = get_date(d)
    year = cur_date.year
    year_start_monday = get_year_start_monday(year, as_iso_date=False)
    days_since_year_start_monday = (cur_date - year_start_monday).days
    week = int(days_since_year_start_monday / DAYS_IN_WEEK)
    if week >= WEEKS_IN_YEAR:
        year += 1
        week = 0
    return year, week


def get_week_abs_from_year_and_week(year, week, min_year=arg.DEFAULT):
    min_year = arg.undefault(min_year, MIN_YEAR)
    week_abs = (year - min_year) * WEEKS_IN_YEAR + week
    return week_abs


def get_week_abs_from_date(d, min_year=arg.DEFAULT):
    year, week = get_year_and_week_from_date(d)
    week_abs = get_week_abs_from_year_and_week(year, week, min_year=min_year)
    return week_abs


def get_week_no_from_date(d):
    _, week_no = get_year_and_week_from_date(d)
    return week_no


def get_year_and_week_from_week_abs(week_abs, min_year=arg.DEFAULT):
    min_year = arg.undefault(min_year, MIN_YEAR)
    delta_year = int(week_abs / WEEKS_IN_YEAR)
    year = min_year + delta_year
    week = week_abs - delta_year * WEEKS_IN_YEAR
    return year, week


def get_date_from_week_abs(week_abs, min_year=arg.DEFAULT, as_iso_date=True):
    year, week = get_year_and_week_from_week_abs(week_abs, min_year=min_year)
    cur_date = get_date_from_year_and_week(year, week, as_iso_date=as_iso_date)
    return cur_date
