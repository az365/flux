from datetime import date, timedelta

DAYS_IN_WEEK = 7
WEEKS_IN_YEAR = 52

MIN_YEAR = 2010


def get_min_year():
    return MIN_YEAR


def set_min_year(year):
    global MIN_YEAR
    MIN_YEAR = year


def get_date(d, return_is_it_iso_date=False):
    if isinstance(d, date):
        cur_date = d
        is_iso_date = False
    elif isinstance(d, str):
        cur_date = date.fromisoformat(d)
        is_iso_date = True
    else:
        raise TypeError('Argument must be date in iso-format as str or python date (got {})'.format(type(d)))
    if return_is_it_iso_date:
        return cur_date, is_iso_date
    else:
        return cur_date


def to_date(d, as_iso_date=False):
    cur_date = get_date(d)
    if as_iso_date:
        return cur_date.isoformat()
    else:
        return cur_date


def get_monday_date(d, as_iso_date=None):
    cur_date, is_iso_date = get_date(d, return_is_it_iso_date=True)
    if as_iso_date is None:
        as_iso_date = is_iso_date
    monday_date = cur_date + timedelta(days=-cur_date.weekday())
    return to_date(monday_date, as_iso_date)


def get_year_start_monday(year, as_iso_date=True):
    year_start_date = date(year, 1, 1)
    year_start_monday = year_start_date + timedelta(days=-year_start_date.weekday())
    return to_date(year_start_monday, as_iso_date)


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


def get_week_abs_from_year_and_week(year, week, min_year=MIN_YEAR):
    week_abs = (year - min_year) * WEEKS_IN_YEAR + week
    return week_abs


def get_week_abs_from_date(d, min_year=MIN_YEAR):
    year, week = get_year_and_week_from_date(d)
    week_abs = get_week_abs_from_year_and_week(year, week, min_year=min_year)
    return week_abs


def get_week_no_from_date(d):
    _, week_no = get_year_and_week_from_date(d)
    return week_no


def get_year_and_week_from_week_abs(week_abs, min_year=MIN_YEAR):
    delta_year = int(week_abs / WEEKS_IN_YEAR)
    year = min_year + delta_year
    week = week_abs - delta_year * WEEKS_IN_YEAR
    return year, week


def get_date_from_week_abs(week_abs, min_year=MIN_YEAR, as_iso_date=True):
    year, week = get_year_and_week_from_week_abs(week_abs, min_year=min_year)
    cur_date = get_date_from_year_and_week(year, week, as_iso_date=as_iso_date)
    return cur_date
