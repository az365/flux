import gzip as gz

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx
    from ..utils import arguments as arg


VERBOSE_STEP = 10000


def iterable(any_iterable):
    return fx.AnyFlux(any_iterable)


def from_list(input_list):
    def get_generator_from_list(mylist):
        for i in mylist:
            yield i
    return fx.AnyFlux(
        get_generator_from_list(input_list),
        count=len(input_list),
    )


def count_lines(filename, encoding=None, gzip=False, chunk_size=8192, end='\n'):
    if gzip:
        fileholder = gz.open(filename, 'r')
        end = end.encode(encoding) if encoding else end.encode()
    else:
        fileholder = open(filename, 'r', encoding=encoding) if encoding else open(filename, 'r')
    count_n = sum(chunk.count(end) for chunk in iter(lambda: fileholder.read(chunk_size), ''))
    fileholder.close()
    return count_n + 1


def from_file(
        filename,
        encoding=None, gzip=False,
        skip_first_line=False, max_count=None,
        end='\n',
        check=arg.DEFAULT, expected_count=arg.DEFAULT,
        verbose=False, step=VERBOSE_STEP,
):
    def lines_from_fileholder(fh, count):
        for n, row in enumerate(fh):
            if isinstance(row, bytes):
                row = row.decode(encoding) if encoding else row.decode()
            if end:
                row = row.rstrip(end)
            yield row
            if (count > 0) and (n + 1 == count):
                break
        fh.close()

    check = arg.undefault(check, False if gzip else True)
    if check:
        if verbose:
            print('Checking file:', filename, '...', end='\r')
        lines_count = count_lines(filename, encoding, gzip)
    else:
        lines_count = max_count or 0
    if max_count and max_count < lines_count:
        lines_count = max_count
    if lines_count:
        expected_count = lines_count

    if gzip:
        fileholder = gz.open(filename, 'r')
    else:
        fileholder = open(filename, 'r', encoding=encoding) if encoding else open(filename, 'r')
    flux_from_file = fx.LinesFlux(
        lines_from_fileholder(fileholder, lines_count),
        lines_count,
        source=filename,
    )
    if verbose:
        message = verbose if isinstance(verbose, str) else 'Reading {}'.format(filename.split('/')[-1])
        flux_from_file = flux_from_file.progress(
            step=step,
            message=message,
            expected_count=expected_count,
        )
    if skip_first_line:
        flux_from_file = flux_from_file.skip(1)
    return flux_from_file


def from_parquet(parquet):
    def get_records():
        for n in range(parquet.num_rows):
            yield parquet.slice(n, 1).to_pydict()
    return fx.RecordsFlux(
        get_records(),
        count=parquet.num_rows,
    ).map(
        lambda r: {k: v[0] for k, v in r.items()}
    )
