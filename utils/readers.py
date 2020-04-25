import gzip

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import fluxes as fx


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


def count_lines(filename, encoding=None, gz=False, chunk_size=8192):
    if gz:
        fileholder = gzip.open(filename, 'r')
    else:
        fileholder = open(filename, 'r', encoding=encoding) if encoding else open(filename, 'r')
    count_n = sum(chunk.count('\n') for chunk in iter(lambda: fileholder.read(chunk_size), ''))
    fileholder.close()
    return count_n + 1


def from_file(
        filename,
        encoding=None, gz=False,
        skip_first_line=False, max_n=None,
        rstrip='\n',
        verbose=False, step_n=VERBOSE_STEP,
):
    def lines_from_fileholder(fh, count):
        for n, row in enumerate(fh):
            if rstrip:
                row = row.rstrip(rstrip)
            yield row
            if count and (n + 1 == count):
                break
        fh.close()

    if verbose:
        print('Checking', filename, end='\r')
    lines_count = count_lines(filename, encoding, gz)
    if max_n and max_n < lines_count:
        lines_count = max_n
    if verbose:
        print(' ' * 80, end='\r')
        print(verbose if isinstance(verbose, str) else 'Reading file:', filename)
    if gz:
        fileholder = gzip.open(filename, 'r')
    else:
        fileholder = open(filename, 'r', encoding=encoding) if encoding else open(filename, 'r')

    flux_from_file = fx.LinesFlux(
        lines_from_fileholder(fileholder, lines_count),
        lines_count,
        source=filename,
    )
    if verbose:
        flux_from_file = flux_from_file.progress(
            count=lines_count,
            step=step_n,
            message='Reading {}'.format(filename.split('/')[-1]),
        )
    if skip_first_line:
        flux_from_file = flux_from_file.skip(1)
    return flux_from_file


def from_parquet(parquet):
    def get_records():
        for n in range(parquet.num_rows):
            yield parquet.slice(n, 1).to_pydict()
    return fx.RecordsFlux(
        items=get_records(),
        count=parquet.num_rows,
    ).map(
        lambda r: {k: v[0] for k, v in r.items()}
    )
