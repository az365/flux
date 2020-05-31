from enum import Enum


MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'flux_{}.tmp'
TMP_FILES_ENCODING = 'utf8'


try:  # Assume we're a sub-module in a package.
    from streams.any_flux import AnyFlux
    from streams.lines_flux import LinesFlux
    from streams.rows_flux import RowsFlux
    from streams.pairs_flux import PairsFlux
    from streams.schema_flux import SchemaFlux
    from streams.records_flux import RecordsFlux
    from streams.pandas_flux import PandasFlux
    from utils import (
        arguments as arg,
        schema,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams.any_flux import AnyFlux
    from .streams.lines_flux import LinesFlux
    from .streams.rows_flux import RowsFlux
    from .streams.pairs_flux import PairsFlux
    from .streams.schema_flux import SchemaFlux
    from .streams.records_flux import RecordsFlux
    from .streams.pandas_flux import PandasFlux
    from .utils import (
        arguments as arg,
        schema,
    )


class FluxType(Enum):
    AnyFlux = 'AnyFlux'
    LinesFlux = 'LinesFlux'
    RowsFlux = 'RowsFlux'
    PairsFlux = 'PairsFlux'
    SchemaFlux = 'SchemaFlux'
    RecordsFlux = 'RecordsFlux'
    PandasFlux = 'PandasFlux'


def get_class(flux_type):
    if isinstance(flux_type, str):
        flux_type = FluxType(flux_type)
    assert isinstance(flux_type, FluxType), TypeError(
        'flux_type must be an instance of FluxType (but {} as type {} received)'.format(flux_type, type(flux_type))
    )
    if flux_type == FluxType.AnyFlux:
        return AnyFlux
    elif flux_type == FluxType.LinesFlux:
        return LinesFlux
    elif flux_type == FluxType.RowsFlux:
        return RowsFlux
    elif flux_type == FluxType.PairsFlux:
        return PairsFlux
    elif flux_type == FluxType.SchemaFlux:
        return SchemaFlux
    elif flux_type == FluxType.RecordsFlux:
        return RecordsFlux
    elif flux_type == FluxType.PandasFlux:
        return PandasFlux


def is_flux(obj):
    return isinstance(
        obj,
        (AnyFlux, LinesFlux, RowsFlux, PairsFlux, SchemaFlux, RecordsFlux, PandasFlux),
    )


def is_row(item):
    return RowsFlux.is_valid_item(item)


def is_record(item):
    return RecordsFlux.is_valid_item(item)


def is_schema_row(item):
    return isinstance(item, schema.SchemaRow)


def concat(*list_fluxes):
    list_fluxes = arg.update(list_fluxes)
    result = list_fluxes[0]
    for cur_flux in list_fluxes[1:]:
        result = result.add_flux(cur_flux)
    return result
