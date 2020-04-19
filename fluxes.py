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
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .streams.any_flux import AnyFlux
    from .streams.lines_flux import LinesFlux
    from .streams.rows_flux import RowsFlux
    from .streams.pairs_flux import PairsFlux
    from .streams.schema_flux import SchemaFlux
    from .streams.records_flux import RecordsFlux
    from .utils import arguments as arg


class FluxType(Enum):
    AnyFlux = 'AnyFlux'
    LinesFlux = 'LinesFlux'
    RowsFlux = 'RowsFlux'
    PairsFlux = 'PairsFlux'
    SchemaFlux = 'SchemaFlux'
    RecordsFlux = 'RecordsFlux'


def get_class(flux_type):
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


def is_flux(obj):
    return isinstance(
        obj,
        (AnyFlux, LinesFlux, RowsFlux, PairsFlux, SchemaFlux, RecordsFlux),
    )


def concat(*list_fluxes):
    list_fluxes = arg.update(list_fluxes)
    result = list_fluxes[0]
    for cur_flux in list_fluxes[1:]:
        result = result.add_flux(cur_flux)
    return result
