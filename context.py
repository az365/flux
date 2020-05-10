from datetime import datetime
import gc

try:  # Assume we're a sub-module in a package.
    import fluxes as fx
    import conns as cs
    from utils import (
        arguments as arg,
        log_progress,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import fluxes as fx
    from . import conns as cs
    from .utils import (
        arguments as arg,
        log_progress,
    )


DEFAULT_FLUX_CONFIG = dict(
    max_items_in_memory=fx.MAX_ITEMS_IN_MEMORY,
    tmp_files_template=fx.TMP_FILES_TEMPLATE,
    tmp_files_encoding=fx.TMP_FILES_ENCODING,
)


class FluxContext:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(FluxContext, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(
            self,
            flux_config=arg.DEFAULT,
            conn_config=arg.DEFAULT,
            logger=arg.DEFAULT
    ):
        self.logger = arg.undefault(logger, log_progress.get_logger())
        self.flux_config = arg.undefault(flux_config, DEFAULT_FLUX_CONFIG)
        self.conn_config = arg.undefault(conn_config, dict())
        self.flux_instances = dict()
        self.conn_instances = dict()

        tmp_files_template = self.flux_config.get('tmp_files_template')
        if tmp_files_template:
            self.conn_instances['tmp'] = cs.LocalFolder(tmp_files_template, context=self)

    def get_context(self):
        return self

    def get_logger(self):
        if self.logger is not None:
            return self.logger
        else:
            return log_progress.get_logger()

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def conn(self, conn_type, name=arg.DEFAULT, check=True, **kwargs):
        name = arg.undefault(name, datetime.now().isoformat())
        conn_class = cs.get_class(conn_type)
        conn_object = conn_class(context=self, **kwargs)
        self.conn_instances[name] = conn_object
        if check:
            conn_object.check()
        return conn_object

    def flux(self, flux_type, name=arg.DEFAULT, check=True, **kwargs):
        name = arg.undefault(name, datetime.now().isoformat())
        flux_class = fx.get_class(flux_type)
        flux_object = flux_class(
            context=self,
            **kwargs
        ).fill_meta(
            check=check,
            **self.flux_config
        )
        self.flux_instances[name] = flux_object
        if check:
            if hasattr(flux_object, 'check'):
                flux_object.check()
        return flux_object

    def get(self, name, deep=True):
        if name in self.flux_instances:
            return self.flux_instances[name]
        elif name in self.conn_instances:
            return self.conn_instances[name]
        elif deep:
            for c in self.conn_instances:
                if hasattr(c, 'get_items'):
                    if name in c.get_items():
                        return c.get_items()[name]

    def get_tmp_folder(self):
        return self.conn_instances.get('tmp')

    def close_conn(self, name, recursively=False, verbose=True):
        closed_count = 0
        this_conn = self.conn_instances[name]
        closed_count += this_conn.close() or 0
        this_conn.close()
        if recursively and hasattr(this_conn, 'get_links'):
            for link in this_conn.get_links():
                closed_count += link.close() or 0
        if verbose:
            self.log('{} connection(s) closed.'.format(closed_count))
        else:
            return closed_count

    def close_flux(self, name, recursively=False, verbose=True):
        this_flux = self.flux_instances[name]
        closed_fluxes, closed_links = this_flux.close() or 0
        if recursively and hasattr(this_flux, 'get_links'):
            for link in this_flux.get_links():
                closed_links += link.close() or 0
        if verbose:
            self.log('{} flux(es) and {} link(s) closed.'.format(closed_fluxes, closed_links))
        else:
            return closed_fluxes, closed_links

    def leave_conn(self, name, recursively=True, verbose=True):
        if name in self.conn_instances:
            self.close_conn(name, recursively=recursively, verbose=verbose)
            self.conn_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    def leave_flux(self, name, recursively=True, verbose=True):
        if name in self.flux_instances:
            self.close_flux(name, recursively=recursively, verbose=verbose)
            self.flux_instances.pop(name)
            gc.collect()
            if not verbose:
                return 1

    @staticmethod
    def flux_classes():
        return fx

    @staticmethod
    def conn_classes():
        return cs

    @staticmethod
    def flux_types():
        return fx.FluxType

    @staticmethod
    def conn_types():
        return cs.ConnType
