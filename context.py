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
            cls.instance = super(FluxContext, cls).__new__(cls)
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

    @staticmethod
    def get_default_instance_name():
        return datetime.now().isoformat()

    def conn(self, conn, name=arg.DEFAULT, check=True, redefine=True, **kwargs):
        name = arg.undefault(name, self.get_default_instance_name())
        conn_object = self.conn_instances.get(name)
        if conn_object:
            if redefine or cs.is_conn(conn):
                self.leave_conn(name, verbose=False)
            else:
                return conn_object
        if cs.is_conn(conn):
            conn_object = conn
        else:
            conn_class = cs.get_class(conn)
            conn_object = conn_class(context=self, **kwargs)
        self.conn_instances[name] = conn_object
        if check and hasattr(conn_object, 'check'):
            conn_object.check()
        return conn_object

    def flux(self, flux, name=arg.DEFAULT, check=True, **kwargs):
        name = arg.undefault(name, self.get_default_instance_name())
        if fx.is_flux(flux):
            flux_object = flux
        else:
            flux_class = fx.get_class(flux)
            flux_object = flux_class(**kwargs)
        flux_object = flux_object.set_name(
            name,
            register=False,
        ).fill_meta(
            context=self,
            check=check,
            **self.flux_config
        )
        self.flux_instances[name] = flux_object
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

    def rename_flux(self, old_name, new_name):
        assert old_name in self.flux_instances, 'Flux must be defined (name {} is not registered)'.format(old_name)
        if new_name != old_name:
            self.flux_instances[new_name] = self.flux_instances.pop(old_name)

    def get_job_folder(self):
        job_folder_obj = self.conn_instances.get('job')
        if job_folder_obj:
            return job_folder_obj
        else:
            job_folder_path = self.flux_config.get('job_folder', '')
            job_folder_obj = cs.LocalFolder(job_folder_path, context=self)
            self.conn_instances['job'] = job_folder_obj
            return job_folder_obj

    def get_tmp_folder(self):
        tmp_folder = self.conn_instances.get('tmp')
        if tmp_folder:
            return tmp_folder
        else:
            tmp_files_template = self.flux_config.get('tmp_files_template')
            if tmp_files_template:
                tmp_folder = cs.LocalFolder(tmp_files_template, context=self)
                self.conn_instances['tmp'] = tmp_folder
                return tmp_folder

    def close_conn(self, name, recursively=False, verbose=True):
        closed_count = 0
        this_conn = self.conn_instances[name]
        closed_count += this_conn.close() or 0
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

    def close_all_conns(self, recursively=False, verbose=True):
        closed_count = 0
        for name in self.conn_instances:
            closed_count += self.close_conn(name, recursively=recursively, verbose=False)
        if verbose:
            self.log('{} connection(s) closed.'.format(closed_count))
        else:
            return closed_count

    def close_all_fluxes(self, recursively=False, verbose=True):
        closed_fluxes, closed_links = 0, 0
        for name in self.flux_instances:
            closed_fluxes, closed_links = self.close_flux(name, recursively=recursively)
        if verbose:
            self.log('{} flux(es) and {} link(s) closed.'.format(closed_fluxes, closed_links))
        else:
            return closed_fluxes, closed_links

    def close_all(self, verbose=True):
        closed_conns = self.close_all_conns(recursively=True, verbose=False)
        closed_fluxes, closed_links = self.close_all_fluxes(recursively=True, verbose=False)
        if verbose:
            self.log('{} conn(s), {} flux(es), {} link(s) closed.'.format(closed_conns, closed_fluxes, closed_links))
        else:
            return closed_conns, closed_fluxes, closed_links

    def leave_all_conns(self, recursively=False):
        closed_count = self.close_all_conns(verbose=False)
        left_count = 0
        for name in self.conn_instances.copy():
            left_count += self.leave_conn(name, recursively=recursively, verbose=False)
        self.log('{} connection(s) closed, {} connection(s) left.'.format(closed_count, left_count))

    def leave_all_fluxes(self, recursively=False):
        closed_fluxes, closed_links = self.close_all_fluxes(verbose=False)
        left_count = 0
        for name in self.flux_instances.copy():
            left_count += self.leave_flux(name, recursively=recursively, verbose=False)
        self.log('{} flux(es) and {} link(s) closed, {} flux(es) left'.format(closed_fluxes, closed_links, left_count))

    def leave_all(self):
        self.close_all(verbose=True)
        self.leave_all_conns(recursively=True)
        self.leave_all_fluxes(recursively=True)

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
