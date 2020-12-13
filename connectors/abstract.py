from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        log_progress,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        log_progress,
    )


AUTO = arg.DEFAULT
DEFAULT_PATH_DELIMITER = '/'
CHUNK_SIZE = 8192


class AbstractConnector(ABC):
    def __init__(
            self,
            name,
            parent=None,
    ):
        self.name = name
        self.parent = parent

    @staticmethod
    def is_context():
        return False

    def is_root(self):
        return self.get_parent().is_context()

    @abstractmethod
    def has_hierarchy(self):
        pass

    def get_name(self):
        return self.name

    def get_parent(self):
        return self.parent

    def get_storage(self):
        if not self.is_root():
            return self.get_parent().get_storage()

    def get_context(self):
        return self.get_parent().get_context()

    def get_logger(self):
        if self.get_context():
            return self.get_context().get_logger()
        else:
            return log_progress.get_logger()

    def log(self, msg, level=AUTO, end=AUTO, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_names_hierarchy(self):
        if self.is_root():
            hierarchy = list()
        else:
            hierarchy = self.get_parent().get_names_hierarchy()
        return hierarchy + [self.get_name()]

    def get_path_prefix(self):
        return self.get_storage().get_path_prefix()

    def get_path_delimiter(self):
        return self.get_storage().get_path_delimiter()

    def get_path(self):
        if self.is_root():
            return self.get_path_prefix()
        else:
            return self.get_parent().get_path() + self.get_path_delimiter() + self.get_name()

    def get_path_as_list(self):
        if self.is_root():
            return [self.get_path_prefix()]
        else:
            return self.get_parent().get_path_as_list() + [self.name.split(self.get_path_delimiter())]

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('context')
        return meta


class AbstractFolder:
    def __init__(
            self,
            path,
    ):
        self.path = path

    def get_path(self):
        return self.path

    def get_path_as_list(self, delimiter=AUTO):
        path_delimiter = arg.undefault(delimiter, self.get_delimiter())
        return self.get_path().split(path_delimiter)

    def get_delimiter(self):
        return DEFAULT_PATH_DELIMITER

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('files')
        return meta
