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


class AbstractConnector:
    def __init__(
            self,
            name,
            context=None,
    ):
        self.name = name
        self.context = context

    def get_context(self):
        return self.context

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
