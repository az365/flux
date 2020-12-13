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
        parent = self.get_parent()
        if parent and not self.is_root():
            return parent.get_storage()

    def get_context(self):
        parent = self.get_parent()
        if parent:
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
        storage = self.get_storage()
        if storage:
            return storage.get_path_delimiter()
        else:
            return DEFAULT_PATH_DELIMITER

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


class LeafConnector(AbstractConnector):
    def __init__(
            self,
            name,
            parent=None,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )

    def is_root(self):
        return False

    def has_hierarchy(self):
        return False


class HierarchicConnector(AbstractConnector):
    def __init__(
            self,
            name,
            parent=None,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.children = dict()

    def has_hierarchy(self):
        return True

    @abstractmethod
    def get_default_child_class(self):
        pass

    def get_child_class_by_name(self, name):
        return self.get_default_child_class()

    def child(self, name, **kwargs):
        cur_child = self.children.get(name)
        if not cur_child:
            child_class = self.get_child_class_by_name(name)
            cur_child = child_class(name, **kwargs)
            self.children[name] = cur_child
        return cur_child

    def get_items(self):
        return self.children

    def get_meta(self):
        meta = super().get_meta()
        meta.pop('children')
        return meta


class AbstractStorage(HierarchicConnector):
    def __init__(
            self,
            name,
            context,
            verbose=True,
    ):
        super().__init__(
            name=name,
            parent=context,
        )
        self.verbose = verbose

    def is_root(self):
        return True

    @abstractmethod
    def get_default_child_class(self):
        pass

    def get_parent(self):
        return self.get_context()

    def get_storage(self):
        return self

    def get_path_prefix(self):
        return ''

    def get_path_delimiter(self):
        return DEFAULT_PATH_DELIMITER

    def get_path(self):
        return self.get_path_prefix()

    def get_path_as_list(self):
        return [self.get_path()]


class AbstractFolder(HierarchicConnector):
    def __init__(
            self,
            name,
            parent,
            verbose=AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.verbose = verbose if verbose is not None and verbose != AUTO else parent.verbose

    def is_root(self):
        False

    @abstractmethod
    def get_default_child_class(self):
        pass


class FlatFolder(AbstractFolder):
    def __init__(
            self,
            name,
            parent,
            verbose=AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
            verbose=verbose,
        )

    @abstractmethod
    def get_default_child_class(self):
        pass

    def get_path_prefix(self):
        return self.get_storage().get_path_prefix()

    def get_path_delimiter(self):
        return self.get_storage().get_path_delimiter()

    def get_path_as_list(self):
        return self.get_parent().get_path_as_list() + self.get_name().split(self.get_path_delimiter())


class HierarchicFolder(HierarchicConnector):
    def __init__(
            self,
            name,
            parent,
            verbose=AUTO,
    ):
        super().__init__(
            name=name,
            parent=parent,
        )
        self.verbose = verbose if verbose is not None and verbose != AUTO else parent.verbose

    def get_default_child_class(self):
        return self.__class__

    def get_folders(self):
        folders = list()
        for folder in self.children:
            folders.append(folder)
        return folders
