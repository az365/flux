from enum import Enum
from datetime import datetime
import logging

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        functions as fs,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        functions as fs,
    )


DEFAULT_STEP = 10
DEFAULT_LOGGER_NAME = 'flux'
DEFAULT_LOGGING_LEVEL = logging.WARNING
LINE_LEN = 100


class OperationStatus(Enum):
    New = 'new'
    # Started = 'started'
    InProgress = 'in_progress'
    Done = 'done'


class LoggingLevel(Enum):
    Debug = logging.DEBUG
    Info = logging.INFO
    Warning = logging.WARNING
    Error = logging.ERROR
    Critical = logging.CRITICAL


def get_method_name(level=LoggingLevel.Info):
    if not isinstance(level, LoggingLevel):
        level = LoggingLevel(level)
    if level == LoggingLevel.Debug:
        return 'debug'
    elif level == LoggingLevel.Info:
        return 'info'
    elif level == LoggingLevel.Warning:
        return 'warning'
    elif level == LoggingLevel.Error:
        return 'error'
    elif level == LoggingLevel.Critical:
        return 'critical'


def get_logger(name=DEFAULT_LOGGER_NAME):
    logger = logging.getLogger(name)
    logger.setLevel(DEFAULT_LOGGING_LEVEL)
    return logger


def format_message(*messages, max_len=LINE_LEN):
    messages = arg.update(messages)
    message = ' '.join([str(m) for m in messages])
    if len(message) > max_len:
        message = message[:max_len - 2] + '..'
    return message


def progress(items, name='Progress', count=None, step=DEFAULT_STEP, logger=get_logger()):
    return Progress(
        name,
        count=count,
        logger=logger,
    ).iterate(
        items,
        step=step,
    )


def clear_line():
    print('\r', end='')
    print(' ' * LINE_LEN, end='\r')


def show(*messages, end=arg.DEFAULT, clear_before=True):
    message = format_message(*messages, max_len=LINE_LEN)
    end = arg.undefault(end, '\r' if message.endswith('...') else '\n')
    if clear_before:
        clear_line()
    print(message, end=end)


def log(msg, level=arg.DEFAULT, logger=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
    level = arg.undefault(level, LoggingLevel.Info if verbose else LoggingLevel.Debug)
    logger = arg.undefault(logger, get_logger())
    if isinstance(msg, (list, tuple)):
        msg = format_message(*msg)
    if not isinstance(level, LoggingLevel):
        level = LoggingLevel(level)
    if logger:
        logging_method = getattr(logger, get_method_name(level))
        logging_method(msg)
    if verbose and level.value < logger.level:
        show(msg, end=end)


class Progress:
    def __init__(
            self,
            name='Progress',
            count=None,
            verbose=True,
            logger=True,
            context=None,
    ):
        self.name = name
        self.expected_count = count
        self.verbose = verbose
        self.state = OperationStatus.New
        self.position = 0
        self.start_time = None
        self.context = context
        if isinstance(logger, bool):
            if logger:
                self.logger = context.logger if context else get_logger()
            else:
                self.logger = None  # Do not log
        else:
            self.logger = logger

    def get_logger(self):
        return self.logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=arg.DEFAULT):
        log(
            logger=self.get_logger(),
            msg=msg, level=level, end=end,
            verbose=arg.undefault(verbose, self.verbose),
        )

    def update_now(self, cur):
        self.position = cur or self.position or 0
        if self.state != OperationStatus.InProgress:
            self.start(cur)
        if self.expected_count:
            percent = fs.percent(str)((self.position + 1) / self.expected_count)
            line = '{}: {} ({}/{}) items processed'.format(self.name, percent, self.position + 1, self.expected_count)
        else:
            line = '{}: {} items processed'.format(self.name, self.position + 1)
        self.log(line, level=LoggingLevel.Debug, end='\r')

    def update_with_step(self, position, step=DEFAULT_STEP):
        cur_increment = position - (self.position or 0)
        self.position = position
        step_passed = (self.position + 1) % step == 0
        step_passed = step_passed or (cur_increment >= step)
        pool_finished = 0 < (self.expected_count or 0) < (self.position + 1)
        if step_passed or pool_finished:
            self.update_now(position)

    def update(self, position, step=None):
        if step is None or step == 1:
            self.update_now(position)
        else:
            self.update_with_step(position, step)

    def start(self, position=0):
        self.state = OperationStatus.InProgress
        self.start_time = datetime.now()
        self.position = position or self.position or 0
        if self.position != position:
            self.update(position)
        elif self.verbose:
            self.log('{} ({} items): starting...'.format(self.name, self.expected_count))

    def finish(self, cur=None):
        self.update(cur)
        message = '{}: Done. {} items processed'.format(self.name, self.position + 1)
        self.log(message)

    def iterate(self, items, name=None, expected_count=None, step=DEFAULT_STEP):
        self.name = name or self.name
        if isinstance(items, (set, list, tuple)):
            self.expected_count = len(items)
        else:
            self.expected_count = expected_count or self.expected_count
        n = 0
        self.start()
        for n, item in enumerate(items):
            self.update(n, step)
            yield item
        self.finish(n)
