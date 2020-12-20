from enum import Enum
from datetime import datetime, timedelta
from functools import wraps
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


DEFAULT_STEP = 10000
DEFAULT_LOGGER_NAME = 'flux'
DEFAULT_LOGGING_LEVEL = logging.WARNING
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_LINE_LEN = 127
LONG_LINE_LEN = 600


class OperationStatus(Enum):
    New = 'new'
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


def get_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL):
    logger = Logger(name=name, level=level)
    return logger


def deprecated(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        message = 'Method {}.{}() is deprecated.'
        get_logger().warning(message.format(func.__module__, func.__name__))
        return func(*args, **kwargs)
    return new_func


def deprecated_with_alternative(alternative):
    def _deprecated(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            message = 'Method {}.{}() is deprecated, use {} instead.'
            get_logger().warning(message.format(func.__module__, func.__name__, alternative))
            return func(*args, **kwargs)
        return new_func
    return _deprecated


class Logger:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Logger, cls).__new__(cls)
        return cls.instance

    def __init__(
            self,
            name=DEFAULT_LOGGER_NAME,
            level=DEFAULT_LOGGING_LEVEL,
            formatter=DEFAULT_FORMATTER,
            max_line_len=DEFAULT_LINE_LEN,
    ):
        self.base_logger = logging.getLogger(name)
        self.base_logger.setLevel(level)
        if not self.base_logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter(formatter)
            ch.setFormatter(formatter)
            self.base_logger.addHandler(ch)
        self.level = level
        self.max_line_len = max_line_len

    def progress(self, items, name='Progress', count=None, step=DEFAULT_STEP):
        return Progress(
            name,
            count=count,
            logger=self,
        ).iterate(
            items,
            step=step,
        )

    def new_progress(self, name, **kwargs):
        return Progress(name, logger=self, **kwargs)

    def log(self, msg, level=arg.DEFAULT, logger=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        level = arg.undefault(level, LoggingLevel.Info if verbose else LoggingLevel.Debug)
        logger = arg.undefault(logger, self.base_logger)
        if isinstance(msg, (list, tuple)):
            msg = self.format_message(*msg)
        if not isinstance(level, LoggingLevel):
            level = LoggingLevel(level)
        if logger:
            if level.value >= self.level:
                logging_method = getattr(logger, get_method_name(level))
                logging_method(msg)
        if verbose and level.value < logger.level:
            self.show(msg, end=end)

    def debug(self, msg):
        self.log(msg=msg, level=LoggingLevel.Debug)

    def info(self, msg):
        self.log(msg=msg, level=LoggingLevel.Info)

    def warning(self, msg):
        self.log(msg=msg, level=LoggingLevel.Warning)

    def error(self, msg):
        self.log(msg=msg, level=LoggingLevel.Error)

    def critical(self, msg):
        self.log(msg=msg, level=LoggingLevel.Critical)

    def format_message(self, *messages, max_len=arg.DEFAULT):
        messages = arg.update(messages)
        max_len = arg.undefault(max_len, self.max_line_len)
        message = ' '.join([str(m) for m in messages])
        if len(message) > max_len:
            message = message[:max_len - 2] + '..'
        return message

    def clear_line(self):
        print('\r', end='')
        print(' ' * self.max_line_len, end='\r')

    def show(self, *messages, end=arg.DEFAULT, clear_before=True):
        message = self.format_message(*messages, max_len=LONG_LINE_LEN if end == '\n' else self.max_line_len)
        end = arg.undefault(end, '\r' if message.endswith('...') else '\n')
        if clear_before:
            remainder = self.max_line_len - len(message)
            message += ' ' * remainder
        print(message, end=end)


class Progress:
    def __init__(
            self,
            name='Progress',
            count=None,
            timing=True,
            verbose=True,
            logger=arg.DEFAULT,
            context=None,
    ):
        self.name = name
        self.expected_count = count
        self.verbose = verbose
        self.state = OperationStatus.New
        self.position = 0
        self.timing = timing
        self.start_time = None
        self.past_time = timedelta(0)
        self.context = context
        if logger is None:
            self.logger = None
        elif logger == arg.DEFAULT:
            self.logger = context.get_logger() if context else get_logger()
        else:
            self.logger = logger

    def get_logger(self):
        return self.logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=arg.DEFAULT):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                logger=self.get_logger(),
                msg=msg, level=level, end=end,
                verbose=arg.undefault(verbose, self.verbose),
            )

    def is_started(self):
        return self.start_time is not None

    def is_finished(self):
        if self.expected_count:
            return self.position >= self.expected_count

    def evaluate_share(self):
        if self.expected_count:
            return (self.position + 1) / self.expected_count

    def evaluate_speed(self):
        cur_time = datetime.now()
        self.past_time = cur_time - self.start_time
        if self.past_time.total_seconds():
            speed = self.position / self.past_time.total_seconds()
            return int(speed)

    def get_timing_str(self):
        if self.is_finished():
            finish_time = datetime.now()
            self.past_time = finish_time - self.start_time
            past_minutes = int(self.past_time.total_seconds() / 60)
            return "{:02}:{:02}+{:02}:{:02}'{:02}={:02}:{:02}".format(
                self.start_time.hour, self.start_time.minute,
                int(past_minutes / 60), past_minutes % 60, int(self.past_time.total_seconds()) % 60,
                finish_time.hour, finish_time.minute,
            )
        elif self.expected_count:
            finish_time = self.start_time + self.past_time / (self.evaluate_share() or 0.001)
            rest_time = finish_time - datetime.now()
            has_long_rest_time = rest_time.total_seconds() >= 100 * 60
            if has_long_rest_time:
                return "{:02}:{:02}+{:02}+{:03}~{:02}:{:02}".format(
                    self.start_time.hour, self.start_time.minute,
                    int(round(self.past_time.total_seconds() / 60, 0)), int(rest_time.total_seconds() / 60),
                    finish_time.hour, finish_time.minute,
                )
            else:
                return "{:02}:{:02}+{:02}+{:02}'{:02}~{:02}:{:02}".format(
                    self.start_time.hour, self.start_time.minute,
                    int(round(self.past_time.total_seconds() / 60, 0)),
                    int(rest_time.total_seconds() / 60), int(rest_time.total_seconds()) % 60,
                    finish_time.hour, finish_time.minute,
                )
        else:
            return "{:02}:{:02}+{:02}".format(
                self.start_time.hour, self.start_time.minute, int(round(self.past_time.total_seconds() % 60, 0)),
            )

    def update_now(self, cur):
        self.position = cur or self.position or 0
        if self.state != OperationStatus.InProgress:
            self.start(cur)
        if self.expected_count:
            percent = fs.percent(str)(self.evaluate_share())
            line = '{}: {} ({}/{}) items processed'.format(self.name, percent, self.position + 1, self.expected_count)
        else:
            line = '{}: {} items processed'.format(self.name, self.position + 1)
        if self.timing:
            line = '{} {} ({} it/sec)'.format(self.get_timing_str(), line, self.evaluate_speed())
        self.log(line, level=LoggingLevel.Debug, end='\r')

    def update_with_step(self, position, step=arg.DEFAULT):
        step = arg.undefault(step, DEFAULT_STEP)
        cur_increment = position - (self.position or 0)
        self.position = position
        step_passed = (self.position + 1) % step == 0
        step_passed = step_passed or (cur_increment >= step)
        pool_finished = 0 < (self.expected_count or 0) < (self.position + 1)
        if step_passed or pool_finished:
            self.update_now(position)

    def update(self, position, step=None, message=None):
        if message and message != arg.DEFAULT:
            self.name = message
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

    def finish(self, position=None):
        self.expected_count = position
        self.update(position)
        message = '{}: Done. {} items processed'.format(self.name, self.position + 1)
        if self.timing:
            message = '{} {} ({} it/sec)'.format(self.get_timing_str(), message, self.evaluate_speed())
        self.log(message)

    def iterate(self, items, name=None, expected_count=None, step=arg.DEFAULT):
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
