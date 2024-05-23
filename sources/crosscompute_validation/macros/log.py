import re
from logging import (
    basicConfig,
    getLogger,
    DEBUG,
    INFO)
from os.path import expanduser


def configure_argument_parser_for_logging(argument_parser):
    argument_parser.add_argument(
        '--debug', dest='with_debug', action='store_true', default=False,
        help='show debugging messages')


def configure_logging_from(args, logging_level_by_package_name):
    with_debug = args.with_debug
    configure_logging(with_debug)
    configure_logging_level_by_package_name(logging_level_by_package_name)


def configure_logging(with_debug, timestamp='%Y%m%d-%H%M%S'):
    if with_debug:
        logging_level = DEBUG
        logging_format = (
            '%(asctime)s %(levelname)s %(name)s:%(lineno)s %(message)s')
    else:
        logging_level = INFO
        logging_format = '%(asctime)s %(levelname)s %(message)s'
    basicConfig(format=logging_format, datefmt=timestamp, level=logging_level)


def configure_logging_level_by_package_name(logging_level_by_package_name):
    for package_name, logging_level in logging_level_by_package_name.items():
        getLogger(package_name).setLevel(logging_level)


def redact_path(x):
    return re.sub(r'^' + re.escape(expanduser('~')), '~', str(x))
