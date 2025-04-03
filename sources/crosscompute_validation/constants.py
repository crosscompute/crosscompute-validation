import re

from . import __version__ as protocol_version


PROTOCOL_VERSION = protocol_version


ERROR_CONFIGURATION_NOT_FOUND = -100


CONFIGURATION_NAME = 'automate.yaml'
TOOL_NAME = 'Tool X'
KIT_NAME = 'Kit X'
TOOL_VERSION = '0.0.0'


STEP_NAMES = 'input', 'log', 'output', 'debug', 'print'


VARIABLE_ID_PATTERN = re.compile(r'[a-zA-Z0-9_]+$')
VARIABLE_ID_TEMPLATE_PATTERN = re.compile(r'{ *([a-zA-Z0-9_| ]+?) *}')


RAW_DATA_BYTE_COUNT = 1024
RAW_DATA_CACHE_LENGTH = 256


SCRIPT_LANGUAGE = 'python'
ENGINE_NAME = 'podman'
IMAGE_NAME = 'python'


PRINTER_NAMES = 'pdf',


SUPPORT_EMAIL = 'support@crosscompute.com'


STEP_INPUT = 'i'
STEP_OUTPUT = 'o'
STEP_LOG = 'l'
STEP_DEBUG = 'd'
STEP_PRINT = 'p'


DATA_VALUE = 'v'
DATA_PATH = 'p'
DATA_URI = 'u'
DATA_CONFIGURATION = 'c'
