import re


LOGGING_LEVEL_BY_PACKAGE_NAME = {}


ERROR_CONFIGURATION_NOT_FOUND = -100


CONFIGURATION_NAME = 'automate.yaml'
TOOL_NAME = 'Tool X'
TOOLKIT_NAME = 'Toolkit X'
TOOL_VERSION = '0.0.0'
STEP_NAMES = 'input', 'output', 'log', 'debug', 'print'


ATTRIBUTION_URI_AND_IMAGE_TEXT = '''\
[<img src="{image_uri}" alt="{name}" loading="lazy">]({owner_uri}) © {year}'''
ATTRIBUTION_URI_TEXT = '''\
[{name}]({owner_uri}) © {year}'''
ATTRIBUTION_TEXT = '''\
{name} © {year}'''


VARIABLE_ID_TEMPLATE_PATTERN = re.compile(r'{ *([a-zA-Z0-9_| ]+?) *}')


RAW_DATA_BYTE_COUNT = 1024
RAW_DATA_CACHE_LENGTH = 256


SCRIPT_LANGUAGE = 'python'
ENGINE_NAME = 'podman'
IMAGE_NAME = 'python'
PACKAGE_MANAGER_NAMES = 'dnf', 'apt', 'pip', 'npm'


SUPPORT_EMAIL = 'support@crosscompute.com'


S_INPUT = 'i'
S_OUTPUT = 'o'
S_LOG = 'l'
S_DEBUG = 'd'
S_PRINT = 'p'


D_VALUE = 'v'
D_PATH = 'p'
D_URI = 'u'
