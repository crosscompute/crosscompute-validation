import re
from os.path import expanduser


def redact_path(x):
    return re.sub(r'^' + re.escape(expanduser('~')), '~', str(x))
