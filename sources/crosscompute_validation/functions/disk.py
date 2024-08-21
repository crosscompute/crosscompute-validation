import re
from pathlib import Path

from crosscompute_macros.disk import (
    list_paths)


async def get_matching_paths(path_template):
    # TODO: Rethink the necessity of suffix
    path_template = Path(path_template)
    expression = path_template.name
    if '{suffix}' in expression:
        expression = expression.replace('{suffix}', '.*')
    if '{index}' in expression:
        expression = expression.replace('{index}', '[0-9]+')
    pattern = re.compile(expression + '$')
    paths = await list_paths(path_template.parent)
    return [_ for _ in paths if pattern.match(_)]
