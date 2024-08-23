import re
from pathlib import Path

from crosscompute_macros.disk import (
    is_existing_path,
    list_paths)


async def get_matching_paths(path_template):
    path = Path(path_template)
    expression = path.name
    has_suffix = '{suffix}' in expression
    has_index = '{index}' in expression
    if not has_suffix and not has_index:
        is_existing = await is_existing_path(path)
        return [path_template] if is_existing else []
    if has_suffix:
        expression = expression.replace('{suffix}', '.*')
    if has_index:
        expression = expression.replace('{index}', '[0-9]+')
    paths = await list_paths(path.parent)
    pattern = re.compile(expression + '$')
    return [_ for _ in paths if pattern.match(_)]
