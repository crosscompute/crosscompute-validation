import re
from pathlib import PurePath

from crosscompute_macros.disk import (
    list_paths)


async def get_matching_paths(path_template):
    path = PurePath(path_template)
    expression = path.name.format(suffix='.*', index='[0-9]+')
    folder = path.parent
    paths = await list_paths(folder)
    pattern = re.compile(expression + '$')
    return [folder / _ for _ in paths if pattern.match(_)]
