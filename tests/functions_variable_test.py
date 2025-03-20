import json

import pytest
from aiofiles import open

from crosscompute_macros.abstract import (
    Clay)

from crosscompute_validation.constants import (
    DATA_VALUE)
from crosscompute_validation.functions.variable import (
    LoadableNumberView,
    initialize_view_by_name,
    load_variable_data)


@pytest.mark.asyncio
async def test_load_variable_data(tmp_path):
    folder = tmp_path
    path_name = 'v.dictionary'
    path = folder / path_name
    async with open(path, mode='wt') as f:
        await f.write(json.dumps({'a': 1}))
    initialize_view_by_name({'number': LoadableNumberView})
    variable = Clay(
        id='a', view_name='number', path_name=path_name, configuration={})
    variable_data = await load_variable_data(folder, variable)
    assert variable_data[DATA_VALUE] == 1
