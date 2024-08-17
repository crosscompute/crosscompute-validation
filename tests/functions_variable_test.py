import json

import pytest
from aiofiles import open

from crosscompute_macros.abstract import (
    Clay)
from crosscompute_validation.constants import (
    D_VALUE)
from crosscompute_validation.functions.variable import (
    LoadableNumberView,
    initialize_view_by_name,
    load_variable_data)


@pytest.mark.asyncio
async def test_load_variable_data(tmp_path):
    path_name = 'v.dictionary'
    path = tmp_path / path_name
    async with open(path, mode='wt') as f:
        await f.write(json.dumps({'a': 1}))
    initialize_view_by_name({'number': LoadableNumberView})
    variable = Clay(id='a', view_name='number')
    variable_data = await load_variable_data(path, variable)
    assert variable_data[D_VALUE] == 1
