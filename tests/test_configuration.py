from aiofiles import open
from pytest import mark, raises

from crosscompute_macros.disk import (
    make_link,
    remove_path)
from crosscompute_validation.errors import (
    CrossComputeConfigurationError)
from crosscompute_validation.functions.configuration import (
    Definition,
    validate_paths)


@mark.asyncio
async def test_validate_paths(tmpdir):
    definition = Definition({
        'xs': ['a', 'b']})
    definition.absolute_folder = tmpdir
    await validate_paths(definition)
    definition['xs'] = [{'path': []}]
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    definition['xs'][0]['path'] = str(tmpdir / 'c')
    await make_link(tmpdir / 'c', 'b')
    await make_link(tmpdir / 'b', 'a')
    await make_link(tmpdir / 'a', '/')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    await make_link(tmpdir / 'a', 'c')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    async with open(tmpdir / 'a', 'wt') as f:
        await f.write('A')
    await validate_paths(definition)
